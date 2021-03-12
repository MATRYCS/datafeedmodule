from airflow.providers.presto.hooks.presto import PrestoHook
import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler

from ScyllaDBClient.client import ScyllaClient
from utils import delete_unused_xcoms, encoding_labels, scale_ratio, alter_scylladb_tables


def handle_dates(**kwargs):
    """This function is used to handle registration date"""
    ti = kwargs['ti']

    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    building_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.building")

    building_df['registration_year'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[0]))
    building_df['registration_month'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[1]))
    building_df['registration_day'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[2]))
    new_columns = ['registration_year', 'registration_month', 'registration_day']

    ti.xcom_push(key='building_df', value=building_df.to_dict())
    ti.xcom_push(key='new_columns', value=new_columns)


def encode_building_usage(**kwargs):
    """This function used to encode the building usage"""
    ti = kwargs['ti']
    label_encoder = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(ti.xcom_pull(key='building_df', task_ids='get_building_data'))
    new_columns = ti.xcom_pull(key='new_columns', task_ids='get_building_data')

    delete_unused_xcoms(task_id='get_building_data', key='building_df')
    delete_unused_xcoms(task_id='get_building_data', key='new_columns')
    building_df['building_use_encoded'] = label_encoder.fit_transform(building_df['building_use'])

    new_columns.append('building_use_encoded')
    ti.xcom_push(key='building_df', value=building_df.to_dict())
    ti.xcom_push(key='new_columns', value=new_columns)


def scale_coordinates(**kwargs):
    """This function is used to scale Building Coordinates"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_building_usage')
    )
    new_columns = ti.xcom_pull(key='new_columns', task_ids='encode_building_usage')

    delete_unused_xcoms(task_id='encode_building_usage', key='building_df')
    delete_unused_xcoms(task_id='encode_building_usage', key='new_columns')

    building_df['latitude_scaled'] = scaler.fit_transform(building_df[['latitude']])
    building_df['longitude_scaled'] = scaler.fit_transform(building_df[['longitude']])

    new_columns.extend(['latitude_scaled', 'longitude_scaled'])
    ti.xcom_push(key='new_columns', value=new_columns)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def create_new_columns_buildings(**kwargs):
    """This function is used to alter building table and add new columns to insert transformed data"""
    ti = kwargs['ti']
    scylla_client = ScyllaClient()

    scylla_client.alter_table(table='building', column_name='registration_year', type='int')
    scylla_client.alter_table(table='building', column_name='registration_month', type='int')
    scylla_client.alter_table(table='building', column_name='registration_day', type='int')
    scylla_client.alter_table(table='building', column_name='building_use_encoded', type='int')
    scylla_client.alter_table(table='building', column_name='latitude_scaled', type='float')
    scylla_client.alter_table(table='building', column_name='longitude_scaled', type='float')
    new_columns = ti.xcom_pull(key='new_columns', task_ids='scaling_coords')
    delete_unused_xcoms(key='new_columns', task_id='scaling_coords')


def insert_transformed_building_data(**kwargs):
    """This function is used for inserting transformed data"""
    ti = kwargs['ti']
    scylla_client = ScyllaClient()
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scaling_coords')
    )
    for index, item in building_df.iterrows():
        sql_command = """
            UPDATE building
            SET registration_year = {registration_year},
            registration_month = {registration_month},
            registration_day = {registration_day},
            building_use_encoded = {building_use_encoded},
            latitude_scaled = {latitude_scaled},
            longitude_scaled = {longitude_scaled}
            WHERE registration_number = '{registration_number}'
        """.format(
            registration_year=item['registration_year'],
            registration_month=item['registration_month'],
            registration_day=item['registration_day'],
            building_use_encoded=item['building_use_encoded'],
            latitude_scaled=item['latitude_scaled'],
            longitude_scaled=item['longitude_scaled'],
            registration_number=item['registration_number']
        )
        scylla_client.update_table(sql_command=sql_command)


def process_labels(**kwargs):
    """This function is used for transforming co2_emissions rating labels"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    building_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.co2_emissions")
    encoded_labels = encoding_labels(df=building_df, column='label')
    ti.xcom_push(key='encoded_co2_emissions_df', value=encoded_labels.to_dict())


def scale_co2_emissions_ratio(**kwargs):
    """This function is used to scale CO2 emissions ratio"""
    ti = kwargs['ti']
    encoded_co2_emissions_label = pd.DataFrame(
        ti.xcom_pull(key='encoded_co2_emissions_df', task_ids='encode_co2_emissions_labels')
    )
    delete_unused_xcoms(task_id='encode_co2_emissions_labels', key='encoded_co2_emissions_df')
    scaled_co2_emissions_ratio_df = scale_ratio(column='ratio', df=encoded_co2_emissions_label)
    print(scaled_co2_emissions_ratio_df)


def encode_label_primary_consumption(**kwargs):
    """This function is used to encode the primary consumption rating"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    primary_cons_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.primary_consumption")
    primary_cons_encoded_df = encoding_labels(df=primary_cons_df, column='label')
    ti.xcom_push(key='primary_cons_encoded_df', value=primary_cons_encoded_df.to_dict())


def scale_prim_consumption_ratio(**kwargs):
    """This function is used to scale the primary consumption ratio"""
    ti = kwargs['ti']
    encoded_prim_cons_label = pd.DataFrame(
        ti.xcom_pull(key='primary_cons_encoded_df', task_ids='encode_primary_consumption_label_op')
    )
    delete_unused_xcoms(task_id='encode_primary_consumption_label_op', key='primary_cons_encoded_df')
    scaled_prim_cons_ratio = scale_ratio(column='ratio', df=encoded_prim_cons_label)
    print(scaled_prim_cons_ratio)


def encode_label_heating_demand(**kwargs):
    """This function is used to encode the heating demand rating"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    heating_demand_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.heating_demand")
    heating_demand_df['label'] = heating_demand_df['label'].apply(
        lambda label: 'unknown' if label in ['-', 'N.C.'] else label
    )
    heating_demand_encoded_df = encoding_labels(df=heating_demand_df, column='label')
    ti.xcom_push(key='heating_demand_encoded_df', value=heating_demand_encoded_df.to_dict())


def scale_heating_demand_ratio(**kwargs):
    """This function is used to scale the primary consumption ratio"""
    ti = kwargs['ti']
    heating_demand_encoded_df = pd.DataFrame(
        ti.xcom_pull(key='heating_demand_encoded_df', task_ids='encode_heating_demand_label_op')
    )
    delete_unused_xcoms(task_id='encode_heating_demand_label_op', key='heating_demand_encoded_df')
    scaled_prim_cons_ratio = scale_ratio(column='ratio', df=heating_demand_encoded_df)
    print(scaled_prim_cons_ratio)


def encode_label_cooling_demand(**kwargs):
    """This function is for encoding cooling demand ratings"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    cooling_demand_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.cooling_demand")
    cooling_demand_df['label'] = cooling_demand_df['label'].apply(
        lambda label: 'unknown' if label in ['-', 'N.C.'] else label
    )
    cooling_demand_encoded_df = encoding_labels(df=cooling_demand_df, column='label')
    ti.xcom_push(key='cooling_demand_encoded_df', value=cooling_demand_encoded_df.to_dict())


def scale_cooling_demand_ratio(**kwargs):
    """This function is used to scale provided cooling demand ratio"""
    ti = kwargs['ti']
    cooling_demand_encoded_df = pd.DataFrame(
        ti.xcom_pull(key='cooling_demand_encoded_df', task_ids='encode_cooling_demand_label_op')
    )
    delete_unused_xcoms(task_id='encode_cooling_demand_label_op', key='cooling_demand_encoded_df')
    scaled_cooling_demand_df = scale_ratio(column='ratio', df=cooling_demand_encoded_df)
    print(scaled_cooling_demand_df)


def encode_province_name(**kwargs):
    """This function used to encode the province names"""
    ti = kwargs['ti']
    label_encoder = preprocessing.LabelEncoder()
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    province_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.province")

    province_df['province'] = label_encoder.fit_transform(province_df['province'])
    print(province_df)
