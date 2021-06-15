
import pandas as pd
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler

from MongoDBClient.client import MongoDBClient
from utils import delete_unused_xcoms, encoding_labels, scale_ratio, alter_scylladb_tables, split_to_partitions, \
    load_energy_certificates, fill_na_energy_certificates, init_scylla_conn


def handle_dates(**kwargs):
    """This function is used to handle registration date"""
    ti = kwargs['ti']

    building_df = load_energy_certificates()
    building_df = fill_na_energy_certificates(building_df)

    building_df['registration_year'] = building_df['Registration date'].apply(lambda date: int(date.split('-')[0]))
    building_df['registration_month'] = building_df['Registration date'].apply(lambda date: int(date.split('-')[1]))
    building_df['registration_day'] = building_df['Registration date'].apply(lambda date: int(date.split('-')[2]))
    building_df = building_df.drop('Cooling demand rating', 1)

    building_df.rename(columns={'Cooling demand ratio.1': 'Cooling demand rating'}, inplace=True)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def encode_building_usage(**kwargs):
    """This function used to encode the building usage"""
    ti = kwargs['ti']
    label_encoder = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(ti.xcom_pull(key='building_df', task_ids='get_building_data'))

    delete_unused_xcoms(task_id='get_building_data', key='building_df')
    building_df['building_use_encoded'] = label_encoder.fit_transform(building_df['Building use'])
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_coordinates(**kwargs):
    """This function is used to scale Building Coordinates"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()
    province_encoder = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_building_usage')
    )

    delete_unused_xcoms(task_id='encode_building_usage', key='building_df')

    building_df['Province_encoded'] = province_encoder.fit_transform(building_df['Province'])
    building_df['latitude_scaled'] = scaler.fit_transform(building_df[['latitude']])
    building_df['longitude_scaled'] = scaler.fit_transform(building_df[['longitude']])
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def process_co2_emission_labels(**kwargs):
    """This function is used for transforming co2_emissions rating labels"""
    ti = kwargs['ti']
    co2_emission_label_encoder = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scaling_coords')
    )
    delete_unused_xcoms(task_id='scaling_coords', key='building_df')

    building_df['CO2 emitions Rating encoded'] = co2_emission_label_encoder.fit_transform(
        building_df['CO2 emitions Rating']
    )
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_co2_emissions_ratio(**kwargs):
    """This function is used to scale CO2 emissions ratio"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_co2_emissions_labels')
    )
    delete_unused_xcoms(task_id='encode_co2_emissions_labels', key='building_df')
    building_df = scale_ratio(column='CO2 emissions ratio', df=building_df)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def encode_label_primary_consumption(**kwargs):
    """This function is used to encode the primary consumption rating"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scale_co2_emissions_ratio_op')
    )
    delete_unused_xcoms(task_id='scale_co2_emissions_ratio_op', key='building_df')

    primary_energy_label_encoder = preprocessing.LabelEncoder()
    building_df['Primary energy label encoded'] = primary_energy_label_encoder.fit_transform(
        building_df['Primary energy label']
    )
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_prim_consumption_ratio(**kwargs):
    """This function is used to scale the primary consumption ratio"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_primary_consumption_label_op')
    )
    delete_unused_xcoms(task_id='encode_primary_consumption_label_op', key='building_df')
    building_df = scale_ratio(column='primary consumption ratio', df=building_df)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def encode_label_heating_demand(**kwargs):
    """This function is used to encode the heating demand rating"""
    ti = kwargs['ti']
    heating_demand_label_enc = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scale_prim_consumption_ratio_op')
    )
    delete_unused_xcoms(task_id='scale_prim_consumption_ratio_op', key='building_df')

    building_df['Heating demand rating'] = building_df['Heating demand rating'].apply(
        lambda label: 'unknown' if label in ['-', 'N.C.'] else label
    )
    building_df['Heating demand rating encoded'] = heating_demand_label_enc.fit_transform(
        building_df['Heating demand rating']
    )
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_heating_demand_ratio(**kwargs):
    """This function is used to scale the primary consumption ratio"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_heating_demand_label_op')
    )
    delete_unused_xcoms(task_id='encode_heating_demand_label_op', key='building_df')
    building_df = scale_ratio(column='Heating demand ratio', df=building_df)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def encode_label_cooling_demand(**kwargs):
    """This function is for encoding cooling demand ratings"""
    ti = kwargs['ti']
    cooling_demand_label_enc = preprocessing.LabelEncoder()

    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scale_heating_demand_ratio_op')
    )
    delete_unused_xcoms(task_id='scale_heating_demand_ratio_op', key='building_df')
    building_df['Cooling demand rating'] = building_df['Cooling demand rating'].fillna('unknown')

    building_df['Cooling demand rating'] = building_df['Cooling demand rating'].apply(
        lambda label: 'unknown' if label in ['-', 'N.C.', 'nan'] else label
    )
    building_df['Cooling demand rating encoded'] = cooling_demand_label_enc.fit_transform(
        building_df['Cooling demand rating']
    )
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_cooling_demand_ratio(**kwargs):
    """This function is used to scale provided cooling demand ratio"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_cooling_demand_label_op')
    )
    delete_unused_xcoms(task_id='encode_cooling_demand_label_op', key='building_df')
    building_df['Cooling demand ratio'] = building_df['Cooling demand ratio'].fillna(0.0)
    building_df = scale_ratio(column='Cooling demand ratio', df=building_df)
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def insert_transformed_building_data(**kwargs):
    """This function is used for inserting transformed data"""
    ti = kwargs['ti']
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='scale_cooling_demand_ratio_op')
    )
    mongo_client = MongoDBClient()
    collection_ = mongo_client.create_collection('eren_building')

    unique_years = list(building_df['registration_year'].unique())
    for year in unique_years:
        current_year_data = building_df.loc[building_df['registration_year'] == year]
        mongo_client.insert_many_(df=current_year_data, collection=collection_)
