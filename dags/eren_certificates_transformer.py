from datetime import datetime

import pandas as pd
import psycopg2
from sklearn import preprocessing
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.providers.presto.hooks.presto import PrestoHook
from sklearn.preprocessing import MinMaxScaler

default_args = {
    'start_date': datetime(2021, 3, 1)
}


def delete_unused_xcoms(task_id, key):
    """This functions is used to delete unused xcoms"""
    conn = psycopg2.connect(
        "dbname={dbname} user={user} password={password} host={host} port={port}".format(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres',
            port=5432
        )
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM XCOM WHERE task_id='{}' AND key='{}'".format(task_id, key))
    conn.commit()
    cur.close()


def handle_dates(**kwargs):
    """This function is used to handle registration date"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    building_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.building")

    building_df['registration_year'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[0]))
    building_df['registration_month'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[1]))
    building_df['registration_day'] = building_df['registration_date'].apply(lambda date: int(date.split('-')[2]))

    ti.xcom_push(key='building_df', value=building_df.to_dict())


def encode_building_usage(**kwargs):
    """This function used to encode the building usage"""
    ti = kwargs['ti']
    label_encoder = preprocessing.LabelEncoder()
    building_df = pd.DataFrame(ti.xcom_pull(key='building_df', task_ids='get_building_data'))
    delete_unused_xcoms(task_id='get_building_data', key='building_df')
    building_df['building_use_encoded'] = label_encoder.fit_transform(building_df['building_use'])
    ti.xcom_push(key='building_df', value=building_df.to_dict())


def scale_coordinates(**kwargs):
    """This function is used to scale Building Coordinates"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()
    building_df = pd.DataFrame(
        ti.xcom_pull(key='building_df', task_ids='encode_building_usage')
    )
    delete_unused_xcoms(task_id='encode_building_usage', key='building_df')
    building_df['latitude_scaled'] = scaler.fit_transform(building_df[['latitude']])
    building_df['longitude_scaled'] = scaler.fit_transform(building_df[['longitude']])
    print(building_df)


def encoding_labels(**kwargs):
    """This function used for one hot encoding provided column for the provided pd.DataFrame"""
    df = kwargs['df']
    column = kwargs['column']
    encoded_df = pd.get_dummies(df, columns=[column])
    encoded_df[column] = df[column]
    return encoded_df


def process_labels(**kwargs):
    """This function is used for transforming co2_emissions rating labels"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    building_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.co2_emissions")
    encoded_labels = encoding_labels(df=building_df, column='label')
    ti.xcom_push(key='encoded_co2_emissions_df', value=encoded_labels.to_dict())


def scale_ratio(**kwargs):
    """This function is used to scale provided ratio"""
    df = kwargs['df']
    column = kwargs['column']
    scaler = MinMaxScaler()
    df['{}_scaled'.format(column)] = scaler.fit_transform(df[[column]])
    return df


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




with DAG('eren_certificates_transformer',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    buildings_sensor = CassandraTableSensor(
        task_id='building_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.building'
    )
    co2_emissions_sensor = CassandraTableSensor(
        task_id='co2_emissions_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.co2_emissions'
    )
    primary_consumption_sensor = CassandraTableSensor(
        task_id='primary_consumption_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.primary_consumption'
    )
    heating_demand_sensor = CassandraTableSensor(
        task_id='heating_demand_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.heating_demand'
    )
    # cooling_demand_sensor = CassandraTableSensor(
    #     task_id='cooling_demand_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.cooling_demand'
    # )
    # province_sensor = CassandraTableSensor(
    #     task_id='province_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.province'
    # )

    # municipality_sensor = CassandraTableSensor(
    #     task_id='municipality_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.municipality'
    # )
    building_data_operator = PythonOperator(
        task_id="get_building_data",
        python_callable=handle_dates,
    )
    encode_building_usage_op = PythonOperator(
        task_id='encode_building_usage',
        python_callable=encode_building_usage
    )
    scaling_coords = PythonOperator(
        task_id='scaling_coords',
        python_callable=scale_coordinates
    )

    encode_co2_emissions_labels = PythonOperator(
        task_id='encode_co2_emissions_labels',
        python_callable=process_labels
    )
    scale_co2_emissions_ratio_op = PythonOperator(
        task_id='scale_co2_emissions_ratio_op',
        python_callable=scale_co2_emissions_ratio
    )

    encode_primary_consumption_label_op = PythonOperator(
        task_id='encode_primary_consumption_label_op',
        python_callable=encode_label_primary_consumption
    )
    scale_prim_consumption_ratio_op = PythonOperator(
        task_id='scale_prim_consumption_ratio_op',
        python_callable=scale_prim_consumption_ratio
    )

    encode_heating_demand_label_op = PythonOperator(
        task_id='encode_heating_demand_label_op',
        python_callable=encode_label_heating_demand
    )
    scale_heating_demand_ratio_op = PythonOperator(
        task_id='scale_heating_demand_ratio_op',
        python_callable=scale_heating_demand_ratio
    )

    buildings_sensor >> building_data_operator >> encode_building_usage_op >> scaling_coords
    co2_emissions_sensor >> encode_co2_emissions_labels >> scale_co2_emissions_ratio_op
    primary_consumption_sensor >> encode_primary_consumption_label_op >> scale_prim_consumption_ratio_op
    heating_demand_sensor >> encode_heating_demand_label_op >> scale_heating_demand_ratio_op
