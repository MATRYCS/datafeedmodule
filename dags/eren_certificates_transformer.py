from datetime import datetime

import pandas as pd
import psycopg2
from sklearn import preprocessing
from airflow import DAG
from airflow.operators.dummy import DummyOperator
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
    # co2_emissions_sensor = CassandraTableSensor(
    #     task_id='co2_emissions_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.co2_emissions'
    # )
    # cooling_demand_sensor = CassandraTableSensor(
    #     task_id='cooling_demand_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.cooling_demand'
    # )
    # heating_demand_sensor = CassandraTableSensor(
    #     task_id='heating_demand_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.heating_demand'
    # )
    # province_sensor = CassandraTableSensor(
    #     task_id='province_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.province'
    # )
    # primary_consumption_sensor = CassandraTableSensor(
    #     task_id='primary_consumption_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.primary_consumption'
    # )
    # municipality_sensor = CassandraTableSensor(
    #     task_id='municipality_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.municipality'
    # )

    # accurate = DummyOperator(
    #     task_id='accurate'
    # )
    # inaccurate = DummyOperator(
    #     task_id='inaccurate'
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

    buildings_sensor >> building_data_operator >> encode_building_usage_op >> scaling_coords
    # co2_emissions_sensor >> inaccurate
