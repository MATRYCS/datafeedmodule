import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import BTC_TOWER_DATASET
from PythonProcessors.btc_tower_processor import transform_dates, scale_numerical_vars, insert_btc_data

default_args = {
    'start_date': datetime(2021, 3, 29)
}

with DAG('btc_tower_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    btc_tower_sensor_op = FileSensor(
        task_id='btc_tower_sensor_op',
        poke_interval=30,
        filepath=BTC_TOWER_DATASET
    )
    handle_dates_op = PythonOperator(
        task_id='handle_dates',
        python_callable=transform_dates
    )
    scale_numerical_variables_op = PythonOperator(
        task_id='scale_numerical_vars',
        python_callable=scale_numerical_vars
    )
    store_data_op = PythonOperator(
        task_id='store_data_op',
        python_callable=insert_btc_data
    )

    btc_tower_sensor_op >> handle_dates_op >> scale_numerical_variables_op >> store_data_op
