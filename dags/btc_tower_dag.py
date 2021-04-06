import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import BTC_TOWER_DATASET


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

    btc_tower_sensor_op
