import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import PALMELA_HOURLY_PRODUCTION
from PythonProcessors.coopernico_palmela_processor import handle_null_values, scale_numerical_vars, \
    store_palmela_hourly_data

default_args = {
    'start_date': datetime(2021, 3, 29)
}

with DAG('coopernico_palmela_hourly_production',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    palmela_hourly_production_sensor_op = FileSensor(
        task_id='palmela_hourly_production_sensor',
        poke_interval=30,
        filepath=PALMELA_HOURLY_PRODUCTION
    )

    handle_null_values_op = PythonOperator(
        task_id='handle_null_values',
        python_callable=handle_null_values
    )

    scale_numerical_variables_op = PythonOperator(
        task_id='scale_numerical_variables',
        python_callable=scale_numerical_vars
    )

    store_data_op = PythonOperator(
        task_id='store_date',
        python_callable=store_palmela_hourly_data
    )

    palmela_hourly_production_sensor_op >> handle_null_values_op >> scale_numerical_variables_op >> store_data_op
