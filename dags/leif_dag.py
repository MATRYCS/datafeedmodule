import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import BTC_TOWER_DATASET, LEIF_KPFI_DATASET
from PythonProcessors.leif_preprocessors import handle_dates_kpfi_projects, projects_numerical_values, \
    scale_kpfi_activities, scale_kpfi_data, store_leif_projects, store_leif_activities, store_leif_data

default_args = {
    'start_date': datetime(2021, 3, 29)
}



with DAG('leif_kpfi_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    leif_kpfi_sensor_dag = FileSensor(
        task_id='leif_kpfi_sensor_op',
        poke_interval=30,
        filepath=LEIF_KPFI_DATASET
    )
    # Handle Leif Projects
    handle_leif_project_dates = PythonOperator(
        task_id='handle_project_dates_op',
        python_callable=handle_dates_kpfi_projects
    )
    scale_leif_project_numericals = PythonOperator(
        task_id='scale_leif_project_num_op',
        python_callable=projects_numerical_values
    )
    store_leif_projects = PythonOperator(
        task_id='store_leif_projects',
        python_callable=store_leif_projects
    )
    # Handle KPFI Activities
    scale_leif_kpfi_activities = PythonOperator(
        task_id='scale_leif_kpfi_activities_op',
        python_callable=scale_kpfi_activities
    )
    store_leif_activities = PythonOperator(
        task_id='store_leif_activities_op',
        python_callable=store_leif_activities
    )
    # Handle Leif Data
    scaled_leif_data = PythonOperator(
        task_id='scale_leif_data_op',
        python_callable=scale_kpfi_data
    )
    store_leif_data = PythonOperator(
        task_id='store_leif_data_op',
        python_callable=store_leif_data
    )

    leif_kpfi_sensor_dag >> handle_leif_project_dates >> scale_leif_project_numericals >> store_leif_projects
    leif_kpfi_sensor_dag >> scale_leif_kpfi_activities >> store_leif_activities
    leif_kpfi_sensor_dag >> scaled_leif_data >> store_leif_data
