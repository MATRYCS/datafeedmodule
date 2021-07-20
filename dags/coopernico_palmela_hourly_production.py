import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor


sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import PALMELA_HOURLY_PRODUCTION
from MongoDBClient.collection_handler import CollectionHandler


from PythonProcessors.coopernico_palmela_processor import handle_null_values, scale_numerical_vars, \
    store_palmela_hourly_data, check_if_file_is_processed

default_args = {
    'start_date': datetime(2021, 3, 29)
}


def if_new_files():
    # add extra logic here
    files = os.listdir(PALMELA_HOURLY_PRODUCTION)
    if files:
        return 'process'
    else:
        return 'stop_execution'


with DAG('coopernico_palmela_hourly_production',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    # coopernico_files_consumed = Variable.get('files', deserialize_json=True)
    # if 'COOPERNICO' in coopernico_files_consumed.keys():

    palmela_hourly_production_sensor_op = FileSensor(
        task_id='palmela_hourly_production_sensor',
        poke_interval=30,
        filepath=PALMELA_HOURLY_PRODUCTION
    )

    decide_to_process = BranchPythonOperator(
        task_id='decide_to_process',
        python_callable=if_new_files
    )

    stop_execution = DummyOperator(
        task_id="stop_execution",
        dag=dag
    )

    process = DummyOperator(
        task_id="process",
        dag=dag
    )

    files = os.listdir(PALMELA_HOURLY_PRODUCTION)

    for index in range(len(files)):
        current_file = files[index]
        solar_plant_name = current_file.split('-')[0]

        if_already_processed_file = BranchPythonOperator(
            task_id='if_is_processed_{}'.format(index),
            python_callable=check_if_file_is_processed,
            op_kwargs={
                'file': current_file,
                'index': index
            }
        )

        abort_file_op = DummyOperator(
            task_id='abort_{}'.format(index)
        )

        handle_null_values_op = PythonOperator(
            task_id='handle_null_values_{}'.format(index),
            python_callable=handle_null_values,
            op_kwargs={
                'file': os.path.join(PALMELA_HOURLY_PRODUCTION, current_file),
                'solar_plant': solar_plant_name
            }
        )
        scale_numerical_variables_op = PythonOperator(
            task_id='scale_numerical_variables_{}'.format(index),
            op_kwargs={'previous_task': 'handle_null_values_{}'.format(index)},
            python_callable=scale_numerical_vars
        )
        store_data_op = PythonOperator(
            task_id='store_data_{}'.format(index),
            op_kwargs={'previous_task': 'scale_numerical_variables_{}'.format(index)},
            python_callable=store_palmela_hourly_data
        )
        process >> if_already_processed_file >> abort_file_op
        process >> if_already_processed_file >> handle_null_values_op >> scale_numerical_variables_op >> store_data_op

    palmela_hourly_production_sensor_op >> decide_to_process
    decide_to_process >> stop_execution
    decide_to_process >> process
