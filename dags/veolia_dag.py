import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import excel_file_path, VEOLIA_FILE_PATH
from PythonProcessors.veolia_processor import combine_dfs, insert_veolia_data, check_if_file_is_processed, \
    load_and_insert

default_args = {
    'start_date': datetime(2021, 9, 21)
}


def if_new_files():
    # add extra logic here
    files = os.listdir(VEOLIA_FILE_PATH)
    if files:
        return 'process'
    else:
        return 'stop_execution'


with DAG('veolia_dag',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    # if_veolia_data_exists_op = FileSensor(
    #     task_id='if_veolia_data_exists',
    #     poke_interval=30,
    #     filepath=VEOLIA_FILE_PATH
    # )
    if_new_files = BranchPythonOperator(
        task_id='if_new_files',
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

    files = os.listdir(VEOLIA_FILE_PATH)
    for index in range(len(files)):
        current_file = files[index]

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

        # get_data_op = PythonOperator(
        #     task_id="load_veolia_data_{}".format(index),
        #     python_callable=combine_dfs,
        #     op_kwargs={'previous_task': 'if_is_processed_{}'.format(index)}
        # )
        insert_veolia_data_op = PythonOperator(
            task_id="insert_veolia_{}".format(index),
            python_callable=load_and_insert,
            op_kwargs={'previous_task': 'if_is_processed_{}'.format(index)}
        )
        process >> if_already_processed_file >> abort_file_op
        process >> if_already_processed_file >> insert_veolia_data_op

    # if_veolia_data_exists_op
    if_new_files >> stop_execution
    if_new_files >> process
