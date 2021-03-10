import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from PythonProcessors.eren_cons_centers_processors import encode_cons_center_type, scale_cons_centers_numerical_vars

default_args = {
    'start_date': datetime(2021, 3, 1)
}
with DAG('eren_cons_centers',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    consumer_center_sensor = CassandraTableSensor(
        task_id='consumer_center_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.consumer_center'
    )
    encode_consumer_center_type_op = PythonOperator(
        task_id='encode_consumer_center_type_op',
        python_callable=encode_cons_center_type
    )
    scale_numerical_vars_cons_centers_op = PythonOperator(
        task_id='scale_numerical_vars_cons_centers_op',
        python_callable=scale_cons_centers_numerical_vars
    )

    # management_center_sensor = CassandraTableSensor(
    #     task_id='management_center_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.management_center'
    # )
    # monthly_diesel_consumption_sensor = CassandraTableSensor(
    #     task_id='monthly_diesel_consumption_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.management_center'
    # )
    # spending_center_sensor = CassandraTableSensor(
    #     task_id='spending_center_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.spending_center'
    # )
    # monthly_electricity_consumption_sensor = CassandraTableSensor(
    #     task_id='monthly_electricity_consumption_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.monthly_electricity_consumption'
    # )
    # monthly_gas_consumption_sensor = CassandraTableSensor(
    #     task_id='monthly_gas_consumption_sensor',
    #     cassandra_conn_id='matrycs_scylladb_conn',
    #     table='matrycs.monthly_gas_consumption'
    # )

    consumer_center_sensor >> encode_consumer_center_type_op >> scale_numerical_vars_cons_centers_op
