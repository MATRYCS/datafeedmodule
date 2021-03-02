from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor

default_args = {
    'start_date': datetime(2021, 3, 1)
}
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
    cooling_demand_sensor = CassandraTableSensor(
        task_id='cooling_demand_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.cooling_demand'
    )
    heating_demand_sensor = CassandraTableSensor(
        task_id='heating_demand_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.heating_demand'
    )
    province_sensor = CassandraTableSensor(
        task_id='province_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.province'
    )
    primary_consumption_sensor = CassandraTableSensor(
        task_id='primary_consumption_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.primary_consumption'
    )
    municipality_sensor = CassandraTableSensor(
        task_id='municipality_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.municipality'
    )

    accurate = DummyOperator(
        task_id='accurate'
    )
    inaccurate = DummyOperator(
        task_id='inaccurate'
    )

    buildings_sensor >> accurate
    co2_emissions_sensor >> inaccurate

