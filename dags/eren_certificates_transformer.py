import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from PythonProcessors.eren_certificates_processors import handle_dates, encode_building_usage, scale_coordinates, \
    process_labels, scale_co2_emissions_ratio, encode_label_primary_consumption, scale_prim_consumption_ratio, \
    encode_label_heating_demand, scale_heating_demand_ratio, encode_label_cooling_demand, scale_cooling_demand_ratio

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
    cooling_demand_sensor = CassandraTableSensor(
        task_id='cooling_demand_sensor',
        cassandra_conn_id='matrycs_scylladb_conn',
        table='matrycs.cooling_demand'
    )
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

    encode_cooling_demand_label_op = PythonOperator(
        task_id='encode_cooling_demand_label_op',
        python_callable=encode_label_cooling_demand
    )
    scale_cooling_demand_ratio_op = PythonOperator(
        task_id='scale_cooling_demand_ratio_op',
        python_callable=scale_cooling_demand_ratio
    )

    buildings_sensor >> building_data_operator >> encode_building_usage_op >> scaling_coords
    co2_emissions_sensor >> encode_co2_emissions_labels >> scale_co2_emissions_ratio_op
    primary_consumption_sensor >> encode_primary_consumption_label_op >> scale_prim_consumption_ratio_op
    heating_demand_sensor >> encode_heating_demand_label_op >> scale_heating_demand_ratio_op
    cooling_demand_sensor >> encode_cooling_demand_label_op >> scale_cooling_demand_ratio_op
