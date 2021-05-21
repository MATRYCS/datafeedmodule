import os
import sys
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.cassandra.sensors.table import CassandraTableSensor
from airflow.sensors.filesystem import FileSensor

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from settings import ENERGY_EFFICIENCY_CERTS_PATH

from PythonProcessors.eren_certificates_processors import handle_dates, encode_building_usage, scale_coordinates, \
    process_co2_emission_labels, scale_co2_emissions_ratio, encode_label_primary_consumption, \
    scale_prim_consumption_ratio, \
    encode_label_heating_demand, scale_heating_demand_ratio, encode_label_cooling_demand, scale_cooling_demand_ratio, \
    insert_transformed_building_data

default_args = {
    'start_date': datetime(2021, 3, 1)
}

with DAG('eren_certificates_transformer',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    # EREN ECP Building tasks
    certificates_sensor_op = FileSensor(
        task_id='certificates_sensor',
        poke_interval=30,
        filepath=ENERGY_EFFICIENCY_CERTS_PATH
    )
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

    # EREN ECP co2 emission tasks
    encode_co2_emissions_labels = PythonOperator(
        task_id='encode_co2_emissions_labels',
        python_callable=process_co2_emission_labels
    )
    scale_co2_emissions_ratio_op = PythonOperator(
        task_id='scale_co2_emissions_ratio_op',
        python_callable=scale_co2_emissions_ratio
    )

    # EREN ECP primary consumption
    encode_primary_consumption_label_op = PythonOperator(
        task_id='encode_primary_consumption_label_op',
        python_callable=encode_label_primary_consumption
    )
    scale_prim_consumption_ratio_op = PythonOperator(
        task_id='scale_prim_consumption_ratio_op',
        python_callable=scale_prim_consumption_ratio
    )

    # EREN ECP heating demand
    encode_heating_demand_label_op = PythonOperator(
        task_id='encode_heating_demand_label_op',
        python_callable=encode_label_heating_demand
    )
    scale_heating_demand_ratio_op = PythonOperator(
        task_id='scale_heating_demand_ratio_op',
        python_callable=scale_heating_demand_ratio
    )

    # EREN ECP Cooling Demand
    encode_cooling_demand_label_op = PythonOperator(
        task_id='encode_cooling_demand_label_op',
        python_callable=encode_label_cooling_demand
    )
    scale_cooling_demand_ratio_op = PythonOperator(
        task_id='scale_cooling_demand_ratio_op',
        python_callable=scale_cooling_demand_ratio
    )

    # Insert data to Scylla
    insert_transformed_building_data_op = PythonOperator(
        task_id='insert_transformed_data_op',
        python_callable=insert_transformed_building_data
    )

    certificates_sensor_op >> building_data_operator >> encode_building_usage_op >> scaling_coords >> encode_co2_emissions_labels >> scale_co2_emissions_ratio_op >> encode_primary_consumption_label_op >> scale_prim_consumption_ratio_op >> encode_heating_demand_label_op >> scale_heating_demand_ratio_op >> encode_cooling_demand_label_op >> scale_cooling_demand_ratio_op >> insert_transformed_building_data_op
