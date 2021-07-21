import pandas as pd
from sklearn.preprocessing import MinMaxScaler

from MongoDBClient.client import MongoDBClient
from MongoDBClient.collection_handler import CollectionHandler
from utils import read_palmela_hourly_production, delete_unused_xcoms, split_to_partitions, init_scylla_conn, \
    delete_previous_xcoms, remove_xcoms_after_run


def handle_dates(row):
    """
    This function is used for transforming dates
    :param palmela_hourly_production:
    :return:
    """
    return row.year, row.month, row.day, row.hour


def check_if_file_is_processed(**kwargs):
    ti = kwargs['ti']
    file = kwargs['file']
    index = kwargs['index']
    collection_handler = CollectionHandler()
    status = collection_handler.append_processed_files(file_name=file)
    remove_xcoms_after_run(task_ids=[
        'palmela_hourly_production_sensor',
        'decide_to_process',
        'stop_execution',
        'process'
    ])
    if status:
        return 'handle_null_values_{}'.format(index)
    else:
        return 'abort_{}'.format(index)


def handle_null_values(**kwargs):
    """
    This function is used for handling null values in Palmela Hourly Production
    :param kwargs: provided kwargs
    :return: None
    """
    ti = kwargs['ti']
    file = kwargs['file']
    solar_plant = kwargs['solar_plant']
    previous_task = kwargs['previous_task']

    palmela_hourly_production = read_palmela_hourly_production(file)
    palmela_hourly_production = palmela_hourly_production.rename(columns={
        'Data': 'Date',
        ' CO2 Evitado': 'Avoided CO2',
        ' Produzida': 'Produced',
        ' Específica': 'Specific'
    })
    palmela_hourly_production['solar_plant'] = [solar_plant] * len(palmela_hourly_production)

    palmela_hourly_production['Avoided CO2'] = palmela_hourly_production['Avoided CO2']. \
        replace(' ', '0.0').astype('float')
    palmela_hourly_production['Produced'] = palmela_hourly_production['Produced']. \
        replace(' ', '0.0').astype('float')
    palmela_hourly_production['Specific'] = palmela_hourly_production['Specific']. \
        replace(' ', '0.0').astype('float')

    palmela_hourly_production['Year'], palmela_hourly_production['Month'], palmela_hourly_production['Day'], \
    palmela_hourly_production['Hour'] = zip(*palmela_hourly_production['Date'].apply(handle_dates))
    palmela_hourly_production['Date'] = palmela_hourly_production['Date'].astype('string')
    palmela_hourly_production = palmela_hourly_production.drop([' '], axis=1)
    delete_previous_xcoms(task_id=previous_task)
    ti.xcom_push(key='palmela_hourly_production', value=palmela_hourly_production.to_dict())


def scale_numerical_vars(**kwargs):
    """
    This function is used for scaling numerical variables in palmela hourly production dataset
    :param kwargs: provided kwargs
    :return: None
    """
    ti = kwargs['ti']
    previous_task = kwargs['previous_task']
    scaler = MinMaxScaler()
    palmela_hourly_production = pd.DataFrame(ti.xcom_pull(
        key='palmela_hourly_production',
        task_ids=previous_task)
    )
    delete_unused_xcoms(task_id=previous_task, key='palmela_hourly_production')
    delete_previous_xcoms(task_id=previous_task)
    palmela_hourly_production[['Avoided CO2', 'Produced_scaled', 'Specific']] = scaler.fit_transform(
        palmela_hourly_production[['Avoided CO2', 'Produced', 'Specific']])
    palmela_hourly_production = palmela_hourly_production.drop_duplicates()
    ti.xcom_push(key='palmela_hourly_production', value=palmela_hourly_production.to_dict())


def store_palmela_hourly_data(**kwargs):
    """
    This function is used for storing palmela hourly production data
    :param kwargs: provided kwargs
    :return: None
    """
    ti = kwargs['ti']
    previous_task = kwargs['previous_task']

    palmela_hourly_production = pd.DataFrame(ti.xcom_pull(
        key='palmela_hourly_production',
        task_ids=previous_task)
    ).rename(columns={
        'Date': 'timestamp',
        'Year': 'year',
        'Month': 'month',
        'Day': 'day',
        'Hour': 'hour',
        'Produced': 'produced',
        'Produced_scaled': 'produced_scaled',
        'Avoided CO2': 'avoided_co2'
    })
    delete_unused_xcoms(task_id=previous_task, key='palmela_hourly_production')
    delete_previous_xcoms(task_id=previous_task)
    mongo_client = MongoDBClient()
    collection_ = mongo_client.create_collection('coopernico_solar_plants')
    mongo_client.insert_many_(df=palmela_hourly_production, collection=collection_)
    mongo_client.close_connection()