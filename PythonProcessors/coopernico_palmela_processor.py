import pandas as pd
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from sklearn.preprocessing import MinMaxScaler

from models.Coopernico.models import PalmelaHourlyProduction
from utils import read_palmela_hourly_production, delete_unused_xcoms, split_to_partitions, init_scylla_conn


def handle_dates(row):
    """
    This function is used for transforming dates
    :param palmela_hourly_production:
    :return:
    """
    return row.year, row.month, row.day, row.hour


def handle_null_values(**kwargs):
    """
    This function is used for handling null values in Palmela Hourly Production
    :param kwargs: provided kwargs
    :return: None
    """
    ti = kwargs['ti']
    palmela_hourly_production = read_palmela_hourly_production()
    palmela_hourly_production = palmela_hourly_production.rename(columns={
        'Data': 'Date',
        ' CO2 Evitado': 'Avoided CO2',
        ' Produzida': 'Produced',
        ' Específica': 'Specific'
    })

    palmela_hourly_production['Avoided CO2'] = palmela_hourly_production['Avoided CO2']. \
        replace(' ', '0.0').astype('float')
    palmela_hourly_production['Produced'] = palmela_hourly_production['Produced']. \
        replace(' ', '0.0').astype('float')
    palmela_hourly_production['Specific'] = palmela_hourly_production['Specific']. \
        replace(' ', '0.0').astype('float')

    palmela_hourly_production['Year'], palmela_hourly_production['Month'], palmela_hourly_production['Day'], \
    palmela_hourly_production['Hour'] = zip(*palmela_hourly_production['Date'].apply(handle_dates))
    palmela_hourly_production['Date'] = palmela_hourly_production['Date'].astype('string')

    ti.xcom_push(key='palmela_hourly_production', value=palmela_hourly_production.to_dict())


def scale_numerical_vars(**kwargs):
    """
    This function is used for scaling numerical variables in palmela hourly production dataset
    :param kwargs: provided kwargs
    :return: None
    """
    ti = kwargs['ti']
    scaler = MinMaxScaler()
    palmela_hourly_production = pd.DataFrame(ti.xcom_pull(
        key='palmela_hourly_production',
        task_ids='handle_null_values')
    )
    delete_unused_xcoms(task_id='handle_null_values', key='palmela_hourly_production')
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

    palmela_hourly_production = pd.DataFrame(ti.xcom_pull(
        key='palmela_hourly_production',
        task_ids='scale_numerical_variables')
    )
    delete_unused_xcoms(task_id='scale_numerical_variables', key='palmela_hourly_production')

    partitioned_palmela_data = split_to_partitions(palmela_hourly_production, 100)
    init_scylla_conn()
    sync_table(PalmelaHourlyProduction)

    # Upload Batch data to ScyllaDB
    num_of_partitions = 0
    for partition in partitioned_palmela_data:
        print("Loading {}/{} partition to ScyllaDB".format(num_of_partitions, 100))
        with BatchQuery() as b:
            for index, item in partition.iterrows():
                PalmelaHourlyProduction.batch(b).create(
                    Timestamp=item['Date'],
                    Year=item['Year'],
                    Month=item['Month'],
                    Day=item['Day'],
                    Hour=item['Hour'],
                    Produced=item['Produced'],
                    Produced_scaled=item['Produced_scaled'],
                    Specific=item['Specific'],
                    AvoidedCO2=item['Avoided CO2']
                )
        num_of_partitions += 1
