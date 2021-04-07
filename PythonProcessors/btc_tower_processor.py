import pandas as pd
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from sklearn.preprocessing import MinMaxScaler

from models.BTC.models import BtcTower
from settings import BTC_TOWER_DATASET
from utils import delete_unused_xcoms, split_to_partitions, init_scylla_conn


def handle_dates(row):
    """
    This function is used for transforming dates to year/month/day/hour
    """
    return row.year, row.month, row.day, row.hour


def load_btc_tower_dataset():
    """This function is used to load BTC TOWER DATASET

    Returns: pd.DataFrame
        btc tower dataframe
    """
    btc_tower_df = pd.read_excel(
        BTC_TOWER_DATASET,
        usecols=['TIMESTAMP', 'LOCATION', 'ENERGY_SOURCE', 'MEASURE', 'UNIT_OF_MEASURE', 'INTERVAL', 'VALUE'],
        parse_dates=['TIMESTAMP'],
        engine='openpyxl',
    )
    return btc_tower_df


def transform_dates(**kwargs):
    """This function is used to transform  dates

    Parameters:
        kwargs: keyword arguments
            Provided kwarg arguments

    Returns: pd.Dataframe
        transformed pandas.DataFrame
    """
    ti = kwargs['ti']
    btc_tower_df = load_btc_tower_dataset()
    btc_tower_df['Year'], btc_tower_df['Month'], btc_tower_df['Day'], \
    btc_tower_df['Hour'] = zip(*btc_tower_df['TIMESTAMP'].apply(handle_dates))
    btc_tower_df['TIMESTAMP'] = btc_tower_df['TIMESTAMP'].astype('string')

    ti.xcom_push(key='btc_tower_df', value=btc_tower_df.to_dict())


def scale_numerical_vars(**kwargs):
    """
    This function is used to scale numerical variables
    :param kwargs: provided kwargs
    :return: pd.Dataframe
    """
    ti = kwargs['ti']
    scaler = MinMaxScaler()
    btc_tower_df = pd.DataFrame(ti.xcom_pull(
        key='btc_tower_df',
        task_ids='handle_dates')
    )
    btc_tower_df['VALUE'] = scaler.fit_transform(btc_tower_df[['VALUE']])
    delete_unused_xcoms(task_id='handle_dates', key='btc_tower_df')

    ti.xcom_push(key='btc_tower_df', value=btc_tower_df.to_dict())


def insert_btc_data(**kwargs):
    """This function is used to insert btc tower data to Scylla"""
    ti = kwargs['ti']

    btc_tower_df = pd.DataFrame(ti.xcom_pull(
        key='btc_tower_df',
        task_ids='scale_numerical_vars')
    )
    delete_unused_xcoms(task_id='scale_numerical_vars', key='btc_tower_df')

    partitioned_btc_data = split_to_partitions(btc_tower_df, 1000)
    init_scylla_conn()
    sync_table(BtcTower)

    # Upload Batch data to ScyllaDB
    num_of_partitions = 0
    for partition in partitioned_btc_data:
        print("Loading {}/{} partition to ScyllaDB".format(num_of_partitions, 1000))
        with BatchQuery() as b:
            for index, item in partition.iterrows():
                BtcTower.batch(b).create(
                    Timestamp=item['TIMESTAMP'],
                    Year=item['Year'],
                    Month=item['Month'],
                    Day=item['Day'],
                    Hour=item['Hour'],
                    Location=item['LOCATION'],
                    energy_source=item['ENERGY_SOURCE'],
                    interval=item['INTERVAL'],
                    value=item['VALUE'],
                    unit_of_measure=item['UNIT_OF_MEASURE'],
                    measure=item['MEASURE']
                )
        num_of_partitions += 1
