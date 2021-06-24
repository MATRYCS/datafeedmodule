import pandas as pd
from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from sklearn.preprocessing import MinMaxScaler

from MongoDBClient.client import MongoDBClient
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
    btc_tower_df['VALUE_scaled'] = scaler.fit_transform(btc_tower_df[['VALUE']])
    btc_tower_df = btc_tower_df.drop_duplicates(subset=['TIMESTAMP'])
    delete_unused_xcoms(task_id='handle_dates', key='btc_tower_df')

    ti.xcom_push(key='btc_tower_df', value=btc_tower_df.to_dict())


def insert_btc_data(**kwargs):
    """This function is used to insert btc tower data to Scylla"""
    ti = kwargs['ti']

    btc_tower_df = pd.DataFrame(ti.xcom_pull(
        key='btc_tower_df',
        task_ids='scale_numerical_vars')
    ).rename(columns={
        'TIMESTAMP': 'timestamp',
        'Year': 'year',
        'Month': 'month',
        'Day': 'Day',
        'Hour': 'hour',
        'LOCATION': 'location',
        'ENERGY_SOURCE': 'energy_source',
        'INTERVAL': 'interval',
        'VALUE': 'value',
        'UNIT_OF_MEASURE': 'unit_of_measure',
        'MEASURE': 'measure'
    })
    delete_unused_xcoms(task_id='scale_numerical_vars', key='btc_tower_df')
    mongo_client = MongoDBClient()
    collection_ = mongo_client.create_collection('btc_tower')
    mongo_client.insert_many_(df=btc_tower_df, collection=collection_)
