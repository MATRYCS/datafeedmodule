import re

import psycopg2
import pandas as pd
import numpy as np
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT, Cluster
from cassandra.cqlengine import management, connection
from cassandra.cqlengine.management import sync_table
from cassandra.policies import RetryPolicy

from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler

from settings import ENERGY_EFFICIENCY_CERTS_PATH, CONNECTION_NAME, KEY_SPACE, PALMELA_HOURLY_PRODUCTION, \
    LEIF_KPFI_DATASET, LEIF_DATA_COLUMNS


def delete_unused_xcoms(task_id, key):
    """This functions is used to delete unused xcoms"""
    conn = psycopg2.connect(
        "dbname={dbname} user={user} password={password} host={host} port={port}".format(
            dbname='airflow',
            user='airflow',
            password='airflow',
            host='postgres',
            port=5432
        )
    )
    cur = conn.cursor()
    cur.execute("DELETE FROM XCOM WHERE task_id='{}' AND key='{}'".format(task_id, key))
    conn.commit()
    cur.close()


def categorical_encoding(**kwargs):
    """This function is used for label encoding"""
    df = kwargs['df']
    column = kwargs['column']
    label_encoder = preprocessing.LabelEncoder()
    df[column] = label_encoder.fit_transform(df[column])
    return df


def encoding_labels(**kwargs):
    """This function used for one hot encoding provided column for the provided pd.DataFrame"""
    df = kwargs['df']
    column = kwargs['column']
    encoded_df = pd.get_dummies(df, columns=[column])
    encoded_df[column] = df[column]
    return encoded_df


def scale_ratio(**kwargs):
    """This function is used to scale provided ratio"""
    df = kwargs['df']
    column = kwargs['column']
    scaler = MinMaxScaler()
    df['{}_scaled'.format(column)] = scaler.fit_transform(df[[column]])
    return df


def map_months(**kwargs):
    """This function is used to transform month to number"""
    month_mapper = {
        'January': 1,
        'February': 2,
        'March': 3,
        'April': 4,
        'May': 5,
        'June': 6,
        'July': 7,
        'August': 8,
        'September': 9,
        'October': 10,
        'November': 11,
        'December': 12
    }
    df = kwargs['df']
    column = kwargs['column']
    df['{}_encoded'.format(column)] = df[column].apply(lambda row: month_mapper[row])
    return df


def alter_scylladb_tables(**kwargs):
    """This function is used for altering tables in Matrycs ScyllaDB"""
    table = kwargs['table']
    column_name = kwargs['column_name']
    type = kwargs['type']
    try:
        exec_profile = ExecutionProfile(
            consistency_level=ConsistencyLevel.LOCAL_QUORUM,
            serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
            request_timeout=90)
        cluster = Cluster(['matrycs.epu.ntua.gr'], port=9042, execution_profiles={EXEC_PROFILE_DEFAULT: exec_profile})
        session = cluster.connect()
        session.set_keyspace("matrycs")
        session.execute(
            "ALTER TABLE {table} ADD {column_name} {type} ".format(table=table, column_name=column_name, type=type)
        )
    except Exception as ex:
        print(ex)


def split_to_partitions(df, partition_number):
    """This function is used to split to partitions the provided dataFrame
    Args:
        df: provided dataframe
        partition_number: number of partitions
    Returns:
        list of pandas dataframes
    """
    permuted_indices = np.random.permutation(len(df))
    partitions = []
    for i in range(partition_number):
        partitions.append(df.iloc[permuted_indices[i::partition_number]])
    return partitions


def load_energy_certificates():
    """
    This function is used to load energy efficiency certificates from path source

    Args:
        path: provided path

    Returns: energy efficiency pandas dataFrame

    """
    energy_cert_data = pd.read_csv(
        # "C:/Users/pkapsalis.EPU/PycharmProjects/DataFeed/data/EREN/certificados-de-eficiencia-energetica.csv",
        ENERGY_EFFICIENCY_CERTS_PATH,
        sep=';',
        dtype={'primary consumption ratio': 'float',
               'CO2 emissions ratio': 'float',
               'Cooling demand ratio': 'float',
               'Heating demand ratio': 'float'
               })
    return energy_cert_data


def fill_na_energy_certificates(certificates_df):
    """
    This function is used to fill NaN values in Energy certificates DF
    :param certificates_df: provided energy certificates pd.DataFrame
    :return: transformed dataFrame
    """
    certificates_df['primary consumption ratio'] = certificates_df['primary consumption ratio'].fillna(0)
    certificates_df['Primary energy label'] = certificates_df['Primary energy label'].fillna('unknown')
    certificates_df['CO2 emissions ratio'] = certificates_df['CO2 emissions ratio'].fillna(0)
    certificates_df['Heating demand ratio'] = certificates_df['Heating demand ratio'].fillna(0)
    certificates_df['Heating demand rating'] = certificates_df['Heating demand rating'].fillna('unknown')
    certificates_df['Cooling demand ratio'] = certificates_df['Cooling demand ratio'].fillna(0)
    certificates_df['Cooling demand rating'] = certificates_df['Cooling demand ratio.1'].fillna('unknown')
    return certificates_df


def read_palmela_hourly_production(file_path):
    """
    This function is used for loading Coopernico's Palmela Hourly Production
    :return: pd.DataFrame
    """
    palmela_hourly_df = pd.read_csv(file_path, parse_dates=['Data'])
    return palmela_hourly_df


def init_scylla_conn():
    """
    This function is used for initializing ScyllaDB connection
    """
    exec_profile = ExecutionProfile(
        request_timeout=100000,
        consistency_level=ConsistencyLevel.LOCAL_ONE,
        serial_consistency_level=ConsistencyLevel.LOCAL_SERIAL,
    )
    profiles = {'node1': exec_profile}
    cluster = Cluster(["matrycs.epu.ntua.gr"], execution_profiles=profiles, connect_timeout=100000)
    session = cluster.connect()
    connection.register_connection(CONNECTION_NAME, session=session)

    management.create_keyspace_simple(
        KEY_SPACE,
        connections=[CONNECTION_NAME],
        replication_factor=3
    )
    # sync_table(Building)


def load_leif_data(sheet_name, date_columns=None):
    """This function is used to load LEIF projects"""
    if date_columns:
        leif_projects = pd.read_excel(
            LEIF_KPFI_DATASET,
            engine='openpyxl',
            sheet_name=sheet_name,
            parse_dates=date_columns
        )
    else:
        leif_projects = pd.read_excel(
            LEIF_KPFI_DATASET,
            engine='openpyxl',
            sheet_name=sheet_name
        )
    return leif_projects


def load_leif_data_spec():
    """This function is used for loading LEIF data"""
    leif_data = pd.read_excel(
        LEIF_KPFI_DATASET,
        engine='openpyxl',
        header=None,
        skiprows=1,
        sheet_name='DATA'
    )
    leif_data = leif_data.rename(columns=LEIF_DATA_COLUMNS)
    return leif_data


def handle_number_of_floors(row):
    """This function is used to handle number of floors"""
    if row == 'n/d' or row == 'n.i.':
        row = int(row.replace(row, '0'))

    if isinstance(row, str):
        digits = [int(s) for s in row.split() if s.isdigit()]
        if digits:
            row = max(digits)
        else:
            row = 0
    return row


def extract_year(url):
    """This function is used to extract year from text"""
    return re.findall(r'(\d{4})', url)


def handle_building_year(row):
    """This function is used to handle building years"""
    if row == 'n/d' or row == 'n.i.':
        row = int(row.replace(row, '0'))
    if row == '60-70tie gadi':
        row = 1970
    if isinstance(row, str):
        list_of_years = extract_year(row)
        if len(list_of_years) > 1:
            row = list_of_years[1]
        elif len(list_of_years) == 1:
            row = list_of_years[0]
        else:
            row = 0
    return row


def process_leif_data(leif_df):
    """This function is used to transform leif data DF"""
    leif_df['Renewable Energy Produced energy'] = leif_df['Renewable Energy Produced energy'].fillna(0.0)
    leif_df['Renewable Energy CO2 emission'] = leif_df['Renewable Energy CO2 emission'].fillna(0.0)
    leif_df['House Constants Hot Water Source of heat'] = leif_df[
        'House Constants Hot Water Source of heat'].fillna('n/a')
    leif_df['House Constants Heating Source of heat'] = leif_df[
        'House Constants Heating Source of heat'].fillna('n/a')

    leif_df['Number of floors'] = leif_df['Number of floors'].apply(handle_number_of_floors)
    leif_df['Number of floors'] = leif_df['Number of floors'].fillna(0)

    leif_df['Year of building'] = leif_df['Year of building'].fillna(0)
    leif_df['Year of building'] = leif_df['Year of building'].apply(handle_building_year).astype('int')
    leif_df['house_id'] = leif_df.index
    return leif_df
