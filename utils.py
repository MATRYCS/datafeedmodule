import psycopg2
import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import ExecutionProfile, EXEC_PROFILE_DEFAULT, Cluster
from sklearn import preprocessing
from sklearn.preprocessing import MinMaxScaler


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
