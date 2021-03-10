import psycopg2
import pandas as pd
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
