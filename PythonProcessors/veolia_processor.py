import os
import sys
import pandas as pd

from MongoDBClient.client import MongoDBClient
from MongoDBClient.collection_handler import CollectionHandler
from settings import excel_file_path, sheet_names_15
from utils import delete_unused_xcoms, remove_xcoms_after_run


def handle_dates_extended(row):
    """
    This function is used for transforming dates to year/month/day/hour
    """
    return row.year, row.month, row.day, row.hour, row.minute


def read_data(sheet_name):
    """This function is used to read sheet names from Veolia excel files"""
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name, engine='openpyxl').rename(
        columns={'Installation name': "installation_name", 'Variable name': "variable_name"}
    )
    df['Value'] = df['Value'].str.replace(',', '.').astype(float)
    df['Year'], df['Month'], df['Day'], \
    df['Hour'], df['Minute'] = zip(*df['Datehour'].apply(handle_dates_extended))
    return df


def combine_dfs(**kwargs):
    """This function is used to combine dataframes from sheets"""
    df_list = [read_data(sheet) for sheet in sheet_names_15]
    result = pd.concat(df_list, ignore_index=True)
    return result


def insert_veolia_data(**kwargs):
    """This function is used to insert btc tower data to Scylla"""
    ti = kwargs['ti']
    previous_task = kwargs['previous_task']
    veolia_df = pd.DataFrame(ti.xcom_pull(
        key='veolia_df',
        task_ids=previous_task
    ))
    delete_unused_xcoms(task_id=previous_task, key='veolia_df')
    mongo_client = MongoDBClient()
    collection_ = mongo_client.create_collection('veolia_vars_data')
    mongo_client.insert_many_(df=veolia_df, collection=collection_)


def load_and_insert():
    combined_dfs = combine_dfs()
    mongo_client = MongoDBClient()
    collection_ = mongo_client.create_collection('veolia_vars_data')
    mongo_client.insert_many_(df=combined_dfs, collection=collection_)


def check_if_file_is_processed(**kwargs):
    ti = kwargs['ti']
    file = kwargs['file']
    index = kwargs['index']
    collection_handler = CollectionHandler()
    status = collection_handler.append_processed_files(file_name=file)
    remove_xcoms_after_run(task_ids=[
        'if_veolia_data_exists',
        'if_new_files',
        'stop_execution',
        'process'
    ])
    if status:
        return 'insert_veolia_{}'.format(index)
    else:
        return 'abort_{}'.format(index)
