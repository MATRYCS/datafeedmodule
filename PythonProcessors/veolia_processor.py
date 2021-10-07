import os
import sys
import pandas as pd

from settings import excel_file_path, sheet_names_15


def read_data(sheet_name):
    """This function is used to read sheet names from Veolia excel files"""
    df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
    df['Value'] = df['Value'].str.replace(',', '.').astype(float)
    return df


def combine_dfs():
    """This function is used to combine dataframes from sheets"""
    df_list = [read_data(sheet) for sheet in sheet_names_15]
    result = pd.concat(df_list, ignore_index=True)
    return result
