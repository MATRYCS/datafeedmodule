import pandas as pd
from airflow.providers.presto.hooks.presto import PrestoHook

from utils import encoding_labels, scale_ratio, delete_unused_xcoms, map_months, categorical_encoding


def encode_cons_center_type(**kwargs):
    """This function is used to encode the heating demand rating"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    consumer_center_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.consumer_center")
    consumer_center_df = categorical_encoding(df=consumer_center_df, column='type')
    ti.xcom_push(key='consumer_center_df', value=consumer_center_df.to_dict())


def scale_cons_centers_numerical_vars(**kwargs):
    """This function is used to scale numerical variables from consumer centers pd.DataFrame"""
    ti = kwargs['ti']
    encoded_cons_center_df = pd.DataFrame(
        ti.xcom_pull(key='consumer_center_df', task_ids='encode_consumer_center_type_op')
    )
    delete_unused_xcoms(task_id='encode_consumer_center_type_op', key='consumer_center_df')

    scaled_builded_surface_df = scale_ratio(column='builded_surface', df=encoded_cons_center_df)
    scaled_latitude_df = scale_ratio(column='latitude', df=scaled_builded_surface_df)
    scaled_longitude_df = scale_ratio(column='longitude', df=scaled_latitude_df)
    scaled_occupation_df = scale_ratio(column='occupation', df=scaled_longitude_df)
    print(scaled_occupation_df)


def encode_electricity_consumption(**kwargs):
    """This function is used to encode the electricity consumption month"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    electricity_consumption_df = ph.get_pandas_df("SELECT * FROM cassandra.matrycs.monthly_electricity_consumption")
    encoded_distributors = categorical_encoding(df=electricity_consumption_df, column='distributor')
    electricity_consumption_encoded_df = map_months(df=encoded_distributors, column='month')
    ti.xcom_push(key='electricity_consumption_encoded_df', value=electricity_consumption_encoded_df.to_dict())


def scale_numerical_vars_electricity_consumption(**kwargs):
    """This function is used for scaling numerical variables in electricity consumption DF"""
    ti = kwargs['ti']
    electricity_consumption_encoded_df = pd.DataFrame(
        ti.xcom_pull(key='electricity_consumption_encoded_df', task_ids='encode_electricity_consumption_op')
    )
    delete_unused_xcoms(task_id='encode_electricity_consumption_op', key='electricity_consumption_encoded_df')

    scaled_gd_base_20 = scale_ratio(df=electricity_consumption_encoded_df, column='gd_base_20')
    scaled_gd_base_26 = scale_ratio(df=scaled_gd_base_20, column='gd_base_26')
    scaled_p1_contracted_power = scale_ratio(df=scaled_gd_base_26, column='p1_contracted_power')
    scaled_p2_contracted_power = scale_ratio(df=scaled_p1_contracted_power, column='p2_contracted_power')
    scaled_p3_contracted_power = scale_ratio(df=scaled_p2_contracted_power, column='p3_contracted_power')
    scaled_p4_contracted_power = scale_ratio(df=scaled_p3_contracted_power, column='p4_contracted_power')
    scaled_p5_contracted_power = scale_ratio(df=scaled_p4_contracted_power, column='p5_contracted_power')
    scaled_p6_contracted_power = scale_ratio(df=scaled_p5_contracted_power, column='p6_contracted_power')
    scale_total_cons = scale_ratio(df=scaled_p6_contracted_power, column='total_monthly_consumption')
    print(scale_total_cons)


def encode_gas_consumption(**kwargs):
    """This function is used for encoding categorical vars in gas consumption table"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    gas_consumption_df = ph.get_pandas_df("SELECT * FROM cassandra.matrycs.monthly_gas_consumption")
    month_encoded_df = map_months(df=gas_consumption_df, column='month')
    encoded_distributors = categorical_encoding(df=month_encoded_df, column='gas_distributor')
    ti.xcom_push(key='encoded_gas_consumption_df', value=encoded_distributors.to_dict())


def scale_vars_gas_consumption(**kwargs):
    """This function is used to scale the numerical variables in gas consumption data"""
    ti = kwargs['ti']
    encoded_gas_consumption_df = pd.DataFrame(
        ti.xcom_pull(key='encoded_gas_consumption_df', task_ids='encode_gas_consumption_op')
    )
    delete_unused_xcoms(task_id='encode_gas_consumption_op', key='encoded_gas_consumption_df')

    scaled_gd_base_20 = scale_ratio(df=encoded_gas_consumption_df, column='gd_base_20')
    scaled_gd_base_26 = scale_ratio(df=scaled_gd_base_20, column='gd_base_26')
    scale_monthly_gas_cons = scale_ratio(df=scaled_gd_base_26, column='monthly_gas_consumption')
    print(scale_monthly_gas_cons)