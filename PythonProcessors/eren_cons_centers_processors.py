import pandas as pd
from airflow.providers.presto.hooks.presto import PrestoHook

from utils import encoding_labels, scale_ratio, delete_unused_xcoms


def encode_cons_center_type(**kwargs):
    """This function is used to encode the heating demand rating"""
    ti = kwargs['ti']
    ph = PrestoHook(presto_conn_id='matrycs_presto_conn')
    consumer_center_df = ph.get_pandas_df(hql="SELECT * FROM cassandra.matrycs.consumer_center")
    consumer_center_df = encoding_labels(df=consumer_center_df, column='type')
    ti.xcom_push(key='consumer_center_df', value=consumer_center_df.to_dict())


def scale_cons_centers_numerical_vars(**kwargs):
    """This function is used to scale numerical variables from consumer centers pd.DataFrame"""
    ti = kwargs['ti']
    encoded_cons_center_df = pd.DataFrame(
        ti.xcom_pull(key='consumer_center_df', task_ids='encode_consumer_center_type_op')
    )
    delete_unused_xcoms(task_id='encode_consumer_center_type_op', key='consumer_center_df')

    scaled_builded_surface_df= scale_ratio(column='builded_surface', df=encoded_cons_center_df)
    scaled_latitude_df = scale_ratio(column='latitude', df=scaled_builded_surface_df)
    scaled_longitude_df = scale_ratio(column='longitude', df=scaled_latitude_df)
    scaled_occupation_df = scale_ratio(column='occupation', df=scaled_longitude_df)
    print(scaled_occupation_df)