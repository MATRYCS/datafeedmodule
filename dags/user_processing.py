import json

from airflow.models import DAG
from datetime import datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.sqlite.operators.sqlite import SqliteOperator
from pandas.io.json import json_normalize

default_args = {
    'start_date': datetime(2020, 1, 1)
}


def _processing_user(**kwargs):
    ti = kwargs['ti']
    users = ti.xcom_pull(task_ids='extracting_user', key='return_value')
    if 'results' not in users.keys():
        raise ValueError
    else:
        user = users['results'][0]
        processed_user = json_normalize({
            'firstname': user['name']['first'],
            'lastname': user['name']['last'],
            'country': user['location']['country'],
            'username': user['login']['username'],
            'password': user['login']['password'],
            'email': user['email']
        })
        processed_user.to_csv('/tmp/processed_user.csv', index=None, header=False)

with DAG('user_processing',
         schedule_interval='@daily',
         default_args=default_args,
         catchup=False
         ) as dag:
    creating_sqlite_table = SqliteOperator(
        task_id='creating_sqlite_table_task',
        sqlite_conn_id='db_sqlite',
        sql="""
            CREATE TABLE IF NOT EXISTS users(
            email TEXT PRIMARY KEY,
            firstName TEXT NOT NULL,
            lastName TEXT NOT NULL,
            country TEXT NOT NULL,
            password TEXT NOT NULL);
        """
    )
    is_api_available = HttpSensor(
        task_id='is_api_available',
        http_conn_id='user_api',
        endpoint='api/',
    )
    extracting_user = SimpleHttpOperator(
        task_id='extracting_user',
        method='GET',
        http_conn_id='user_api',
        endpoint='api/',
        log_response=True,
        response_filter=lambda response: json.loads(response.text)
    )
    processing_users = PythonOperator(
        task_id='processing_users',
        python_callable=_processing_user
    )
    storing_user = BashOperator(
        task_id='storing_user',
        bash_command='sleep 4'
    )



    creating_sqlite_table >> is_api_available >> extracting_user >> processing_users
