import os

# APPLICATION SETTINGS
PROJECT_FOLDER = '/opt/airflow'
KEY_SPACE = "matrycs_transformed"
CONNECTION_NAME = "matrycs_connection"

# EREN DATASET SPECIFICS
ENERGY_EFFICIENCY_CERTS_PATH = os.path.join(PROJECT_FOLDER, 'data/EREN/certificados-de-eficiencia-energetica.csv')

# Coopernico DATASET SPECIFICS
PALMELA_HOURLY_PRODUCTION = os.path.join(
    PROJECT_FOLDER, 'data/Coopernico/27 Adega Palmela-Horario-2020-02-02-2021-03-10.csv'
)
