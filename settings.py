import os

# APPLICATION SETTINGS
PROJECT_FOLDER = '/opt/airflow'
KEY_SPACE = "matrycs_transformed"
CONNECTION_NAME = "matrycs_connection"

MONGODB_HOSTNAME = os.environ.get('MONGODB_HOST', 'matrycs.epu.ntua.gr')
MONGO_PORT = os.environ.get('MONGO_PORT', 27017)
MONGO_PASS = os.environ.get('MONGO_INITDB_ROOT_PASSWORD', 'admin')
MONGO_USER = os.environ.get('MONGO_INITDB_ROOT_USERNAME', 'admin')
DATABASE_NAME = os.environ.get('DATABASE', 'matrycs_transformed')
MONGO_URI = 'mongodb://{}:{}@{}:{}/'.format(
    MONGO_USER,
    MONGO_PASS,
    MONGODB_HOSTNAME,
    MONGO_PORT
)

# EREN DATASET SPECIFICS
ENERGY_EFFICIENCY_CERTS_PATH = os.path.join(PROJECT_FOLDER, 'data/EREN/certificados-de-eficiencia-energetica.csv')

# Coopernico DATASET SPECIFICS
PALMELA_HOURLY_PRODUCTION = os.path.join(
    PROJECT_FOLDER, 'data/Coopernico/27 Adega Palmela-Horario-2020-02-02-2021-03-10.csv'
)

# BTC Tower DATASET SPECIFICS
BTC_TOWER_DATASET = os.path.join(
    PROJECT_FOLDER, 'data/BTC/BTC tower.xlsx'
)

# LEIF DATASET SPECIFICS
LEIF_KPFI_DATASET = os.path.join(
    PROJECT_FOLDER, 'data/LEIF/Data_KPFI.LV - DATA.xlsx'
)
LEIF_DATA_COLUMNS = {
    0: 'Reporting year',
    1: 'Project No.',
    2: 'Planned CO2 emission reduction per year',
    # 3: 'Planned heat consumption, kWh/m2',
    3: 'House Nr',
    4: 'Year of building',
    5: 'Number of floors',
    6: 'Total heating area',
    7: 'House functions',
    10: 'Energy consumption before project, MWh',
    11: 'Energy consumption before project, kWh/m2 gadā',
    12: 'CO2 Emission before project',
    14: 'House Constants Heating Source of heat',
    15: 'House Constants Heating CO2 Emission factor',
    16: 'House Constants Heating CO2 Emission factor (audit)',
    17: 'House Constants Hot Water Source of heat',
    18: 'House Constants Hot water CO2 Emission factor',
    19: 'House Constants Electricity CO2 Emission factor',
    20: 'Heating avg data before project Total energy consumption',
    21: 'Heating avg data before project CO2 emission',
    22: 'Heating Reporting Year data Total energy consumption',
    23: 'Heating Reporting Year CO2 emission',
    24: 'Heating Difference Total energy consumption',
    25: 'Heating Difference CO2 emission',
    26: 'Hot Water avg data before project Total energy consumption',
    27: 'Hot Water avg data before project CO2 emission',
    28: 'Hot Water Reporting Year data Total energy consumption',
    29: 'Hot Water Reporting Year data CO2 emission',
    30: 'Hot Water Difference Total energy consumption',
    31: 'Hot Water Difference CO2 emission',
    32: 'Electricity avg data before project Total energy consumption',
    33: 'Electricity avg data before project CO2 emission',
    34: 'Electricity Reporting Year data Total energy consumption',
    35: 'Electricity Reporting Year data CO2 emission',
    36: 'Electricity Difference Total energy consumption',
    37: 'Electricity Difference CO2 emission',
    38: 'Renewable Energy Produced energy',
    39: 'Renewable Energy CO2 emission',
    40: 'TOTALS Total Energy Consumption MWh',
    41: 'TOTALS Total Energy Consumption kWh/m2',
    42: 'TOTALS Building CO2 emission in reporting year, tCO2 gadā',
    43: 'TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā'
}