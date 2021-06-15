from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

from MongoDBClient.client import MongoDBClient
from models.LEIF.models import LeifProject, LeifActivity, LeifBuilding, LeifBeforeProjectAvg, ReportingYearData, \
    TotalAfterProject
from utils import load_leif_data, delete_unused_xcoms, load_leif_data_spec, process_leif_data, split_to_partitions, \
    init_scylla_conn


def handle_dates(row):
    """
    This function is used for transforming dates to year/month/day/hour
    """
    return row.year, row.month, row.day, row.hour


def handle_dates_kpfi_projects(**kwargs):
    """This function is used to handle dates on KPFI projects"""
    ti = kwargs['ti']
    kpfi_project_df = load_leif_data(
        sheet_name='PROJECTS',
        date_columns=['Start date of the project', 'End date of the project']
    )
    kpfi_project_df['start Year'], kpfi_project_df['start Month'], kpfi_project_df['start Day'], \
    kpfi_project_df['start Hour'] = zip(*kpfi_project_df['Start date of the project'].apply(handle_dates))
    kpfi_project_df['end Year'], kpfi_project_df['end Month'], kpfi_project_df['end Day'], \
    kpfi_project_df['end Hour'] = zip(*kpfi_project_df['End date of the project'].apply(handle_dates))

    kpfi_project_df['End date of the project'] = kpfi_project_df['End date of the project'].astype('string')
    kpfi_project_df['Start date of the project'] = kpfi_project_df['Start date of the project'].astype('string')
    ti.xcom_push(key='kpfi_project_df', value=kpfi_project_df.to_dict())


def projects_numerical_values(**kwargs):
    """This function is used to scale numerical values"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()

    kpfi_project_df = pd.DataFrame(ti.xcom_pull(
        key='kpfi_project_df',
        task_ids='handle_project_dates_op')
    )
    kpfi_project_df[['Total project costs scaled', 'Grant financing scaled']] = scaler.fit_transform(
        kpfi_project_df[['Total project costs', 'Grant financing']]
    )
    delete_unused_xcoms(task_id='handle_project_dates_op', key='kpfi_project_df')
    ti.xcom_push(key='kpfi_project_df', value=kpfi_project_df.to_dict())


def store_leif_projects(**kwargs):
    """This function is used for storing Leif project data"""
    ti = kwargs['ti']

    kpfi_project_df = pd.DataFrame(ti.xcom_pull(
        key='kpfi_project_df',
        task_ids='scale_leif_project_num_op')
    )
    kpfi_project_df=kpfi_project_df.rename(columns={'Project No.': 'Project No'})
    delete_unused_xcoms(task_id='scale_leif_project_num_op', key='kpfi_project_df')
    mongo_client = MongoDBClient()

    leif_activity_collection_ = mongo_client.create_collection('leif_project')
    mongo_client.insert_many_(kpfi_project_df, leif_activity_collection_)


def scale_kpfi_activities(**kwargs):
    """This function is used to scale numerical values in KPFI activities"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()

    leif_activities = load_leif_data(sheet_name='ACTIVITIES').dropna()
    leif_activities[
        ['CO2 reduction scaled', 'Energy reduction scaled', 'CO2 emission factor scaled']] = scaler.fit_transform(
        leif_activities[['CO2 reduction', 'Energy reduction', 'CO2 emission factor']]
    )
    leif_activities = leif_activities.dropna()
    ti.xcom_push(key='leif_activities', value=leif_activities.to_dict())


def store_leif_activities(**kwargs):
    """This function is used to store leif activities"""
    ti = kwargs['ti']
    leif_activities = pd.DataFrame(ti.xcom_pull(
        key='leif_activities',
        task_ids='scale_leif_kpfi_activities_op')
    )
    leif_activities = leif_activities.rename(columns={'Project No.': 'Project No'})
    delete_unused_xcoms(task_id='scale_leif_kpfi_activities_op', key='leif_activities')
    mongo_client = MongoDBClient()

    leif_activity_collection_ = mongo_client.create_collection('leif_activity')
    mongo_client.insert_many_(leif_activities, leif_activity_collection_)


def scale_kpfi_data(**kwargs):
    """This function is used to scale numerical values in KPFI data"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()

    leif_data = load_leif_data_spec()
    processed_leif_data = process_leif_data(leif_data)

    processed_leif_data[[
        'Planned CO2 emission reduction per year scaled',
        'Total heating area scaled',
        'Energy consumption before project, MWh scaled',
        'Energy consumption before project, kWh/m2 gadā scaled',
        'CO2 Emission before project scaled',
        'House Constants Heating CO2 Emission factor scaled',
        'House Constants Heating CO2 Emission factor (audit) scaled',
        'House Constants Hot water CO2 Emission factor scaled',
        'House Constants Electricity CO2 Emission factor scaled',
        'Heating avg data before project Total energy consumption scaled',
        'Heating avg data before project CO2 emission scaled',
        'Heating Reporting Year data Total energy consumption scaled',
        'Heating Reporting Year CO2 emission scaled',
        'Hot Water avg data before project Total energy consumption scaled',
        'Hot Water avg data before project CO2 emission scaled',
        'Hot Water Reporting Year data Total energy consumption scaled',
        'Hot Water Reporting Year data CO2 emission scaled',
        'Electricity avg data before project Total energy consumption scaled',
        'Electricity avg data before project CO2 emission scaled',
        'Electricity Reporting Year data Total energy consumption scaled',
        'Electricity Reporting Year data CO2 emission scaled',
        'Renewable Energy Produced energy scaled',
        'Renewable Energy CO2 emission scaled',
        'TOTALS Total Energy Consumption MWh scaled',
        'TOTALS Total Energy Consumption kWh/m2 scaled',
        'TOTALS Building CO2 emission in reporting year, tCO2 gadā scaled',
        'TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā scaled'
    ]] = scaler.fit_transform(
        processed_leif_data[[
            'Planned CO2 emission reduction per year',
            'Total heating area',
            'Energy consumption before project, MWh',
            'Energy consumption before project, kWh/m2 gadā',
            'CO2 Emission before project',
            'House Constants Heating CO2 Emission factor',
            'House Constants Heating CO2 Emission factor (audit)',
            'House Constants Hot water CO2 Emission factor',
            'House Constants Electricity CO2 Emission factor',
            'Heating avg data before project Total energy consumption',
            'Heating avg data before project CO2 emission',
            'Heating Reporting Year data Total energy consumption',
            'Heating Reporting Year CO2 emission',
            'Hot Water avg data before project Total energy consumption',
            'Hot Water avg data before project CO2 emission',
            'Hot Water Reporting Year data Total energy consumption',
            'Hot Water Reporting Year data CO2 emission',
            'Electricity avg data before project Total energy consumption',
            'Electricity avg data before project CO2 emission',
            'Electricity Reporting Year data Total energy consumption',
            'Electricity Reporting Year data CO2 emission',
            'Renewable Energy Produced energy',
            'Renewable Energy CO2 emission',
            'TOTALS Total Energy Consumption MWh',
            'TOTALS Total Energy Consumption kWh/m2',
            'TOTALS Building CO2 emission in reporting year, tCO2 gadā',
            'TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā'
        ]]
    )
    ti.xcom_push(key='leif_data', value=processed_leif_data.to_dict())


def store_leif_data(**kwargs):
    """This function is used to store leif data from KPFI"""
    ti = kwargs['ti']
    leif_data = pd.DataFrame(ti.xcom_pull(
        key='leif_data',
        task_ids='scale_leif_data_op')
    )
    leif_data = leif_data.rename(columns={'Project No.': 'Project No'})
    delete_unused_xcoms(task_id='scale_leif_data_op', key='leif_data')
    leif_building = leif_data[
        ['house_id', 'Year of building', 'Number of floors', 'Total heating area', 'Total heating area scaled',
         'House functions', 'House Constants Heating Source of heat', 'House functions',
         'House Constants Heating Source of heat', 'House Constants Heating CO2 Emission factor',
         'House Constants Heating CO2 Emission factor scaled', 'House Constants Heating CO2 Emission factor (audit)',
         'House Constants Hot Water Source of heat', 'House Constants Hot water CO2 Emission factor',
         'House Constants Hot water CO2 Emission factor scaled', 'House Constants Electricity CO2 Emission factor',
         'House Constants Electricity CO2 Emission factor scaled'
         ]]
    leif_before_project_avg = leif_data[[
        'house_id', 'Project No', 'Heating avg data before project Total energy consumption',
        'Heating avg data before project Total energy consumption scaled',
        'Heating avg data before project CO2 emission',
        'Heating avg data before project CO2 emission scaled',
        'Hot Water avg data before project Total energy consumption',
        'Hot Water avg data before project Total energy consumption scaled',
        'Hot Water avg data before project CO2 emission',
        'Hot Water avg data before project CO2 emission scaled',
        'Electricity avg data before project Total energy consumption',
        'Electricity avg data before project Total energy consumption scaled',
        'Electricity avg data before project CO2 emission',
        'Electricity avg data before project CO2 emission scaled'
    ]]
    reporting_year_data = leif_data[[
        'house_id', 'Project No', 'Heating Reporting Year data Total energy consumption',
        'Heating Reporting Year data Total energy consumption scaled', 'Heating Reporting Year CO2 emission',
        'Heating Reporting Year CO2 emission scaled', 'Hot Water Reporting Year data Total energy consumption',
        'Hot Water Reporting Year data Total energy consumption scaled', 'Hot Water Reporting Year data CO2 emission',
        'Hot Water Reporting Year data CO2 emission scaled', 'Electricity Reporting Year data Total energy consumption',
        'Electricity Reporting Year data Total energy consumption scaled',
        'Electricity Reporting Year data CO2 emission',
        'Electricity Reporting Year data CO2 emission scaled', 'Renewable Energy Produced energy',
        'Renewable Energy Produced energy scaled', 'Renewable Energy CO2 emission',
        'Renewable Energy CO2 emission scaled'
    ]]
    totals_after_project = leif_data[[
        'house_id', 'Project No', 'TOTALS Total Energy Consumption MWh', 'TOTALS Total Energy Consumption MWh scaled',
        'TOTALS Total Energy Consumption kWh/m2', 'TOTALS Total Energy Consumption kWh/m2 scaled',
        'TOTALS Building CO2 emission in reporting year, tCO2 gadā',
        'TOTALS Building CO2 emission in reporting year, tCO2 gadā scaled',
        'TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā',
        'TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā scaled'
    ]]

    mongo_client = MongoDBClient()

    leif_building_collection_ = mongo_client.create_collection('leif_building')
    leif_before_project_avg_collection_ = mongo_client.create_collection('leif_before_project_avg')
    leif_reporting_year_data_collection_ = mongo_client.create_collection('leif_reporting_year_data')
    leif_totals_after_project_collection_ = mongo_client.create_collection('leif_totals_after_project')

    mongo_client.insert_many_(leif_building, leif_building_collection_)
    mongo_client.insert_many_(leif_before_project_avg, leif_before_project_avg_collection_)
    mongo_client.insert_many_(reporting_year_data, leif_reporting_year_data_collection_)
    mongo_client.insert_many_(totals_after_project, leif_totals_after_project_collection_)