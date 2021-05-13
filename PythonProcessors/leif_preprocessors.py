from cassandra.cqlengine.management import sync_table
from cassandra.cqlengine.query import BatchQuery
from sklearn.preprocessing import MinMaxScaler
import pandas as pd

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
    delete_unused_xcoms(task_id='scale_leif_project_num_op', key='kpfi_project_df')

    partitioned_palmela_data = split_to_partitions(kpfi_project_df, 100)
    init_scylla_conn()
    sync_table(LeifProject)

    # Upload Batch data to ScyllaDB
    num_of_partitions = 0
    for partition in partitioned_palmela_data:
        print("Loading {}/{} partition to ScyllaDB".format(num_of_partitions, 100))
        with BatchQuery() as b:
            for index, item in partition.iterrows():
                LeifProject.batch(b).create(
                    project_no=item['Project No.'],
                    project_co2_reduction=item['Project CO2 reduction'],
                    start_date=item['Start date of the project'],
                    end_date=item['End date of the project'],
                    total_project_costs=item['Total project costs'],
                    total_project_costs_scaled=item['Total project costs scaled'],
                    grant_financing=item['Grant financing'],
                    grant_financing_scaled=item['Grant financing scaled'],
                    start_year=item['start Year'],
                    start_month=item['start Month'],
                    start_day=item['start Day'],
                    start_hour=item['start Hour'],
                    end_year=item['end Year'],
                    end_month=item['end Month'],
                    end_day=item['end Day'],
                    end_hour=item['end Hour'],
                )
        num_of_partitions += 1


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
    delete_unused_xcoms(task_id='scale_leif_kpfi_activities_op', key='leif_activities')
    print(leif_activities)
    print(leif_activities.columns)

    partitioned_leif_activities = split_to_partitions(leif_activities, 100)
    init_scylla_conn()
    sync_table(LeifActivity)

    # Upload Batch data to ScyllaDB
    num_of_partitions = 0
    for partition in partitioned_leif_activities:
        print("Loading {}/{} partition to ScyllaDB".format(num_of_partitions, 100))
        with BatchQuery() as b:
            for index, item in partition.iterrows():
                LeifActivity.batch(b).create(
                    project_no=item['Project No.'],
                    project_activity=item['Project activity/building'],
                    type_of_activity=item['Type of activity'],
                    type_of_building=item['Type of building'],
                    co2_reduction=item['CO2 reduction'],
                    co2_reduction_scaled=item['CO2 reduction scaled'],
                    energy_reduction=item['Energy reduction'],
                    energy_reduction_scaled=item['Energy reduction scaled'],
                    co2_emission_factor=item['CO2 emission factor'],
                    co2_emission_factor_scaled=item['CO2 emission factor scaled']
                )
        num_of_partitions += 1


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
    delete_unused_xcoms(task_id='scale_leif_data_op', key='leif_data')
    print(leif_data.columns)

    partitioned_leif_data = split_to_partitions(leif_data, 300)
    init_scylla_conn()

    sync_table(LeifBuilding)
    sync_table(LeifBeforeProjectAvg)
    sync_table(ReportingYearData)
    sync_table(TotalAfterProject)

    # Upload Batch data to ScyllaDB
    num_of_partitions = 0
    for partition in partitioned_leif_data:
        print("Loading {}/{} partition to ScyllaDB".format(num_of_partitions, 300))
        with BatchQuery() as b:
            for index, item in partition.iterrows():
                LeifBuilding.batch(b).create(
                    house_nr=item['house_id'],
                    year_of_building=item['Year of building'],
                    number_of_floors=item['Number of floors'],
                    total_heating_area=item['Total heating area'],
                    total_heating_area_scaled=item['Total heating area scaled'],
                    house_functions=item['House functions'],
                    heating_source_of_heat=item['House Constants Heating Source of heat'],
                    heating_co2_emission_factor=item['House Constants Heating CO2 Emission factor'],
                    heating_co2_emission_factor_scaled=item['House Constants Heating CO2 Emission factor scaled'],
                    heating_co2_emission_factor_audit=item['House Constants Heating CO2 Emission factor (audit)'],
                    heating_co2_emission_factor_audit_scaled=item['House Constants Heating CO2 Emission factor (audit) scaled'],
                    hot_water_source_of_heat=item['House Constants Hot Water Source of heat'],
                    hot_water_co2_emission_factor=item['House Constants Hot water CO2 Emission factor'],
                    hot_water_co2_emission_factor_scaled=item['House Constants Hot water CO2 Emission factor scaled'],
                    electricity_co2_emission_factor=item['House Constants Electricity CO2 Emission factor'],
                    electricity_co2_emission_factor_scaled=item['House Constants Electricity CO2 Emission factor scaled'],
                )
                LeifBeforeProjectAvg.batch(b).create(
                    house_nr=item['house_id'],
                    project_no=item['Project No.'],
                    heating_total_consumption=item['Heating avg data before project Total energy consumption'],
                    heating_total_consumption_scaled=item['Heating avg data before project Total energy consumption scaled'],
                    heating_co2_emission=item['Heating avg data before project CO2 emission'],
                    heating_co2_emission_scaled=item['Heating avg data before project CO2 emission scaled'],
                    hot_water_total_consumption=item['Hot Water avg data before project Total energy consumption'],
                    hot_water_total_consumption_scaled=item['Hot Water avg data before project Total energy consumption scaled'],
                    hot_water_co2_emission=item['Hot Water avg data before project CO2 emission'],
                    hot_water_co2_emission_scaled=item['Hot Water avg data before project CO2 emission scaled'],
                    electricity_total_consumption=item['Electricity avg data before project Total energy consumption'],
                    electricity_total_consumption_scaled=item['Electricity avg data before project Total energy consumption scaled'],
                    electricity_co2_emission=item['Electricity avg data before project CO2 emission'],
                    electricity_co2_emission_scaled=item['Electricity avg data before project CO2 emission scaled'],
                )
                ReportingYearData.batch(b).create(
                    house_nr=item['house_id'],
                    project_no=item['Project No.'],
                    heating_total_consumption=item['Heating Reporting Year data Total energy consumption'],
                    heating_total_consumption_scaled=item['Heating Reporting Year data Total energy consumption scaled'],
                    heating_co2_emission=item['Heating Reporting Year CO2 emission'],
                    heating_co2_emission_scaled=item['Heating Reporting Year CO2 emission scaled'],
                    hot_water_total_consumption=item['Hot Water Reporting Year data Total energy consumption'],
                    hot_water_total_consumption_scaled=item['Hot Water Reporting Year data Total energy consumption scaled'],
                    hot_water_co2_emission=item['Hot Water Reporting Year data CO2 emission'],
                    hot_water_co2_emission_scaled=item['Hot Water Reporting Year data CO2 emission scaled'],
                    electricity_total_consumption=item['Electricity Reporting Year data Total energy consumption'],
                    electricity_total_consumption_scaled=item['Electricity Reporting Year data Total energy consumption scaled'],
                    electricity_co2_emission=item['Electricity Reporting Year data CO2 emission'],
                    electricity_co2_emission_scaled=item['Electricity Reporting Year data CO2 emission scaled'],
                    renewable_energy_produced_energy=item['Renewable Energy Produced energy'],
                    renewable_energy_produced_energy_scaled=item['Renewable Energy Produced energy scaled'],
                    renewable_energy_co2_emission=item['Renewable Energy CO2 emission'],
                    renewable_energy_co2_emission_scaled=item['Renewable Energy CO2 emission scaled']
                )
                TotalAfterProject.batch(b).create(
                    house_nr=item['house_id'],
                    project_no=item['Project No.'],
                    total_energy_consumption_mwh=item['TOTALS Total Energy Consumption MWh'],
                    total_energy_consumption_mwh_scaled=item['TOTALS Total Energy Consumption MWh scaled'],
                    total_energy_consumption_kwh_m2=item['TOTALS Total Energy Consumption kWh/m2'],
                    total_energy_consumption_kwh_m2_scaled=item['TOTALS Total Energy Consumption kWh/m2 scaled'],
                    co2_emission=item['TOTALS Building CO2 emission in reporting year, tCO2 gadā'],
                    co2_emission_scaled=item['TOTALS Building CO2 emission in reporting year, tCO2 gadā scaled'],
                    co2_emission_reduction=item['TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā'],
                    co2_emission_reduction_scaled=item['TOTALS Building CO2 emission reduction in reporting year, tCO2 gadā scaled']
                )
        num_of_partitions += 1



