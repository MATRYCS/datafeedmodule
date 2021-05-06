from sklearn.preprocessing import MinMaxScaler
import pandas as pd

from utils import load_leif_data, delete_unused_xcoms


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


def scale_kpfi_activities(**kwargs):
    """This function is used to scale numerical values in KPFI activities"""
    ti = kwargs['ti']
    scaler = MinMaxScaler()

    leif_activities = load_leif_data(sheet_name='ACTIVITIES').dropna()
    leif_activities[
        ['CO2 reduction scaled', 'Energy reduction scaled', 'CO2 emission factor scaled']] = scaler.fit_transform(
        leif_activities[['CO2 reduction', 'Energy reduction', 'CO2 emission factor']]
    )
    ti.xcom_push(key='leif_activities', value=leif_activities.to_dict())
