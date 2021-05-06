import pandas as pd
from sklearn.preprocessing import MinMaxScaler


def load_leif_data(sheet_name):
    """This function is used to load LEIF projects"""
    leif_projects = pd.read_excel(
        'C:/Users/pkapsalis.EPU/PycharmProjects/ReasoningEngine/Data/LEIF/Data_KPFI.LV - DATA.xlsx',
        engine='openpyxl',
        sheet_name=sheet_name,
    )
    return leif_projects

scaler = MinMaxScaler()
leif_projects = load_leif_data(sheet_name='ACTIVITIES').dropna()
print(leif_projects.isna().sum())

# leif_projects[['c', 'k']]= scaler.fit_transform(leif_projects[['Total project costs', 'Grant financing']])
# print(leif_projects)