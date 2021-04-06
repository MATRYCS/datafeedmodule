import pandas as pd

from settings import BTC_TOWER_DATASET


def load_btc_tower_dataset():
    """This function is used to load BTC TOWER DATASET

    Returns: pd.DataFrame
        btc tower dataframe
    """
    btc_tower_df = pd.read_excel(
        BTC_TOWER_DATASET,
        usecols=['DATE', 'TIMESTAMP', 'LOCATION', 'ENERGY_SOURCE', 'MEASURE', 'UNIT_OF_MEASURE', 'INTERVAL', 'VALUE'],
        parse_dates=['DATE', 'TIMESTAMP']
    )
    return btc_tower_df
