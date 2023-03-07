from pathlib import Path

import pandas as pd

import constants

HOME_DIR_ETL = f"{Path.home()}/etl_storage"
from datetime import datetime


def transform():
    # transform today's df
    todays_weather_df = pd.read_csv(f"{HOME_DIR_ETL}/extract/todays_weather_df.csv")
    todays_weather_df.columns = todays_weather_df.columns.str.lstrip('r.')
    transformed_todays_weather_data = todays_weather_df
    transformed_todays_weather_data.to_csv(f"{HOME_DIR_ETL}/transform/transformed_todays_weather_df.csv", index=False)

    historical_data_last_sync_file = f"{HOME_DIR_ETL}/extract/historical_data_last_sync"
    with open(historical_data_last_sync_file, 'r') as f:
        last_sync_date_str = f.read()
    last_sync_date = datetime.strptime(last_sync_date_str, '%Y-%m-%d')

    delta = datetime.today() - last_sync_date

    if delta.days == 0 or delta.days > constants.historical_date_fetch_days_interval:
        print("transform historical data")
        # transform historical data
        historical_weather_df = pd.read_csv(f"{HOME_DIR_ETL}/extract/historical_weather_df.csv")
        transformed_historical_weather_df = historical_weather_df
        transformed_historical_weather_df.to_csv(f"{HOME_DIR_ETL}/transform/transformed_historical_weather_df.csv", index=False)


if __name__ == '__main__':
    transform()
