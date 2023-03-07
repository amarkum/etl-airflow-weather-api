import json
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
# import requests_cache
from slugify import slugify
from pathlib import Path

import constants
from city_names import get_cities

HOME_DIR_ETL = f"{Path.home()}/etl_storage"


def get_api_key():
    """
    Retrieves the API key from an environment variable.
    """
    # api_key = os.environ.get('WEATHER_API_KEY')
    api_key = "9ba89614bb72467e907224009230302"
    if not api_key:
        raise ValueError('WEATHER_API_KEY environment variable not set.')
    return api_key


def get_city_parameters(cities):
    """
    Returns a list of city parameters for the weatherapi.com bulk API.
    """
    return [{"q": city, "custom_id": f"{slugify(city)}-request"} for city in cities]


def get_todays_weather_data(cities):
    """
    Returns a dataframe containing today's weather data for the specified cities.
    """

    city_parameters = [{"q": city, "custom_id": f"{slugify(city)}-request"} for city in cities]
    payload = json.dumps({
        "locations": city_parameters
    })
    headers = {
        'Content-Type': 'application/json'
    }
    api_key = get_api_key()
    url = f"http://api.weatherapi.com/v1/current.json?key={api_key}&q=bulk"
    # with requests_cache.disabled():
    response = requests.request("GET", url, headers=headers, data=payload)
    cities_weather_data = [{'r': city['query']} for city in response.json()['bulk']]
    todays_weather_df = pd.json_normalize(cities_weather_data)

    return todays_weather_df


def get_historical_weather_data(cities):
    """
    Returns a dataframe containing historical weather data for the specified cities.
    """
    historical_weather_df = pd.DataFrame()
    api_key = get_api_key()
    for city in cities:
        params = {
            'key': api_key,
            'q': city,
            'dt': (datetime.today() - timedelta(days=constants.historical_data_tenure)).strftime('%Y-%m-%d'),
            'end_dt': datetime.today().strftime('%Y-%m-%d'),
            'lang': 'en',
            'units': 'metric',
        }
        url = 'https://api.weatherapi.com/v1/history.json'
        # with requests_cache.disabled():
        try:
            response = requests.get(url, params=params)
            data = response.json()
            loc_data = data['location']
            daily_data = data['forecast']['forecastday']
            curr_weather_df = pd.json_normalize(daily_data)
            curr_weather_df = curr_weather_df.assign(location=loc_data['name'], region=loc_data['region'],
                                                     country=loc_data['country'])
            curr_weather_df = curr_weather_df.drop(['hour'], axis=1)
            historical_weather_df = pd.concat([historical_weather_df, curr_weather_df])
        except:
            print(f"Error getting data for city {city}")
    return historical_weather_df


def extract():
    no_of_cities = 10
    cities = get_cities()
    cities = cities[:no_of_cities]

    """
    Extracts weather data for the specified cities.
    """
    todays_weather_df = get_todays_weather_data(cities)
    todays_weather_df.to_csv(f"{HOME_DIR_ETL}/extract/todays_weather_df.csv", index=False)
    print(todays_weather_df)

    # stores meta about historical sync - last synced
    historical_data_last_sync_file = f"{HOME_DIR_ETL}/extract/historical_data_last_sync"
    delta = None
    if os.path.exists(historical_data_last_sync_file):
        with open(historical_data_last_sync_file, 'r') as f:
            last_sync_date_str = f.read()
        last_sync_date = datetime.strptime(last_sync_date_str, '%Y-%m-%d')
        delta = datetime.today() - last_sync_date

    if not delta or delta.days > constants.historical_date_fetch_days_interval:
        historical_weather_df = get_historical_weather_data(cities)
        historical_weather_df.to_csv(f"{HOME_DIR_ETL}/extract/historical_weather_df.csv", index=False)
        historical_data_sync_date = datetime.today().strftime('%Y-%m-%d')
        with open(historical_data_last_sync_file, 'w') as f:
            f.write(historical_data_sync_date)
        print(historical_weather_df)


if __name__ == '__main__':
    extract()
