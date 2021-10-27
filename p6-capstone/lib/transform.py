import os
import logging

import pandas as pd
import googlemaps

log = logging.getLogger(__name__)

def _get_latest_3_years(temp: pd.DataFrame) -> pd.DataFrame:
    max_date = temp['date'].max()
    DAYS_PER_YEAR = 365
    latest_3_years = max_date - pd.to_timedelta(3*DAYS_PER_YEAR, unit='D')
    temp = temp[temp['date'] > latest_3_years].copy()

    return temp

def _convert_temp_coordinates(temp: pd.DataFrame) -> pd.DataFrame:
    temp['latitude'] = temp['latitude'].str.replace(r'(\d{1,3}\.\d{1,2})N', r'\1', regex=True)
    temp['latitude'] = temp['latitude'].str.replace(r'(\d{1,3}\.\d{1,2})S', r'-\1', regex=True)

    temp['longitude'] = temp['longitude'].str.replace(r'(\d{1,3}\.\d{1,2})E', r'\1', regex=True)
    temp['longitude'] = temp['longitude'].str.replace(r'(\d{1,3}\.\d{1,2})W', r'-\1', regex=True)

    temp['latitude'] = temp['latitude'].astype(float)
    temp['longitude'] = temp['longitude'].astype(float)

    return temp

def _filter_temperatures(temp: pd.DataFrame) -> pd.DataFrame:
    temp = temp[temp['country']=='UNITED STATES']

    temp = _get_latest_3_years(temp)
    temp['month'] = temp['date'].dt.month

    temp = _convert_temp_coordinates(temp)

    return temp

def _find_state_by_coordinates(temp: pd.DataFrame, use_google_api: bool = False) -> pd.DataFrame:
    if not use_google_api:
        # Google Geocoder API costs money, so for testing we are reusing the 
        # already enriched dataset we retrieved using our free trial
        temp = pd.read_csv('s3://p6-capstone/data/GlobalLandTemperaturesByCityEnriched.csv')
    else:
        google_api_key = os.environ['GOOGLE_API_KEY']
        gmaps = googlemaps.Client(key=google_api_key)

        states = []
        for lat, lon in temp[['latitude', 'longitude']].itertuples(index=False):
            response = gmaps.reverse_geocode([lat, lon], result_type="administrative_area_level_1")
            if response:
                state = response[0]['address_components'][0]['short_name']
                states.append(state)
            else:
                states.append(pd.NA)

        temp['state_code'] = states

    return temp

def calc_avg_monthly_temp_by_city_and_state(
    temp: pd.DataFrame, 
    use_google_api: bool = False
) -> pd.DataFrame:
    log.info("Transforming and aggregating temperature data...")
    temp = _filter_temperatures(temp)
    temp = _find_state_by_coordinates(temp, use_google_api=use_google_api)

    group_cols = ['state_code', 'city', 'month']
    agg_temp = temp.groupby(group_cols, as_index=False).mean()
    return agg_temp[[
        'state_code',
        'city',
        'month',
        'avg_temperature',
        'avg_temperature_uncertainty'
    ]]

def agg_airports_by_city_and_state(airport: pd.DataFrame) -> pd.DataFrame:
    log.info("Transforming and aggregating airport data...")
    airport = airport[airport['iso_country_code']=='US'].copy()
    airport['state_code'] = airport['iso_region_code'].str.split('-', expand=True)[1]

    airport = airport.groupby(['state_code', 'city'], as_index=False).agg({
        'airport_id': 'size',
        'elevation_ft': 'mean',
        'type': pd.Series.mode
    })
    airport.columns = [
        'state_code',
        'city',
        'no_airports',
        'avg_elevation',
        'most_common_type'
    ]
    airport['most_common_type'] = airport['most_common_type'].apply(lambda x: x if isinstance(x, str) else pd.NA)

    return airport