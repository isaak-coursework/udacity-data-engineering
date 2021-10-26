import logging

import pandas as pd

from lib.sas_code_lookups import (
    country_codes,
    city_codes,
    arrival_mode_codes,
    state_codes,
    visa_codes
)

log = logging.getLogger(__name__)

def _map_columns_to_types(
    df: pd.DataFrame, 
    type_map: dict[str, type]
) -> pd.DataFrame:
    for col, type_ in type_map.items():
        try:
            df[col] = df[col].astype(type_)
        except ValueError as e:
            log.warning(f"Type casting failed for column: {col}")
            raise
    return df

def _map_sas_codes(immi: pd.DataFrame) -> pd.DataFrame:
    immi['country_of_citizenship'] = immi['country_of_citizenship_code'].map(country_codes)
    immi['country_of_residence'] = immi['country_of_residence_code'].map(country_codes)
    immi['arrival_port'] = immi['arrival_port_code'].map(city_codes)
    immi['arrival_mode'] = immi['arrival_mode_code'].map(arrival_mode_codes)
    immi['state_settled'] = immi['state_settled_code'].map(state_codes)
    immi['visa_reason'] = immi['visa_code'].map(visa_codes)

    return immi

def _convert_sas_date(sas_date_col: pd.Series) -> pd.Series:
    base_date = pd.Timestamp('1960-1-1')
    days_passed_col = pd.to_timedelta(sas_date_col, unit='D')

    return base_date + days_passed_col

def clean_immigration_data(immi: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning immigration data...")
    col_names = {
        'cicid': 'immigrant_id',
        'i94yr': 'year',
        'i94mon': 'month',
        'i94cit': 'country_of_citizenship_code',
        'i94res': 'country_of_residence_code',
        'i94port': 'arrival_port_code',
        'arrdate': 'arrival_date',
        'depdate': 'departure_date',
        'i94mode': 'arrival_mode_code',
        'airline': 'airline',
        'fltno': 'flight_number',
        'i94addr': 'state_settled_code',
        'i94bir': 'age',
        'biryear': 'birth_year',
        'i94visa': 'visa_code',
        'visatype': 'visa_type',
        'gender': 'gender',
    }
    immi = immi[col_names.keys()].rename(columns=col_names)

    dtypes = {
        'immigrant_id': pd.Int64Dtype(),
        'year': pd.Int64Dtype(),
        'month': pd.Int64Dtype(),
        'country_of_citizenship_code': pd.Int64Dtype(),
        'country_of_residence_code': pd.Int64Dtype(),
        'arrival_port_code': str,
        'arrival_date': pd.Int64Dtype(),
        'departure_date': pd.Int64Dtype(),
        'arrival_mode_code': pd.Int64Dtype(),
        'airline': str,
        'flight_number': str,
        'state_settled_code': str,
        'age': pd.Int64Dtype(),
        'birth_year': pd.Int64Dtype(),
        'visa_code': pd.Int64Dtype(),
        'visa_type': str,
        'gender': str,
    }
    immi = _map_columns_to_types(immi, dtypes)

    immi = _map_sas_codes(immi)

    date_cols = [
        'departure_date',
        'arrival_date'
    ]
    for col in date_cols:
        immi[col] = _convert_sas_date(immi[col])

    # See: https://regex101.com/r/hICz3H/1
    arrival_port_regex = r'^(.*?),? (\w{2})(?: \(BPS\))?$'
    immi[['arrival_port_city', 'arrival_port_state']] = immi['arrival_port'].str.extract(arrival_port_regex)

    return immi

def _map_coordinates_to_state():
    pass

def clean_temperature_data(temp: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning temperature data...")
    col_names = {
        'dt': 'date',
        'AverageTemperature': 'avg_temperature',
        'AverageTemperatureUncertainty': 'avg_temperature_uncertainty',
        'City': 'city',
        'Country': 'country',
        'Latitude': 'latitude',
        'Longitude': 'longitude',
    }
    temp = temp.rename(columns=col_names)

    temp['date'] = pd.to_datetime(temp['date'])

    to_capitalize = [
        'city',
        'country'
    ]
    for col in to_capitalize:
        temp[col] = temp[col].str.upper()

    return temp

def clean_us_demographic_data(demo: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning US Demographic data...")
    col_names = {
        'City': 'city',
        'State': 'state',
        'Median Age': 'median_age',
        'Male Population': 'male_population',
        'Female Population': 'female_population',
        'Total Population': 'total_population',
        'Number of Veterans': 'no_veterans',
        'Foreign-born': 'is_foreign_born',
        'Average Household Size': 'avg_household_size',
        'State Code': 'state_code',
        'Race': 'race',
        'Count': 'count',
    }
    demo = demo.rename(columns=col_names)

    to_capitalize = [
        'city',
        'state'
    ]
    for col in to_capitalize:
        demo[col] = demo[col].str.upper()

    return demo

def clean_airport_data(airport: pd.DataFrame) -> pd.DataFrame:
    log.info("Cleaning airport code data...")
    col_names = {
        'ident': 'airport_id',
        'iso_country': 'iso_country_code',
        'iso_region': 'iso_region_code',
        'municipality': 'city'
    }
    airport = airport.rename(columns=col_names)

    airport[['latitude', 'longitude']] = airport['coordinates'].str.split(', ', expand=True)
    airport = airport.drop('coordinates', axis=1)

    airport['city'] = airport['city'].str.upper()

    return airport
