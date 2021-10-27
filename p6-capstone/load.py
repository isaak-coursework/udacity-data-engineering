import logging
import argparse

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from lib.extract import Extractor
from lib.clean import (
    clean_immigration_data,
    clean_temperature_data,
    clean_us_demographic_data,
    clean_airport_data
)
from lib.transform import (
    calc_avg_monthly_temp_by_city_and_state,
    agg_airports_by_city_and_state
)


log = logging.getLogger(__name__)

def process_immigration_data(
    extractor: Extractor,
    db_engine: Engine
):
    immi = extractor.immigration_data()
    immi = clean_immigration_data(immi)

    table_name = 'immigration_base'
    log.info(f"Loading table: {table_name}")
    immi.to_sql(
        table_name, 
        db_engine, 
        if_exists='append',
        chunksize=10000,
        method='multi',
        index=False
    )

def process_temperature_data(
    extractor: Extractor,
    db_engine: Engine,
    use_google_api: bool = False
):
    temp = extractor.temperature_data()
    temp = clean_temperature_data(temp)
    temp = calc_avg_monthly_temp_by_city_and_state(temp, use_google_api=use_google_api)

    table_name = 'temperatures'
    log.info(f"Loading table: {table_name}")
    temp.to_sql(
        table_name, 
        db_engine, 
        if_exists='append',
        index=False
    )

def process_demographic_data(
    extractor: Extractor,
    db_engine: Engine
):
    demo = extractor.us_city_demographic_data()
    demo = clean_us_demographic_data(demo)

    table_name = 'demographics'
    log.info(f"Loading table: {table_name}")
    demo.to_sql(
        table_name, 
        db_engine, 
        if_exists='append',
        index=False
    )

def process_airport_data(
    extractor: Extractor,
    db_engine: Engine
):
    airport = extractor.airport_data()
    airport = clean_airport_data(airport)
    airport = agg_airports_by_city_and_state(airport)

    table_name = 'airports'
    log.info(f"Loading table: {table_name}")
    airport.to_sql(
        table_name, 
        db_engine, 
        if_exists='append',
        index=False
    )

def build_db(local: bool, use_google_api: bool):
    extractor = Extractor(local=local)

    # In a real, non-local DB, the connection string should be loaded from environment
    # variables for security reasons. 
    db_engine = create_engine(f'postgresql+psycopg2://postgres:postgres@localhost:5432/dev')

    process_immigration_data(extractor, db_engine)
    process_temperature_data(extractor, db_engine, use_google_api=use_google_api)
    process_demographic_data(extractor, db_engine)
    process_airport_data(extractor, db_engine)

    log.info("Database build complete! ")

if __name__ =='__main__':
    load_dotenv('./.env')
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s - %(module)s.py]: %(message)s'
    )

    parser=argparse.ArgumentParser(
        description="Create data warehouse!"
    )
    parser.add_argument(
        '-l', '--local',
        action='store_true',
        dest="local",
        default=False,
        help='Run on local machine'
    )
    parser.add_argument(
        '-g', '--google-api',
        action='store_true',
        dest="google",
        default=False,
        help="Enrich data using google's geocoder API. If false, will use pre-enriched data"
    )
    args = parser.parse_args()

    build_db(local=args.local, use_google_api=args.google)
