import logging

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine


log = logging.getLogger(__name__)

def check_tables_not_null(db_engine: Engine):
    log.info('Ensuring all tables have been populated...')
    tables = [
        'immigration_base',
        'temperatures',
        'demographics',
        'airports',
        'immigrant_stay_length_by_state',
        'immigrants_by_city',
        'immigrants_by_state'
    ]
    for table in tables:
        results = db_engine.execute(f"""
            SELECT COUNT(*) > 0
            FROM {table}
        """)
        assert results.fetchone()[0] == True
        log.info(f"Table '{table}'' is not empty!")

def check_departures_after_arrivals(db_engine: Engine):
    log.info("Ensuring less than 1% of departures are listed as after arrivals...")
    results = db_engine.execute(f"""
        SELECT 
            COUNT(*)/(
                SELECT COUNT(*)
                FROM immigration_base
            )::DECIMAL
        FROM immigration_base
        WHERE departure_date < arrival_date
    """)

    assert results.fetchone()[0] < .01
    log.info("Check successful!")
    
if __name__ == '__main__':
    logging.basicConfig(
        level='INFO',
        format='[%(levelname)s - %(module)s.py]: %(message)s'
    )

    # In a real, non-local DB, the connection string should be loaded from environment
    # variables for security reasons. 
    db_engine = create_engine(f'postgresql+psycopg2://postgres:postgres@localhost:5432/dev')

    check_tables_not_null(db_engine)
    check_departures_after_arrivals(db_engine)
