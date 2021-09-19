import argparse
import configparser

import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """COPY data from S3 into staging tables. 

    Args:
        cur (psycopg2 cursor): Cursor for DB connection
        conn (psycopg2.connection): DB connection
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """Insert relevant values from staging tables into star schema. 

    Args:
        cur (psycopg2 cursor): Cursor for DB connection
        conn (psycopg2.connection): DB connection
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main(insert_only, copy_only):
    """Run Sparkify ETL

    Args:
        insert_only (bool): Only run INSERT statements to create warehouse 
            data model. 
        copy_only (bool): Only run COPY statements to pull data from S3 into 
            staging tables. 

    Raises:
        ValueError: Cannot pass both 'insert_only' and 'copy_only'. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    if insert_only and copy_only:
        raise ValueError('To run COPY and INSERTs, run without flags!')
    
    if not insert_only:
        load_staging_tables(cur, conn)

    if not copy_only:
        insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Load Sparkify data to Redshift!"
    )
    parser.add_argument(
        '-i', '--insert',
        action='store_true',
        dest="insert_only",
        default=False,
        help='Only run INSERT statements to extract from staging tables. '
    )
    parser.add_argument(
        '-c','--copy',
        action='store_true',
        dest="copy_only",
        default=False,
        help='Only run COPY statements to load data from S3'
    )
    args = parser.parse_args()
    
    main(args.insert_only, args.copy_only)