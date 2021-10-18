from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator

from operators import (
    StageToRedshiftOperator, 
    LoadFactOperator,
    LoadDimensionOperator, 
    DataQualityOperator
)
import helpers.sql_queries as queries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2019, 1, 12),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG(
    dag_id='udac_example_dag',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)

with dag:
    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='stage_events',
        table_name='staging_events',
        s3_url='s3://udacity-dend/log_data',
        s3_json_schema='s3://udacity-dend/log_json_path.json'
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='stage_songs',
        table_name='staging_songs',
        s3_url='s3://udacity-dend/song_data'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table_name='songplays',
        sql=queries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        table_name='users',
        sql=queries.user_table_insert
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        table_name='songs',
        sql=queries.song_table_insert
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        table_name='artists',
        sql=queries.artist_table_insert
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table_name='time',
        sql=queries.time_table_insert
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        table_names=[
            'songplays',
            'songs',
            'artists',
            'time',
            'users'
        ]
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> \
    (
        stage_events_to_redshift,
        stage_songs_to_redshift
    ) >> \
    load_songplays_table >> \
    (
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ) >> \
    run_quality_checks >> \
    end_operator
