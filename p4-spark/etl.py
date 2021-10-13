import configparser
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_unixtime, 
    monotonically_increasing_id,
    dayofweek
)


config = configparser.ConfigParser()
config.read('dl.cfg')

os.environ['AWS_ACCESS_KEY_ID']=config['AWS']['AWS_ACCESS_KEY_ID']
os.environ['AWS_SECRET_ACCESS_KEY']=config['AWS']['AWS_SECRET_ACCESS_KEY']


def create_spark_session() -> SparkSession:
    """Create a new Spark Session

    Returns:
        SparkSession: Active SparkSession object. 
    """
    spark = SparkSession \
        .builder \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:2.7.0") \
        .getOrCreate()
    return spark


def process_song_data(
    spark: SparkSession, 
    input_data: str, 
    output_data: str
) -> None:
    """Process song data and write the processed data to our data lake in 
    parquet file format. 

    Args:
        spark (SparkSession): The current Spark Session
        input_data (str): S3 bucket URL for the unprocessed data
        output_data (str): S3 target URL for the processed song data
    """
    # get filepath to song data file
    song_data_uri = f"{input_data}/song_data/*/*/*/*.json"
    
    # read song data file
    df = spark.read.json(song_data_uri)
    df.createOrReplaceTempView('staging_songs')

    # extract columns to create songs table
    songs_table = spark.sql("""
        SELECT 
            song_id,
            title,
            artist_id,
            year,
            duration
        FROM staging_songs
    """)
    
    # write songs table to parquet files partitioned by year and artist
    (songs_table.write
                .partitionBy('year','artist_id')
                .parquet(f'{output_data}/songs'))

    # extract columns to create artists table
    artists_table = spark.sql("""
        SELECT DISTINCT
            artist_id,
            artist_name AS name,
            artist_location AS location,
            artist_latitude AS latitude,
            artist_longitude AS longitude
        FROM staging_songs
    """)
    
    # write artists table to parquet files
    (artists_table.write
                  .partitionBy('artist_id')
                  .parquet(f'{output_data}/artists'))


def process_log_data(
    spark: SparkSession, 
    input_data: str, 
    output_data: str
):
    """Process song data and write the processed songplay log data to our data lake in 
    parquet file format. 

    Args:
        spark (SparkSession): The current Spark Session
        input_data (str): S3 bucket URL for the unprocessed data
        output_data (str): S3 target URL for the processed log data
    """
    # get filepath to log data file
    log_data = f"{input_data}/log_data/*/*/*.json"

    # read log data file
    df = spark.read.json(log_data)

    # filter by actions for song plays
    df = df.filter(df['page']=='NextSong')

    # Extract UTC start time
    df = df.withColumn('start_time', from_unixtime(df['ts']/1000))
    df = df.withColumn('weekday', dayofweek('start_time'))
    
    # Register DF as queryable table
    df.createOrReplaceTempView('staging_events')
    
    # extract columns to create time table
    time_table = spark.sql("""
        SELECT DISTINCT
            start_time,
            EXTRACT(HOUR FROM start_time) AS hour,
            EXTRACT(DAY FROM start_time) AS day,
            EXTRACT(WEEK FROM start_time) AS week,
            EXTRACT(MONTH FROM start_time) AS month,
            EXTRACT(YEAR FROM start_time) AS year,
            weekday
        FROM staging_events
    """)
    
    # write time table to parquet files partitioned by year and month
    (time_table.write
                .partitionBy('year', 'month')
                .parquet(f'{output_data}/time'))

    time_table.createOrReplaceTempView('time')

    # extract columns for users table    
    users_table = spark.sql("""
        SELECT DISTINCT
            userId AS user_id,
            firstName AS first_name,
            lastName AS last_name,
            gender,
            level
        FROM staging_events
    """)
    
    # write users table to parquet files
    users_table.write.parquet(f'{output_data}/users')

    # extract columns from joined song and log datasets to create songplays table 
    songplays_table = spark.sql("""
        SELECT 
            se.start_time,
            time.year,
            time.month,
            se.userid,
            se.level,
            ss.song_id,
            ss.artist_id,
            se.sessionid,
            se.location,
            se.useragent
        FROM staging_events se
        LEFT JOIN staging_songs ss
            ON se.artist = ss.artist_name AND se.song = ss.title
        LEFT JOIN time 
            ON time.start_time = se.start_time
    """).withColumn('songplay_id', monotonically_increasing_id())

    # write songplays table to parquet files partitioned by year and month
    (songplays_table.write
                    .partitionBy('year', 'month')
                    .parquet(f'{output_data}/songplays'))


def main():
    """Run data processing job to load data into Sparkfiy data lake. 
    """
    spark = create_spark_session()
    input_data = "s3a://udacity-dend/"
    output_data = "s3a://p4-spark/"
    
    process_song_data(spark, input_data, output_data)    
    process_log_data(spark, input_data, output_data)


if __name__ == "__main__":
    main()
