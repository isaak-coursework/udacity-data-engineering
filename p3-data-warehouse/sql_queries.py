import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

LOG_DATA_S3_URL = config.get('S3', 'LOG_DATA')
LOG_JSON_SCHEMA_S3_URL = config.get('S3', 'LOG_JSONPATH')
SONG_DATA_S3_URL = config.get('S3', 'SONG_DATA')
REGION = config.get('S3', 'REGION')

IAM_ROLE = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

######################################################################
# Staging Tables

# These tables are an intermediary step to allow any ETL 
# transformations to be done using SQL after an initial load
# of data from S3.
######################################################################

staging_events_table_create= """
    CREATE TABLE staging_events (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession SMALLINT,
        lastName TEXT,
        length NUMERIC,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration NUMERIC,
        sessionId INT,
        song TEXT,
        status INT,
        ts TIMESTAMP,
        userAgent TEXT,
        userId INT
    )
"""

staging_songs_table_create = """
    CREATE TABLE staging_songs (
        artist_id TEXT,
        artist_latitude NUMERIC,
        artist_location TEXT,
        artist_longitude NUMERIC,
        artist_name TEXT,
        duration NUMERIC,
        num_songs SMALLINT,
        song_id TEXT,
        title TEXT,
        year SMALLINT
    )
"""


######################################################################
# Warehouse Tables

# These tables will be exposed to users/client BI applications
# NOTE: Redshift does not enforce PRIMARY or FOREIGN KEYS!! 
# Key definitions are informal and for organizational purposes. 
######################################################################

songplay_table_create = """
    CREATE TABLE songplays (
        songplay_id INT IDENTITY(0, 1) PRIMARY KEY,
        start_time TIMESTAMP REFERENCES time(start_time),
        user_id INT REFERENCES users(user_id),
        level TEXT,
        song_id TEXT REFERENCES songs(song_id),
        artist_id TEXT REFERENCES artists(artist_id),
        session_id INT,
        location TEXT,
        user_agent TEXT
    )
    DISTKEY(user_id)
    INTERLEAVED SORTKEY(start_time, song_id, artist_id)
"""

user_table_create = """
    CREATE TABLE users (
        user_id INT PRIMARY KEY,
        first_name TEXT,
        last_name TEXT,
        gender TEXT,
        level TEXT 
    )
    DISTKEY(user_id)
    SORTKEY(user_id)
"""

song_table_create = """
    CREATE TABLE songs (
        song_id TEXT PRIMARY KEY,
        title TEXT,
        artist_id TEXT REFERENCES artists(artist_id),
        year SMALLINT,
        duration NUMERIC
    )
    DISTSTYLE all
    SORTKEY(song_id)
"""

artist_table_create = """
    CREATE TABLE artists (
        artist_id TEXT PRIMARY KEY,
        name TEXT,
        location TEXT,
        latitude NUMERIC,
        longitude NUMERIC
    )
    DISTSTYLE all
    SORTKEY(artist_id)
"""

time_table_create = """
    CREATE TABLE time (
        start_time TIMESTAMP PRIMARY KEY,
        hour SMALLINT,
        day SMALLINT, 
        week SMALLINT,
        month SMALLINT,
        year SMALLINT,
        weekday TEXT
    )
    DISTKEY(start_time)
    SORTKEY(start_time)
"""

# STAGING TABLES

# See: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-conversion.html#copy-timeformat
staging_events_copy = f"""
    COPY staging_events
    FROM {LOG_DATA_S3_URL}
    REGION {REGION}
    IAM_ROLE {IAM_ROLE}
    JSON {LOG_JSON_SCHEMA_S3_URL}
    TIMEFORMAT AS 'epochmillisecs'
"""

staging_songs_copy = f"""
    COPY staging_songs
    FROM {SONG_DATA_S3_URL}
    REGION {REGION}
    IAM_ROLE {IAM_ROLE}
    JSON 'auto'
"""

# FINAL TABLES

songplay_table_insert = """
    INSERT INTO songplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
    )
    SELECT 
        se.ts,
        se.userid,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent
    FROM staging_events se
    JOIN staging_songs ss
        ON se.artist = ss.artist_name AND se.song = ss.title
    WHERE se.page = 'NextSong'
"""

user_table_insert = """
    INSERT INTO users (
        user_id,
        first_name,
        last_name,
        gender,
        level
    )
    SELECT DISTINCT
        userid,
        firstname,
        lastname,
        gender,
        level
    FROM staging_events
    WHERE page = 'NextSong'
"""

song_table_insert = """
    INSERT INTO songs (
        song_id,
        title,
        artist_id,
        year,
        duration
    )
    SELECT 
        song_id,
        title,
        artist_id,
        year,
        duration
    FROM staging_songs
"""

artist_table_insert = """
    INSERT INTO artists (
        artist_id,
        name,
        location,
        latitude,
        longitude
    )
    SELECT 
        artist_id,
        artist_name,
        artist_location,
        artist_latitude,
        artist_longitude
    FROM staging_songs
"""

time_table_insert = """
    INSERT INTO time (
        start_time,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        ts,
        EXTRACT(HOUR FROM ts),
        EXTRACT(DAY FROM ts),
        EXTRACT(WEEK FROM ts),
        EXTRACT(MONTH FROM ts),
        EXTRACT(YEAR FROM ts),
        EXTRACT(WEEKDAY FROM ts)
    FROM staging_events
    WHERE page = 'NextSong'
"""

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    time_table_create,
    user_table_create, 
    artist_table_create, 
    song_table_create, 
    songplay_table_create, 
]

drop_table_queries = [
    staging_events_table_drop, 
    staging_songs_table_drop, 
    songplay_table_drop, 
    user_table_drop, 
    song_table_drop, 
    artist_table_drop, 
    time_table_drop
]

copy_table_queries = [
    staging_events_copy, 
    staging_songs_copy
]

insert_table_queries = [
    songplay_table_insert, 
    user_table_insert, 
    song_table_insert, 
    artist_table_insert, 
    time_table_insert
]
