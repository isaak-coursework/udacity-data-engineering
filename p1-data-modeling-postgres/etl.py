import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
    Process a single song file, parsing and 
    inserting data into tables 'songs' and 'artists'. 
    
    Parameters
    ----------
    cur : 
        psycopg2 cursor object
    filepath :
        absolute or relative path to song file 

    Returns
    -------
    None
    """
    series = pd.read_json(filepath, typ='series')
    
    song_cols = [
        "song_id",
        "title",
        "artist_id",
        "year",
        "duration"
    ]
    song_data = series[song_cols].tolist()
    cur.execute(song_table_insert, song_data)
    
    artist_cols = [
        "artist_id",
        "artist_name",
        "artist_location",
        "artist_latitude",
        "artist_longitude"
    ]
    
    artist_data = series[artist_cols].tolist()
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    Process a single log file, extracting user plays of individual
    songs and inserting data into 'time', 'users', and 'songplays' tables.
    
    Parameters
    ----------
    cur : 
        psycopg2 cursor object
    filepath :
        absolute or relative path to log file 

    Returns
    -------
    None
    """
    df = pd.read_json(filepath, lines=True)

    df = df = df[df["page"]=="NextSong"]

    df["start_time"] = pd.to_datetime(df["ts"], unit="ms")
    t = df["start_time"]
    
    time_data = (
        t,
        t.dt.hour,
        t.dt.day,
        t.dt.week,
        t.dt.month,
        t.dt.year,
        t.dt.weekday
    )
    time_cols = (
        "start_time",
        "hour",
        "day",
        "week",
        "month",
        "year",
        "weekday"
    )
    time_df = pd.concat(time_data, axis=1, keys=time_cols)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    user_cols = [
        "userId",
        "firstName",
        "lastName",
        "gender",
        "level"
    ]
    user_df = df[user_cols]

    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    for index, row in df.iterrows():
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        songplay_data = (
            row.start_time,
            row.userId,
            row.level,
            songid,
            artistid,
            row.sessionId,
            row.location,
            row.userAgent
        )
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
    Retrieve paths to all files in specified dir, then iterate over all files,
    passing filepaths to the supplied function
    
    Parameters
    ----------
    cur : 
        psycopg2 cursor object
    conn : 
        psycopg2 connection object
    filepath :
        absolute or relative path to song or log file
    func : 
        function to parse & load file

    Returns
    -------
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    Connect to DB, run data processing for all song & log files, close DB 
    connection. 

    Returns
    -------
    None
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()