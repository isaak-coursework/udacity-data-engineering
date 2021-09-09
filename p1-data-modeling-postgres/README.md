# Sparkify DB
Sparkify<sup>TM</sup> is a new major player in the music streaming industry. We have *tens* of users already, but so far have no clue what those users have been listening to. 

Our new PostGres database will enable Sparkify's analysts to quickly identify the trends in user activity. 

## Data Model
The database uses a **star schema** to allow for easy, fast analysis of logged song plays without the need for overly complicated queries. It is most important that our analysts be able to answer business questions quickly and clearly. 

### Tables
* **songplays** - Logged, timestamped records of users playing a song

    Columns: 
    * songplay_id
    * start_time
    * user_id
    * level
    * song_id
    * artist_id
    * session_id
    * location
    * user_agent

* **users**

    Columns: 
    * user_id
    * first_name
    * last_name
    * gender
    * level

* **songs** - Details about individual songs available on Sparkify<sup>TM</sup>

    Columns: 
    * song_id
    * title
    * artist_id
    * year
    * duration

* **artists** - Details about all artists with songs available on Sparkify<sup>TM</sup>

    Columns: 
    * artist_id
    * name
    * location
    * latitude
    * longitude

* **time** - Timestamps of records in songplays broken into other units for easy analysis

    Columns: 
    * start_time
    * hour
    * day
    * week
    * month
    * year
    * weekday
