import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


#config = configparser.ConfigParser()
#config.read_file(open('dwh.cfg'))

S3_log_jsonpath = config.get('S3','log_jsonpath')
S3_log_data = config.get('S3','log_data')
S3_song_data = config.get('S3','song_data')
IAM_ROLE_arn = config.get('IAM_ROLE','arn')


# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events_table"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs_table"
songplay_table_drop = "DROP TABLE IF EXISTS songplay_table"
user_table_drop = "DROP TABLE IF EXISTS user_table"
song_table_drop = "DROP TABLE IF EXISTS song_table"
artist_table_drop = "DROP TABLE IF EXISTS artist_table"
time_table_drop = "DROP TABLE IF EXISTS time_table"

# CREATE TABLES

staging_events_table_create= ("""
            CREATE TABLE IF NOT EXISTS staging_events_table (
            artist_name varchar,
            auth varchar,
            firstName varchar,
            gender varchar,
            itemInSession int,
            lastName varchar,
            lenght numeric,
            level varchar,
            location varchar,
            method varchar,
            page varchar,
            registration numeric,
            sessionId int,
            song varchar,
            status int,
            ts bigint,
            userAgent varchar,
            userId varchar)
            
""")

staging_songs_table_create = ("""
        CREATE TABLE IF NOT EXISTS staging_songs_table (
        song_id varchar,
        title varchar,
        duration numeric,
        year int,
        artist_name varchar,
        num_songs varchar,
        artist_id varchar,
        artist_latitude DECIMAL,
        artist_longitude DECIMAL,
        artist_location varchar)
        
""")

songplay_table_create = (""" 
        CREATE TABLE IF NOT EXISTS songplay_table(
        Songplay_id int IDENTITY(1, 1) PRIMARY KEY,
        start_time TIMESTAMP  NOT NULL SORTKEY,
        user_id varchar NOT NULL DISTKEY,
        level varchar,
        song_id varchar,
        artist_id varchar,
        session_id int NOT NULL,
        location varchar,
        user_agent varchar)
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS user_table (
    user_id varchar PRIMARY KEY NOT NULL SORTKEY,
    first_Name varchar,
    last_Name varchar,
    gender varchar,
    level varchar)
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_table (
    song_id varchar PRIMARY KEY SORTKEY,
    title varchar,
    artist_id varchar NOT NULL,
    year int, 
    duration numeric NOT NULL)
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artist_table (
    artist_id varchar PRIMARY KEY SORTKEY,
    name varchar NOT NULL,
    location varchar,
    latitude DECIMAL,
    longitude DECIMAL)
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time_table(
    start_time timestamp PRIMARY KEY SORTKEY,
    hour int,
    day int,
    week int,
    month int,
    year int DISTKEY,
    weekday int)
""")

# STAGING TABLES

staging_events_copy = ("""
    copy staging_events_table from '{}'
    credentials 'aws_iam_role={}'
    JSON '{}'
    region 'us-west-2';
""").format(S3_log_data, IAM_ROLE_arn, S3_log_jsonpath)

staging_songs_copy = (""" 
    copy staging_songs_table from '{}'
    credentials 'aws_iam_role={}'
    JSON 'auto'
    region 'us-west-2';
""").format(S3_song_data, IAM_ROLE_arn)


# FINAL TABLES

songplay_table_insert = ("""

    INSERT INTO songplay_table (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT 
    TIMESTAMP 'epoch' + (s_event.ts/1000*INTERVAL '1 second') as start_time,
    s_event.userId,
    s_event.level,
    s_song.song_id,
    s_song.artist_id,
    s_event.sessionId,
    s_event.location,
    s_event.userAgent
    FROM staging_events_table s_event
    JOIN staging_songs_table s_song
    ON s_event.artist_name = s_song.artist_name
    AND s_event.song = s_song.title
    AND s_event.lenght = s_song.duration
    WHERE s_event.page = 'NextSong' 

""")

user_table_insert = ("""
    INSERT INTO user_table
        WITH uniq_staging_events_table AS (
        SELECT userId, firstName, lastName, gender, level, ROW_NUMBER() OVER(PARTITION BY userid ORDER BY ts DESC) AS rank
        FROM staging_events_table
        WHERE userid IS NOT NULL)
    SELECT userid, firstName, lastName, gender, level
    FROM uniq_staging_events_table
    WHERE rank = 1 AND  userid IS NOT NULL AND firstName IS NOT NULL
""")

song_table_insert = ("""
     INSERT INTO song_table 
     SELECT song_id, title ,artist_id, year, duration
     FROM staging_songs_table
""")

artist_table_insert = ("""
    INSERT INTO artist_table
    SELECT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
    FROM staging_songs_table
""")

time_table_insert = ("""
    INSERT INTO time_table
    SELECT start_time,
    extract(hour from start_time),
    extract(day from start_time),
    extract(week from start_time), 
    extract(month from start_time),
    extract(year from start_time), 
    extract(dayofweek from start_time)
    FROM songplay_table 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
