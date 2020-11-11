import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""CREATE TABLE IF NOT EXISTS staging_events (artist VARCHAR,\
auth VARCHAR,\
firstName VARCHAR,\
gender VARCHAR,\
iteminSession INT,\
lastName VARCHAR,\
length VARCHAR,\
level VARCHAR,\
location VARCHAR,\
method VARCHAR,\
page VARCHAR,\
registration NUMERIC,\
sessionId INT,\
song VARCHAR,\
status INT,\
ts NUMERIC,\
userAgent text,\
userId INT)""")

staging_songs_table_create = ("""CREATE TABLE IF NOT EXISTS staging_songs (num_songs INT,\
artist_id VARCHAR,\
artist_latitude NUMERIC,\
artist_longitude NUMERIC,\
artist_location VARCHAR,\
artist_name VARCHAR,\
song_id VARCHAR,\
title VARCHAR,\
duration NUMERIC,\
year INT)""")


songplay_table_create = ("""CREATE TABLE IF NOT EXISTS song_plays (songplay_id INT IDENTITY (0,1) PRIMARY KEY, \
start_time TIMESTAMP NOT NULL,\
user_id VARCHAR NOT NULL,\
level VARCHAR,\
song_id VARCHAR NOT NULL,\
artist_id VARCHAR NOT NULL,\
session_id VARCHAR NOT NULL,\
location VARCHAR,\
user_agent VARCHAR)""")

user_table_create = ("""CREATE TABLE IF NOT EXISTS users (user_id VARCHAR PRIMARY KEY,\
first_name VARCHAR,\
last_name VARCHAR,\
gender VARCHAR,\
level VARCHAR)""")

song_table_create = ("""CREATE TABLE IF NOT EXISTS songs (song_id VARCHAR PRIMARY KEY,\
title VARCHAR,\
artist_id VARCHAR NOT NULL,\
year INT,\
duration FLOAT)""")

artist_table_create = ("""CREATE TABLE IF NOT EXISTS artists (artist_id VARCHAR PRIMARY KEY,\
name VARCHAR,\
location VARCHAR,\
latitude FLOAT,\
longitude FLOAT)""")

time_table_create = ("""CREATE TABLE IF NOT EXISTS time (start_time TIMESTAMP PRIMARY KEY,\
hour  INT,
day   INT,
week  INT,
month INT,
year  INT,
weekday INT)""")

# STAGING TABLES

staging_events_copy = (""" COPY staging_events from {}
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
JSON {}
""").format(config.get('S3', 'LOG_DATA'), config.get('IAM_ROLE', 'ARN'), config.get('S3', 'LOG_JSONPATH'))

staging_songs_copy = (""" COPY staging_songs from {}
credentials 'aws_iam_role={}'
region 'us-west-2' compupdate off
JSON 'auto' truncatecolumns
""").format(config.get('S3', 'SONG_DATA'), config.get('IAM_ROLE', 'ARN'))

# FINAL TABLES

songplay_table_insert = (""" INSERT INTO song_plays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' , e.userId, e.level, s.song_id, a.artist_id, e.sessionId, a.location, e.userAgent
FROM staging_events e
LEFT JOIN staging_songs s ON s.title = e.song AND e.length=s.duration
LEFT JOIN artists a ON a.name = e.artist
WHERE e.page ='NextSong' AND s.song_id IS NOT NULL AND a.artist_id IS NOT NULL""")

user_table_insert = (""" INSERT INTO users (user_id, first_name, last_name, gender, level)
SELECT DISTINCT userID, firstName, LastName, gender, level
FROM staging_events 
WHERE userId IS NOT NULL AND page ='NextSong'""")

song_table_insert = (""" INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT song_id, title, artist_id, year, duration
FROM staging_songs
WHERE song_id IS NOT NULL
""")

artist_table_insert = (""" INSERT INTO artists (artist_id, name, location, latitude,  longitude)
SELECT DISTINCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude
FROM staging_songs
WHERE artist_id IS NOT NULL """)

time_table_insert = (""" INSERT INTO time (start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT start_time, DATE_PART(hr, start_time), DATE_PART(d, start_time), DATE_PART(w, start_time),
        DATE_PART(mon, start_time), DATE_PART(y, start_time), DATE_PART(dow,start_time)
FROM song_plays
WHERE start_time IS NOT NULL """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
