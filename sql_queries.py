import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS \"user\""
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events(
artist VARCHAR(256),
auth VARCHAR(50),
firstName VARCHAR(50),
gender CHAR(1),
iteminSession INT,
lastName VARCHAR(50),
length DECIMAL(10,5),
level VARCHAR(10),
location VARCHAR(255),
method VARCHAR(10),
page VARCHAR(50),
registration BIGINT,
sessionid INT,
song VARCHAR(256),
status INT,
ts BIGINT,
userAgent VARCHAR(256),
userid INT);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs(
num_songs SMALLINT,
artist_id CHAR(18),
artist_latitude REAL,
artist_longitude REAL,
artist_location VARCHAR(256),
artist_name VARCHAR(256),
song_id CHAR(18),
title VARCHAR(256) DISTKEY,
duration REAL,
year SMALLINT);
""")

songplay_table_create = ("""
CREATE TABLE songplay(
songplay_id CHAR(18),
start_time TIMESTAMP,
user_id INT,
level VARCHAR(10),
song_id VARCHAR(50),
artist_id VARCHAR(50),
session_id INT,
location VARCHAR(256),
user_agent VARCHAR(256)
);
""")

user_table_create = ("""
CREATE TABLE \"user\"(
user_id INT,
first_name VARCHAR(50),
last_name VARCHAR(50),
gender CHAR(1),
level VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE song(
song_id CHAR(18),
title VARCHAR(256),
artist_id CHAR(18),
year TIMESTAMP ,
duration NUMERIC
);
""")

artist_table_create = ("""
CREATE TABLE artist(
artist_id CHAR(18),
name VARCHAR(256),
location VARCHAR(50),
latitude NUMERIC,
longitude NUMERIC
);
""")

time_table_create = ("""
CREATE TABLE time(
start_time TIMESTAMP,
hour INT,
day INT,
week INT,
month INT,
year INT,
weekday INT
);
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events
FROM 's3://udacity-dend/log_data'
IAM_ROLE ''
REGION 'us-west-2'
FORMAT AS JSON 'auto'
;
""").format()

staging_songs_copy = ("""
COPY staging_songs
FROM 's3://udacity-dend/song_data'
IAM_ROLE ''
REGION 'us-west-2'
FORMAT AS JSON 'auto'
;
""").format()

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (songplay_id,start_time,user_id,level,song_id,artist_id,session_id,location,user_agent)
SELECT 
 e.ts as songplay_id,
 TIMESTAMP 'epoch' + e.ts/1000 * interval '1 second' as start_time,
 e.userId as user_id,
 e.level,
 e.song_id,
 e,artist_id,
 e.sessiomId as session_id,
 e.location,
 e.userAgent as user_agent
FROM staging_events e
JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name WHERE e.page = 'NEXTSong';
""")

user_table_insert = ("""
INSERT INTO users (userId,firstName,lastName,gender,level)
SELECT DISTINCT userId, fristName, lastName, gender, level
from stging_events
WHERE userId IS NOT NULL;
""")

song_table_insert = ("""
INSERT INTO songs (song_id,title,artist_id,year,duration)
SELECT DISTINCT song_id, tittle, artist_id, year, duration
from staging_songs
WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist (artist_id,artist_name,artist_location,artist_lattitude,artist_longitude)
SELECT DistiNCT artist_id, artist_name, artist_location, artist_latitude, artist_longitude from staging_songs
WHERE artist_id IS NOT NULL;
""")

time_table_insert = ("""
INSERT INTO artist (start_time, hour, day, week, month, year,weekday)
SELECT DISTINCT 
     TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
     Extract(HOUR FROM start_time) as hour,
     EXTRACT(DAY FROM start_time) as day,
     EXTRACT(WEEK FROM start_time) as week,
     EXTRACT(MONTH FROM start_time) as month,
     EXTRACT(YEAR FROM start_time) as year,
     EXTRACT(DOW FROM start_time) as weekday
FROM staging_events
WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
