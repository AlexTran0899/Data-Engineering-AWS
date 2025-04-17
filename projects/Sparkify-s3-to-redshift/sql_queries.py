import configparser

# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')
ARN = config['IAM_ROLE']['ARN']
LOG_DATA = config['S3']['LOG_DATA']
LOG_JSONPATH = config['S3']['LOG_JSONPATH']
SONG_DATA = config['S3']['SONG_DATA']
S3_REGION = config['S3']['REGION']



# DROP TABLES
staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist VARCHAR(200),
    auth VARCHAR(20),
    firstName VARCHAR(50),
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR(50),
    length DOUBLE PRECISION,
    level VARCHAR(10),
    location VARCHAR(200),
    method VARCHAR(3),
    page VARCHAR(50),
    registration DOUBLE PRECISION,
    sessionId INT,
    song VARCHAR(200),
    status INT,
    ts TIMESTAMP,
    userAgent VARCHAR(200),
    userId INT
)
DISTKEY(sessionId)
SORTKEY(sessionId, itemInSession);
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    song_id VARCHAR(50) PRIMARY KEY,
    num_songs INT,
    artist_id VARCHAR(50),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION,
    artist_location VARCHAR(200),
    artist_name VARCHAR(200),
    title VARCHAR(200),
    duration DOUBLE PRECISION,
    year INT
)
DISTKEY(song_id)
SORTKEY(song_id);
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id INT IDENTITY(0,1) PRIMARY KEY,
    start_time TIMESTAMP NOT NULL,
    user_id INT NOT NULL,
    level VARCHAR(10),
    song_id VARCHAR(50),
    artist_id VARCHAR(50),
    session_id INT,
    location VARCHAR(200),
    user_agent VARCHAR(200)
)
DISTKEY(start_time)
SORTKEY(start_time);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    gender CHAR(1),
    level VARCHAR(10)
)
SORTKEY(user_id);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id VARCHAR(50) PRIMARY KEY,
    title VARCHAR(200),
    artist_id VARCHAR(50),
    year INT,
    duration DOUBLE PRECISION
)
DISTKEY(song_id)
SORTKEY(song_id);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id VARCHAR(50) PRIMARY KEY,
    artist_name VARCHAR(200),
    artist_location VARCHAR(200),
    artist_latitude DOUBLE PRECISION,
    artist_longitude DOUBLE PRECISION
)
SORTKEY(artist_id);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time TIMESTAMP PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday VARCHAR(10)
)
SORTKEY(start_time);
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM '{log_data}'
CREDENTIALS 'aws_iam_role={arn}'
JSON '{log_jsonpath}'
REGION '{region}'
TIMEFORMAT 'epochmillisecs';
""").format(
    log_data=LOG_DATA,
    arn=ARN,
    log_jsonpath=LOG_JSONPATH,
    region=S3_REGION
)

staging_songs_copy = ("""
    copy staging_songs
    from '{song_data}'
    credentials 'aws_iam_role={arn}'
    json 'auto'
    REGION '{region}'
""").format(
    song_data=SONG_DATA,
    arn=ARN,
    log_jsonpath=LOG_JSONPATH,
    region=S3_REGION
)

# FINAL TABLES


songplay_table_insert = ("""
INSERT INTO songplays (start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
SELECT DISTINCT
    se.ts AS start_time,
    se.userId AS user_id,
    se.level AS level,
    ss.song_id AS song_id,
    ss.artist_id AS artist_id,
    se.sessionId AS session_id,
    se.location AS location,
    se.userAgent AS user_agent
FROM staging_events as se
JOIN staging_songs as ss 
ON se.artist = ss.artist_name and se.song = ss.title
WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
INSERT INTO users(user_id, first_name, last_name, gender, level)
SELECT DISTINCT 
    se.userId AS user_id,
    se.firstName AS first_name,
    se.lastName AS last_name,
    se.gender as gender,
    se.level as level
FROM staging_events as se
where se.userId IS NOT NULL
""")

song_table_insert = ("""
INSERT INTO songs(song_id, title, artist_id, year, duration)
SELECT DISTINCT
    ss.song_id AS song_id,
    ss.title AS title,
    ss.artist_id AS artist_id,
    ss.year AS year,
    ss.duration AS duration
FROM staging_songs as ss
WHERE ss.song_id IS NOT NULL
""")

artist_table_insert = ("""
INSERT INTO artists(artist_id, artist_name, artist_location, artist_latitude, artist_longitude)
SELECT DISTINCT
    ss.artist_id AS artist_id,
    ss.artist_name AS artist_name,
    ss.artist_location AS artist_location,
    ss.artist_latitude AS artist_latitude,
    ss.artist_longitude AS artist_longitude
FROM staging_songs as ss
WHERE ss.artist_id IS NOT NULL
""")

time_table_insert = ("""
INSERT INTO time(start_time, hour, day, week, month, year, weekday)
SELECT DISTINCT
    sp.start_time AS start_time,
    EXTRACT(hour FROM sp.start_time) AS hour,
    EXTRACT(day FROM sp.start_time) AS day,
    EXTRACT(week FROM sp.start_time) AS week,
    EXTRACT(month FROM sp.start_time) AS month,
    EXTRACT(year FROM sp.start_time) AS year,
    EXTRACT(weekday FROM sp.start_time) AS weekday
FROM songplays as sp
WHERE sp.start_time IS NOT NULL
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
