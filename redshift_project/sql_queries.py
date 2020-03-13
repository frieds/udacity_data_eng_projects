import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS fact_songplays"
user_table_drop = "DROP TABLE IF EXISTS dim_users"
song_table_drop = "DROP TABLE IF EXISTS dim_songs"
artist_table_drop = "DROP TABLE IF EXISTS dim_artists"
time_table_drop = "DROP TABLE IF EXISTS dim_time"

# CREATE TABLES

staging_events_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_events (
                                  artist          VARCHAR,
                                  auth            VARCHAR,
                                  firstName       VARCHAR,
                                  gender          CHAR(1),
                                  itemInSession   INTEGER,
                                  lastName        VARCHAR,
                                  length          FLOAT,
                                  level           VARCHAR,
                                  location        VARCHAR,
                                  method          VARCHAR,
                                  page            VARCHAR,
                                  registration    FLOAT,
                                  sessionId       INTEGER,
                                  song            VARCHAR,
                                  status          INTEGER,
                                  ts              BIGINT,
                                  userAgent       VARCHAR,
                                  userId          INTEGER
                              );
                              """)

staging_songs_table_create = ("""
                              CREATE TABLE IF NOT EXISTS staging_songs (
                                  num_songs          INTEGER, 
                                  artist_id          VARCHAR,
                                  artist_latitude    FLOAT, 
                                  artist_longitude   FLOAT,
                                  artist_location    VARCHAR,
                                  artist_name        VARCHAR, 
                                  song_id            VARCHAR, 
                                  title              VARCHAR,
                                  duration           FLOAT,
                                  year               INTEGER
                              );
                              """)

songplay_table_create = ("""
                         CREATE TABLE IF NOT EXISTS fact_songplays (
                             id             INTEGER IDENTITY(1,1) PRIMARY KEY,
                             start_time     TIMESTAMP NOT NULL,
                             user_id        INTEGER NOT NULL,
                             level          VARCHAR,
                             song_id        VARCHAR, 
                             artist_id      VARCHAR,
                             session_id     INTEGER, 
                             location       VARCHAR, 
                             user_agent     VARCHAR
                         )
                         """)

user_table_create = ("""
                     CREATE TABLE IF NOT EXISTS dim_users (
                         id            INTEGER PRIMARY KEY, 
                         first_name    VARCHAR, 
                         last_name     VARCHAR, 
                         gender        VARCHAR, 
                         level         VARCHAR
                     )
                     """)

song_table_create = ("""
                     CREATE TABLE IF NOT EXISTS dim_songs (
                         id               VARCHAR PRIMARY KEY, 
                         title            VARCHAR, 
                         artist_id        VARCHAR NOT NULL, 
                         year             INTEGER, 
                         duration         FLOAT
                     )
                     """)

artist_table_create = ("""
                       CREATE TABLE IF NOT EXISTS dim_artists (
                           id             VARCHAR PRIMARY KEY, 
                           name           VARCHAR, 
                           location       VARCHAR, 
                           latitude       FLOAT, 
                           longitude      FLOAT
                       )
                       """)

time_table_create = ("""
                     CREATE TABLE IF NOT EXISTS dim_time (
                         start_time        TIMESTAMP PRIMARY KEY, 
                         hour              INTEGER, 
                         day               INTEGER, 
                         week              INTEGER, 
                         month             INTEGER, 
                         year              INTEGER, 
                         weekday           INTEGER
                     )
                     """)

# STAGING TABLES
staging_events_copy = ("""
                       COPY staging_events 
                       FROM {} 
                       IAM_ROLE {} 
                       json 'auto'
                       """
                       .format(config['S3']['LOG_DATA_S3_PREFIX_LOCATION'], 
                               config['IAM_ROLE']['ARN'])
                       )

staging_songs_copy = ("""
                      COPY staging_songs 
                      FROM {} 
                      IAM_ROLE {} 
                      json 'auto'
                      """
                      .format(config['S3']['SONG_DATA_S3_PREFIX_LOCATION'], 
                              config['IAM_ROLE']['ARN'])
                      )

# FINAL TABLES

# we want every record from staging_events; join to staging_songs to get any song and artist id                   
songplay_table_insert = ("""
                         INSERT INTO fact_songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)   
                         SELECT timestamp 'epoch' + se.ts/1000 * interval '1 second' AS start_time,   
                                se.userId AS user_id,
                                se.level,
                                ss.song_id,
                                ss.artist_id,
                                se.sessionId AS session_id,
                                se.location,
                                se.userAgent as user_agent
                         FROM staging_events se
                         LEFT JOIN staging_songs ss
                         ON se.artist = ss.artist_name
                         AND se.song = ss.title
                         AND se.length = ss.duration            
                         WHERE se.page = 'NextSong' 
                         AND se.userId IS NOT NULL
                         AND se.ts IS NOT NULL
                         AND TO_CHAR(timestamp 'epoch' + se.ts/1000 * interval '1 second', 'YYYY-MM-DD-HH-MI-SS') || se.userId NOT IN (SELECT DISTINCT TO_CHAR(start_time, 'YYYY-MM-DD-HH-MI-SS') || user_id FROM fact_songplays)                         
                         """)

# user can either 1) start on free trial then convert to paid or 2) immediately be paid
# we want to get the last level record in our dataset
# we could get last position of id based on ts and use that record
user_table_insert = ("""
                     INSERT INTO dim_users(id, first_name, last_name, gender, level)        
                     WITH last_songplay_of_user AS (
                     SELECT userId AS user_id, MAX(ts) as max_ts
                     FROM staging_events
                     WHERE page = 'NextSong'
                     GROUP BY user_id
                     )
                     SELECT last_songplay_of_user.user_id AS id,
                            staging_events.firstName AS first_name,
                            staging_events.lastName AS last_name,
                            staging_events.gender,
                            staging_events.level
                     FROM last_songplay_of_user
                     INNER JOIN staging_events
                     ON last_songplay_of_user.max_ts = staging_events.ts  
                     WHERE last_songplay_of_user.user_id NOT IN (SELECT DISTINCT id FROM dim_users)
                     AND last_songplay_of_user.user_id IS NOT NULL
                     """)

song_table_insert = ("""
                     INSERT INTO dim_songs(id, title, artist_id, year, duration)
                     SELECT DISTINCT song_id AS id,
                                     title,
                                     artist_id,
                                     year,
                                     duration
                     FROM staging_songs
                     WHERE staging_songs.song_id NOT IN (SELECT DISTINCT id FROM dim_songs)  
                     AND artist_id IS NOT NULL
                     """)

artist_table_insert = ("""
                       INSERT INTO dim_artists(id, name, location, latitude, longitude)
                       SELECT DISTINCT artist_id AS id,
                                       artist_name AS name,
                                       artist_location AS location,
                                       artist_latitude AS latitude,
                                       artist_longitude AS longitude
                       FROM staging_songs
                       WHERE staging_songs.artist_id NOT IN (SELECT DISTINCT id FROM dim_artists)
                       """)

time_table_insert = ("""
                     INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
                     WITH time_datetimes AS (
                         SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time
                         FROM staging_events
                         WHERE page = 'NextSong'
                         AND ts IS NOT NULL
                         AND start_time NOT IN (SELECT DISTINCT start_time FROM dim_time)
                     )
                     SELECT DISTINCT start_time,
                            EXTRACT(hr from start_time) AS hour,
                            EXTRACT(d from start_time) AS day,
                            EXTRACT(w from start_time) AS week,
                            EXTRACT(mon from start_time) AS month,
                            EXTRACT(yr from start_time) AS year,
                            EXTRACT(dow from start_time) AS weekday
                     FROM time_datetimes
                     """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]

drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]

copy_table_queries = [staging_songs_copy, staging_events_copy]
                      
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
