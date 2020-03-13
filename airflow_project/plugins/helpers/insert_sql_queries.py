class SqlQueries:
    # Redshift doesn't enforce primary keys so I add a check in last line of query to ensure only add new unique records by timestamp-user_id value
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
    # we want to get the last level record in our dataset to know user's current or paid state
    # we could get last position of id based on ts and use that record
    # we also verify we're only uploading new users by user_id
    # however, this SQL isn't perfect. If we did ongoing loads, if we found a user_id that already exists, we'd want to update the level field and keep that user_id record
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

    # only insert song records if id doesn't currently exist in Redshift cluster
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
            AND id IS NOT NULL
    """)

    # only insert artist records if id doesn't exist in table
    artist_table_insert = (""" 
        INSERT INTO dim_artists(id, name, location, latitude, longitude)
            SELECT DISTINCT artist_id AS id,
                            artist_name AS name,
                            artist_location AS location,
                            artist_latitude AS latitude,
                            artist_longitude AS longitude
            FROM staging_songs
            WHERE staging_songs.artist_id NOT IN (SELECT DISTINCT id FROM dim_artists)
            AND id IS NOT NULL
    """)

    # only insert time records if start_time doesn't exist in table
    time_table_insert = (""" 
        INSERT INTO dim_time(start_time, hour, day, week, month, year, weekday)
            WITH time_datetimes AS (
                SELECT timestamp 'epoch' + ts/1000 * interval '1 second' AS start_time
                FROM staging_events
                WHERE page = 'NextSong'
                AND ts IS NOT NULL
                AND start_time NOT IN (SELECT DISTINCT start_time FROM dim_time)
                AND start_time IS NOT NULL
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