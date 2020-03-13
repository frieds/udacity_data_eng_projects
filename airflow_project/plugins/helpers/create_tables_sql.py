class CreateAllTablesSqlQueries:
	create_staging_events_table = (""" 
		CREATE TABLE IF NOT EXISTS public.staging_events (
			artist varchar(256),
			auth varchar(256),
			firstName varchar(256),
			gender varchar(256),
			itemInSession int4,
			lastName varchar(256),
			length numeric(18,0),
			level varchar(256),
			location varchar(256),
			method varchar(256),
			page varchar(256),
			registration numeric(18,0),
			sessionId int4,
			song varchar(256),
			status int4,
			ts int8,
			userAgent varchar(256),
			userId int4
		);
	""")

	create_staging_songs_table = (""" 
		CREATE TABLE IF NOT EXISTS public.staging_songs (
			num_songs int4,
			artist_id varchar(256),
			artist_name varchar(256),
			artist_latitude numeric(18,0),
			artist_longitude numeric(18,0),
			artist_location varchar(256),
			song_id varchar(256),
			title varchar(256),
			duration numeric(18,0),
			year int4
		);
	""")

	create_songplays_table = (""" 
		CREATE TABLE IF NOT EXISTS public.fact_songplays (
			id INTEGER IDENTITY(1,1) PRIMARY KEY,
			start_time timestamp NOT NULL,
			user_id int4 NOT NULL,
			level varchar(256),
			song_id varchar(256),
			artist_id varchar(256),
			session_id int4,
			location varchar(256),
			user_agent varchar(256)
		);
	""")

	create_artists_table = (""" 
		CREATE TABLE IF NOT EXISTS public.dim_artists (
			id varchar(256) PRIMARY KEY,
			name varchar(256),
			location varchar(256),
			latitude numeric(18,0),
			longitude numeric(18,0)
		);
	""")

	create_songs_table = (""" 
		CREATE TABLE IF NOT EXISTS public.dim_songs (
			id varchar(256) PRIMARY KEY,
			title varchar(256),
			artist_id varchar(256),
			year int4,
			duration numeric(18,0)
		);
	""")

	create_time_table = ("""
	CREATE TABLE IF NOT EXISTS public.dim_time (
		start_time timestamp PRIMARY KEY,
		hour int4,
		day int4,
		week int4,
		month varchar(256),
		year int4,
		weekday varchar(256)
	);
	""")

	create_users_tables = (""" 
	CREATE TABLE IF NOT EXISTS public.dim_users (
		id int4 PRIMARY KEY,
		first_name varchar(256),
		last_name varchar(256),
		gender varchar(256),
		level varchar(256)
	);
	""")