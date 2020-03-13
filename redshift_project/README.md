Project completed by Dan Friedman in January 2020.

### Summary of Project

This goal of this project is to perform ELT on logs for the music streaming app, Sparkify, so we can organize user activity data into a cloud data warehouse and make it easy to later perform data analysis. 

Logs are in an S3 bucket. I utilize SQL on a separate compute instance to copy the data from S3 into two staging tables on an AWS Redshift data warehouse. From there, I clean and organize the data using SQL into fact and dimension tables to separate out dimensions for unique users, songs, artists, time (songs were played) and a fact table that includes records of songplays.

### How to Run this ETL Process And Explanation of Each Step

Note: I took the original file provided of `etl.py` and separated it into two scripts.


1. Ensure a Redshift cluster is created and proper variables are noted in `dwh.cfg`
2. `python create_tables.py`: runs SQL queries to create the tables to record data on staging songplay dump from logs, staging songs dump from logs and the tables used for the star schema creation which are `dim_songs`, `dim_artists`, `dim_time`, `dim_users` and `fact_songplays`.
3. `python extract_and_load.py`: runs SQL queries to copy data from s3 logs into the `staging_songs` and `staging_events` table. 
4. `python transform.py` runs SQL queries to transform and insert data from staging tables in Redshift to five star schema tables. 

### Debugging

- If a script stalls, I recommend running `ps aux` command to see list of running processes. I would run a `kill -9 {PID}` to kill running of any scripts that are stuck and restart from step 1 above.