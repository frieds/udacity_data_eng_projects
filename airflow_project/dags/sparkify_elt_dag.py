from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, DataQualityOperator, LoadFactOperator, LoadDimensionOperator)
from helpers.create_tables_sql import CreateAllTablesSqlQueries
from helpers.insert_sql_queries import SqlQueries


AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'dan_friedman',
    'start_date': datetime(2018, 11, 1),    
    # allows task to run even if previous task of run failed
    'depends_on_past': False,
    # upon task failure, up to 3 retries to get it to succeed
    'retries': 3,
    # retries happen every 5 minutes
    'retry_delay': timedelta(minutes=5),
    # do not email on retry
    'email_on_retry': False,    
    # our dag specifies its own catchup through past start date and schedule interval; dag runs will be sequential
    'catchup': False,
    # ensures task marked as failed if it doesn't complete in specified timeframe
    'execution_timeout': timedelta(minutes=10),
    # end date to stop running dag; our logs only have a few days worth of activity so this could even be farther in the past
    'end_date': datetime(2018, 11, 2)
}

create_tables_dag = DAG('create_tables_dag',
                        default_args = default_args,
                        description = 'Create tables for staging sparkify data in redshift and star schema',
                        schedule_interval = '@once'
                        )

elt_dag = DAG('sparkify_elt_dag',
              default_args = default_args,
              description = 'Load sparkify logs from s3 into staging tables in redshift, do transformations to \
                  load data from staging tables into new star schema and do data quality check',
              schedule_interval = '@hourly'
             )

# this dummy operator literally does nothing; it should complete successfully immediately every time
start_operator = DummyOperator(
    task_id = 'Begin_execution', 
    dag = elt_dag)

# Create tasks to create tables in Redshift
create_redshift_staging_events_table = PostgresOperator(
    task_id = "create_redshift_staging_events_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_staging_events_table
)

create_redshift_staging_songs_table = PostgresOperator(
    task_id = "create_redshift_staging_songs_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_staging_songs_table
)

create_redshift_songplays_table = PostgresOperator(
    task_id = "create_redshift_songplays_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_songplays_table
)

create_redshift_users_table = PostgresOperator(
    task_id = "create_redshift_users_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_users_tables
)

create_redshift_artists_table = PostgresOperator(
    task_id = "create_redshift_artists_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_artists_table
)

create_redshift_songs_table = PostgresOperator(
    task_id = "create_redshift_songs_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_songs_table
)

create_redshift_time_table = PostgresOperator(
    task_id = "create_redshift_time_table",
    dag = create_tables_dag,
    postgres_conn_id = "redshift",
    sql = CreateAllTablesSqlQueries.create_time_table
)

# Create tasks to load data from S3 to redshift staging tables
stage_events_to_redshift = StageToRedshiftOperator(
    # first 3 arguments are from BaseOperator arguments
    task_id = 'copy_data_from_s3_logs_data_prefix_to_staging_events_table',
    dag = elt_dag,
    provide_context = True,
    redshift_table = 'staging_events',
    s3_bucket = 'udacity-dend',
    s3_data_category_prefix = 'log_data/',
    execution_date = None
)

stage_songs_to_redshift = StageToRedshiftOperator(
    # first 3 arguments are from BaseOperator arguments
    task_id = 'copy_data_from_s3_song_data_prefix_to_staging_songs_table',
    dag = elt_dag,
    provide_context = True,
    redshift_table = 'staging_songs',    
    s3_bucket = 'udacity-dend',
    s3_data_category_prefix = 'song_data/',
    execution_date = None
)

# Transfer data from staging tables to star schema tables
load_songplays_fact_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag = elt_dag,
    sql_query = SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = 'Load_user_dim_table',
    dag = elt_dag,
    sql_query = SqlQueries.user_table_insert,
    append_data_boolean = False,
    table = "public.dim_users"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = 'Load_song_dim_table',
    dag = elt_dag,
    sql_query = SqlQueries.song_table_insert,
    append_data_boolean = False,
    table = "public.dim_songs"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = 'Load_artist_dim_table',
    dag = elt_dag, 
    sql_query = SqlQueries.artist_table_insert,
    append_data_boolean = False,
    table = "public.dim_artists"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = 'Load_time_dim_table',
    dag = elt_dag,
    sql_query = SqlQueries.time_table_insert,
    append_data_boolean = False,
    table = "public.dim_time"
)

# Run data quality check
run_quality_checks = DataQualityOperator(
    task_id = 'data_quality_check_rows_exist',
    dag = elt_dag,
    tables = ['public.fact_songplays', 'public.dim_songs', 'public.dim_artists', 'public.dim_users', 'public.dim_time']
)

end_operator = DummyOperator(
    task_id = 'Stop_execution',
    dag = elt_dag)

# Define task dependancies
start_operator >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_fact_table >> \
            [load_artist_dimension_table, load_song_dimension_table, load_time_dimension_table,\
                 load_user_dimension_table] >> run_quality_checks >> end_operator

"""there are no dependancies for the create table statements. No need to define dependancies statements. 
They can all run concurrently and will automatically."""