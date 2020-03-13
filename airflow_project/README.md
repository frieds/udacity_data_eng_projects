### Overview of Project
This project is intended to perform the ELT process of loading raw data logs from S3 on Sparkify's user activity into tables on Redshift to "stage" the data, then make transformations to create a star schema of tables in Redshift and then perform data quality checks to see if rows exist in the tables.

### Description of Files

#### sparkify_elt_dag.py
The dag file declares the high-level steps needed to run in the ELT process. The dag objects has parameters on when it should run, how frequently, and more. Operators define tasks to be run in our workflow. We specify tasks for creating Redshift tables, transforming data in Redshift tables and executing SQL queries in Redshift for data quality checks. At the bottom I defined the task dependancies - the order in which tasks will be executed once the dag it relates to is started. 

#### plugins/helpers/create_tables_sql.py
SQL queries that create all the tables for staging the data and star schema tables in Redshift.

#### plugins/helpers/insert_tables_sql.py
SQL queries to insert data from staging tables into star schema fact and dimensional tables.

#### plugins/operators/data_quality.py
A custom operator that validates if a count of records specified exists in a fact or dimensional table.

#### plugins/operators/load_dimension.py
A custom operator to load data from the Redshift staging tables into dimensional tables.

#### plugins/operators/load_fact.py
A custom operator to load data from the staging Redshift tables into a fact table.

#### plugins/operators/stage_redshift.py
A custom operator to load data from Sparkify s3 logs into Redshift staging tables.

### How to Run ELT 

1. Start an Airflow webserver 
2. Verify DAG is running in Airflow UI; if not, toggle DAG on

### Caveats with the code

In an ideal world, new s3 logs would be arriving in a bucket frequently - perhaps every few minutes or hour - and they'd be partitioned by `year/month` prefix in S3. This is helpful so I could create a dag that runs at an hourly frequency and only analyzes new data in s3. However, with the prefixes provided for song and event logs, that partition doesn't always exist. Therefore, for the simplicity of this project, I run the dag hourly as specified and delete the data in the staging tables beforehand so I don't keep on adding duplicative data in the staging tables. There's no primary key in the staging tables or unique identifier for records. 