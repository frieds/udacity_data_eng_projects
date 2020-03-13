Project by Dan Friedman.

### Project Summary

This project includes Python code to:

1) Download log files from AWS S3 on Sparkify's collection of data for songplay listens and song details
2) Run transformations on this data using Apache Spark - specifically the Python wrapper PySpark - to PySpark dataframes that would represent all the data for a dimension or fact table as part of a star schema logic
3) Upload PySpark dataframes - that represent a dim or fact table - to AWS S3 under a `transformed/` prefix.

### How to Execute Project Code

1. Ensure the relevant files of `dl.cfg` (for your user credentials) and `etl.py` are in your directory.
2. Open up a shell
3. Run `python etl.py`

### Explanation of Files

#### `etl.py`

This script reads in raw JSON files in S3 related to songplay listen records and song/artist records to a local machine using PySpark. Then, transformations are made on this data to create 5 different PySpark dataframes that each resemble a table of a star schema. Transformations filter data by columns, records, unique records - especially if a primary key needs to exist in a column - and changes data types. There's a fact table for songplay records and dimension tables for users, time, artists and songs. Then, each of these PySpark dataframes are compressed into Parquet format and uploaded back to S3 into a new bucket - me as the owner - under several conditions. Each compressed parquet file is uploaded to a specific prefix for its data category such as `transformed/artists` for artist records, `transformed/songs` for song records and so forth. Also, some of the data is partitioned in S3 with a prefix for the type of data category and data can be partitioned by certain column names. For example, the songs records are partitioned by `songs/{year}/{artists}`. This would help us later easily retrieve data for a specific prefix such as songs released by a specific year and/or artist rath than having to query/traverse *all* song related records to find something specific related to a year or artist.