from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    sql_query = "SELECT COUNT(*) FROM {}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 tables = [],
                 min_records = 1,                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables
        self.min_records = min_records

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("table needs {self.min_records} to pass our data quality check")
        for table in self.tables:
            full_query = DataQualityOperator.sql_query.format(table)
            self.log.info(f"run SQL query to check count of rows in table: {table}")
            self.log.info(f"SQL query: {full_query}")
            # Airflow PostgresHook documentation: https://github.com/apache/airflow/blob/master/airflow/hooks/dbapi_hook.py
            sql_result = redshift.get_records(full_query)
            records_count_rows = sql_result[0][0]
            self.log.info(f"records_count_rows: {records_count_rows}")
            self.log.info(f"type(records_count_rows): {type(records_count_rows)}")
            if records_count_rows < self.min_records:
                raise ValueError(f"table failed data check because it didn't meet minimum required rows!")
            else:
                self.log.info(f"Data quality on table passed!")