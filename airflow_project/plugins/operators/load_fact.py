from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id= 'redshift',
                 sql_query = "",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info(f"instantiates postgres hook for programmatic access to redshift cluster...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self.log.info("run SQL query to insert data into songplays table")
        self.log.info(f"SQL query: {self.sql_query}")
        redshift.run(self.sql_query)