from airflow.hooks.postgres_hook import PostgresHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 append_data_boolean = "",
                 table = "",
                 sql_query = "",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.append_data_boolean = append_data_boolean,
        self.table = table
        self.sql_query = sql_query

    def execute(self, context):
        self.log.info(f"instantiates postgres hook for programmatic access to redshift cluster...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # if we don't want to append data, delete table and then insert all data
        if self.append_data_boolean is not True:
            delete_sql_query = f'DELETE FROM {self.table}' 
            self.log.info("delete_sql_query: {delete_sql_query}")
            self.log.info(f"run DELETE FROM sql query...")
            redshift.run(delete_sql_query)
            self.log.info("delete SQL query done!")
        self.log.info(f"run INSERT INTO sql query...")
        redshift.run(self.sql_query)
        self.log.info(f"INSERT INTO sql query executed!")