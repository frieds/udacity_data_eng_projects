from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    # https://airflow.readthedocs.io/en/stable/howto/custom-operator.html#templating
    # field names present in template_fields used for templating while rendering the operator
    # need this to pass context macros from dag to task that's running based off this operator
    template_fields = ["s3_prefix"]

    # we specify json 'auto' below b/c column names in json file directly match column names in output target table
    copy_sql = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key_id}'
        SECRET_ACCESS_KEY '{secret_access_key}'
        json 'auto'
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = 'redshift',
                 aws_credentials_id = 'aws_credentials',
                 redshift_table = "",
                 s3_bucket = "",
                 # s3_key can be a prefix for path in s3 bucket
                 s3_data_category_prefix = "",
                 execution_date = "",
                 *args, **kwargs):

        # super function allows us to utilize methods from BaseOperator
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.redshift_table = redshift_table
        self.s3_bucket = s3_bucket
        self.s3_data_category_prefix = s3_data_category_prefix
        self.execution_date = kwargs.get('execution_date')

    def execute(self, context):
        self.log.info(f"instantiates postgres hook for programmatic access to redshift cluster...")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        # in this simplified example, we only load data into Redshift staging tables once
        self.log.info(f"Delete data from Redshift destination table of {self.redshift_table} with query...")
        redshift.run("DELETE FROM {}".format(self.redshift_table))
        self.log.info("table deleted")

        self.log.info("instantiate boto3 s3 client object through Airflow S3Hook wrapper...")
        # verify argument is whether or not to verify SSL certificates
        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/core/session.html#boto3.session.Session.client
        # creates variable to utilize aws hook methods given my user IAM credentials
        # https://airflow.readthedocs.io/en/stable/_api/airflow/contrib/hooks/aws_hook/index.html#airflow.contrib.hooks.aws_hook.AwsHook.get_credentials
        self.log.info("it's instantiated!")
        self.log.info("gets boto3 relevant credentials through S3Hook for access_key and secret_key...")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()

        self.log.info("s3 path with bucket and data category prefix")
        s3_path = f"s3://{self.s3_bucket}/{self.s3_data_category_prefix}"
        self.log.info(f"s3_path: {s3_path}")

        # self.execution_date could be a datetime object like datetime(2018, 2, 2) meaning Feb 2, 2018
        # or self.execution_date should be None or ""
        if self.execution_date is not None or self.execution_date == "":
            # Backfill a specific date
            year = self.execution_date.strftime("%Y")
            month = self.execution_date.strftime("%m")
            day = self.execution_date.strftime("%d")
            # TODO ensure the right number of backslashes here
            s3_path = s3_path + str(year) + "/" + str(month) + "/" + str(day) + "/"
            self.log.info(f"s3_path if execution date exists: {s3_path}")

        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table = self.redshift_table,
            s3_path = s3_path,
            access_key_id = credentials.access_key,
            secret_access_key = credentials.secret_key
        )
        self.log.info(f'formatted_sql command to execute: \n{formatted_sql}')
        self.log.info('Executing COPY command above...')
        redshift.run(formatted_sql)
        self.log.info("COPY command complete!")