from airflow.models import BaseOperator, Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(
        self,
        table_name,
        s3_url,
        s3_region='us-west-2',
        s3_json_schema='auto',
        redshift_conn_id='redshift',
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.table_name = table_name
        self.s3_url = s3_url
        self.s3_region = s3_region
        self.s3_json_schema = s3_json_schema

        self.redshift_conn_id = redshift_conn_id
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
        self.redshift_iam_arn = Variable.get('redshift_arn')

    def execute(self, context):
        self.log.info(f"Truncating table: {self.table_name}")
        self.redshift.run(f"""TRUNCATE TABLE {self.table_name}""")
        self.log.info("Truncation complete. ")

        copy_statement = f"""
            COPY {self.table_name}
            FROM '{self.s3_url}'
            REGION '{self.s3_region}'
            IAM_ROLE '{self.redshift_iam_arn}'
            JSON '{self.s3_json_schema}'
        """
        # Not advisable in production without encrypting secrets
        self.redshift.run(copy_statement)
        self.log.info("Table copy complete. ")
