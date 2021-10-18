from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(
        self,
        table_names,
        redshift_conn_id='redshift',
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)

        self.table_names = table_names

        self.redshift_conn_id = redshift_conn_id
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

    def test_not_null(self):
        for table in self.table_names:
            self.log.info(f"Ensuring table '{table}' not null...")
            records = self.redshift.get_first(f"""
                SELECT COUNT(*) > 0
                FROM {table}
            """)
            assert records[0] == True

    def test_no_duplicates(self):
        for table in self.table_names:
            distinct = self.redshift.get_first(f"""
                SELECT COUNT(*)
                FROM (
                    SELECT DISTINCT *
                    FROM {table}
                )
            """)
            all = self.redshift.get_first(f"""
                SELECT COUNT(*)
                FROM {table}
            """)
            self.log.info(
                f"Table '{table}' has {distinct[0]} distinct rows and"
                + f"{all[0]} total rows. "
            )
            assert distinct[0] == all[0]

    def execute(self, context):
        self.test_not_null()
        self.test_no_duplicates()
