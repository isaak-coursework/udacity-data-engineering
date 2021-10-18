from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    def __init__(
        self,
        table_name,
        sql,
        redshift_conn_id='redshift',
        *args, 
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.table_name = table_name
        self.sql = sql

        self.redshift_conn_id = redshift_conn_id
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)

    def execute(self, context):
        self.redshift.run(f"""
            INSERT INTO {self.table_name}
            {self.sql}
        """)
