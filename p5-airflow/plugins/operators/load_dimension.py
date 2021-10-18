from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'

    def __init__(
        self,
        table_name,
        sql,
        append=False,
        redshift_conn_id='redshift',
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        
        self.table_name = table_name
        self.sql = sql
        self.append = append

        self.redshift_conn_id = redshift_conn_id
        self.redshift = PostgresHook(postgres_conn_id=redshift_conn_id)
        
    def execute(self, context):
        if not self.append:
            self.log.info("Loading dimension with option 'append = True'")
            self.redshift.run(f"""
                TRUNCATE TABLE {self.table_name}
            """)

        self.redshift.run(f"""
            INSERT INTO {self.table_name}
            {self.sql}
        """)
