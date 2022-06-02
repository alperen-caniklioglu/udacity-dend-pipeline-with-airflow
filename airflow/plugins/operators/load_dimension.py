from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
          

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 target_schema="",
                 target_table="",
                 select_sql="",
                 truncate="",
                 *args, **kwargs):
        """
            Inits LoadDimensionOperator class
            
                Args:
                    redshift_conn_id (str): redshift connection name
                    target_schema (str): schema including target table
                    target_table (str): target dimension table
                    select_sql (str): select query to be used in insert statement
                    truncate (boolean): parameter to switch between append or delete/reload options
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_schema=target_schema
        self.target_table=target_table
        self.select_sql=select_sql
        self.truncate=truncate

    def execute(self, context):
        
        redshift_hook = PostgresHook(self.redshift_conn_id) 
        load_sql = f"insert into {self.target_schema}.{self.target_table} {self.select_sql}" # construct insert query using input arguments
        self.log.info("displaying insert query:" + load_sql)
        
        
        if self.truncate:
            redshift_hook.run(f"TRUNCATE TABLE {self.target_schema}.{self.target_table}") # execute truncate query
            self.log.info(f"{self.target_schema}.{self.target_table} truncated.")
            
        try:
            self.log.info(f"{self.target_schema}.{self.target_table} is being populated...")
            redshift_hook.run(load_sql) # execute insert query
            self.log.info(f"{load_sql} executed.")
        except Exception as e:
            print(e)
            
        
            
        
        
