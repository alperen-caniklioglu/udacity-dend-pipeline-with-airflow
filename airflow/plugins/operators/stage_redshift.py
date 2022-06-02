from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
## check following modification
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    template_fields=("s3_key",)
    ui_color = '#358140'
    copy_sql_template = """
    COPY {target_schema}.{target_table}
    FROM '{s3_path}'
    ACCESS_KEY_ID '{access_key_id}'
    SECRET_ACCESS_KEY '{secret_access_key}'
    TIMEFORMAT as 'epochmillisecs'
    FORMAT AS JSON '{json_option}' 
    """
    
    truncate_sql = "TRUNCATE TABLE {table}"

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_schema="",
                 target_table="",
                 aws_credentials="",
                 s3_bucket="",
                 s3_key="",
                 json_copy_option="",
                 json_pathsfile="",
                 truncate="",
                 *args, **kwargs):
        """
            Inits StageToRedshiftOperator class
            
                Args:
                    redshift_conn_id (str): redshift connection name
                    target_schema (str): schema including target table
                    target_table (str): target dimension table
                    aws_credentials (str): aws credentials name
                    s3_bucket (str): s3 bucket name where source data reside
                    s3_key (str): template field; describes s3 access path and can be constructed dynamically
                    json_copy_option (str): copy option for json files (auto or jsonpaths)
                    json_pathsfile (str): json pathsfile to parse source json data. if json_copy_option is jsonpaths, this paramater cannot be None.
                    truncate (boolean): parameter to switch between append or delete/reload options
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_schema=target_schema
        self.target_table = target_table
        self.aws_credentials = aws_credentials
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_copy_option = json_copy_option
        self.json_pathsfile = json_pathsfile
        self.truncate=truncate

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        aws_hook = AwsHook(self.aws_credentials)
        credentials = aws_hook.get_credentials()
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{s3_bucket_param}/{rendered_key_param}".format(s3_bucket_param=self.s3_bucket, rendered_key_param=rendered_key) # construct s3 path
        
        self.log.info(f"processing file: {s3_path}")
        
        # raise error in case of missing json_pathsfile as json_copy_option == "jsonpaths"
        if self.json_copy_option == "jsonpaths":
            if self.json_pathsfile is None:
                raise Exception("Error: illegal configuration. json_pathsfile cannot be None as long as json_copy_option is set to jsonpaths")
            json_option = self.json_pathsfile
        else:
            json_option = self.json_copy_option
        
        # run truncate depending on parameter value
        if self.truncate:
            try:
                redshift_hook.run(f"TRUNCATE TABLE {self.target_schema}.{self.target_table}")
                self.log.info(f"{self.target_schema}.{self.target_table} truncated.")
            except Exception as e:
                print(e)
        
        # format copy_sql_template
        copy_sql = StageToRedshiftOperator.copy_sql_template.format(
            target_schema=self.target_schema,
            target_table=self.target_table,
            s3_path=s3_path,
            access_key_id=credentials.access_key,
            secret_access_key=credentials.secret_key,
            json_option=json_option
        )
        
        try:
            redshift_hook.run(copy_sql)
            self.log.info(f"data successfully staged in {self.target_schema}.{self.target_table}.")
            check_sql = f"select count(*) from {self.target_schema}.{self.target_table}"
            records = redshift_hook.get_records(check_sql)
            self.log.info(f"{self.target_schema}.{self.target_table} contains {records[0][0]} records in total")
        except Exception as e:
            print(e)
            return None
        
            





