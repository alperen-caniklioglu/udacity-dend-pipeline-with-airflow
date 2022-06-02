from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 target_schema="",
                 map_table_column="",
                 check_input="",
                 *args, **kwargs):
        """
            Inits DataQualityOperator class
            
                Args:
                    redshift_conn_id (str): redshift connection name
                    target_schema (str): schema including target table
                    map_table_column (dict): map including table/column pairs to check null values
                    check_input (list): list including dictionary consisting of check feature, query, expected result and check condition/operator
        """

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.target_schema = target_schema
        self.map_table_column = map_table_column
        self.check_input = check_input
        
        
        
       
    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id) # connect to database
        for tab, col in self.map_table_column.items(): # iterate over table and column pairs defined in given map
            self.log.info(f"check for target table: {tab}")
            self.log.info(f"column to be checked: {col}")
            for check_in in self.check_input:
                query_template = check_in.get('check_query')
                query = query_template.format(schema=self.target_schema, table=tab, column=col)
                expected_result = check_in.get('expected_result')
                check_condition = check_in.get('check_condition')
                check_feature = check_in.get('check_feature')
                self.log.info(f"feature to be checked: {check_feature}")
                self.log.info(f"query to be executed: {query}")
                self.log.info(f"expected result: {expected_result}")
                self.log.info(f"check condition: {check_condition}")
                records = redshift_hook.get_records(query)
                check_result = records[0][0]
                self.log.info(f"check result for feature {check_feature}:{check_result}")
                
                # evaluate results
                check_pass = False # default value for verdict
                
                if check_condition == 'equal':
                    if check_result == expected_result:
                        check_pass = True
                        
                elif check_condition == 'greater':
                    if check_result > expected_result:
                        check_pass = True
                    
                elif check_condition == 'less':
                    if check_result < expected_result:
                        check_pass = True 
                else:
                    raise ValueError("Unrecognized check condition!")
                    
                if check_pass:
                    self.log.info(f"Quality check for {tab}: pass.")
                else:
                    raise ValueError(f"Quality check for {tab}:fail. {tab} has {check_result} records for feature {check_feature} in column {col}.")
                    
                    
                
                        
                        
                    
                        
 
                
                    
                    
        
        
        
            