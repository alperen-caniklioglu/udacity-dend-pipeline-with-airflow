import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import LoadDimensionOperator
                              
import sql

def load_dimension_tables_dag(
    parent_dag_name,
    task_id,
    redshift_conn_id,
    schema,
    sql_queries,
    truncate,
    default_args,
    *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        default_args=default_args,
        **kwargs
    )
   
    
    
    for table, sql_query in sql_queries.items():
        load_sql_query=f"INSERT INTO {schema}.{table} {sql_query}"
        load_dim_tab_task = LoadDimensionOperator(
        dag=dag,
        task_id=f"Load_{table}_dim_table",
        redshift_conn_id=redshift_conn_id,
        target_schema=schema,
        target_table=table,
        select_sql=sql_query,
        truncate=truncate
        )
        
        
    return dag
	
