from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator) 
from helpers import SqlQueries
from load_dim_tables_subdag import load_dimension_tables_dag

AWS_KEY = os.environ.get('AWS_KEY')
AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    'start_date': datetime(2018, 11, 1),
    'retries': 3,
    'catchup': False,
    'depends_on_past': False,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

dag = DAG('udacity_dp_dag',
          default_args=default_args, 
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *'
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id="Stage_events",
    dag=dag,
    redshift_conn_id="redshift",
    target_schema="public",
    target_table="staging_events",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
    json_copy_option="jsonpaths",
    json_pathsfile="s3://udacity-dend/log_json_path.json",
    truncate=True
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id="Stage_songs",
    dag=dag,
    redshift_conn_id="redshift",
    target_schema="public",
    target_table="staging_songs",
    aws_credentials="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    json_copy_option="auto",
    truncate=True
)

load_songplays_table = LoadFactOperator(
    task_id="Load_songplays_fact_table",
    dag=dag,
    redshift_conn_id="redshift",
    target_schema="public",
    target_table="songplays",
    select_sql=SqlQueries.songplay_table_insert,
    truncate=False
)



# create a map for dimension table/select query pairs
map_dimtables_sqlqueries = { 'users':SqlQueries.user_table_insert, 
                        'songs':SqlQueries.song_table_insert,
                        'artists':SqlQueries.artist_table_insert,
                        'time':SqlQueries.time_table_insert
}

# call subdag for dimension table loads
load_dim_tabs_task_id = "load_dimension_tables"
load_dimension_tables_subdag = SubDagOperator(
    subdag=load_dimension_tables_dag(
        parent_dag_name="udacity_dp_dag",
        task_id=load_dim_tabs_task_id,
        redshift_conn_id="redshift",
        schema="public",
        sql_queries=map_dimtables_sqlqueries,
        truncate=True,
        default_args=default_args
    ),
    task_id=load_dim_tabs_task_id,
    dag=dag,
)
        



check_table_column_map = {'artists':'name', 'songplays':'level', 'songs':'title','users':'level', 'time':'weekday'} # define a table/column map to check null values
check_nulls_query = "select count(*) from {schema}.{table} where {column} is null" # query template to check null values
dq_check_input = [
    {'check_feature':'null value','check_query':check_nulls_query,'expected_result':0, 'check_condition':'equal'}
    # , {and further check input dicts}
]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    target_schema="public",
    map_table_column=check_table_column_map,
    check_input = dq_check_input
)



end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)



# build process flow / dependencies
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_dimension_tables_subdag
load_dimension_tables_subdag >> run_quality_checks
run_quality_checks >> end_operator





