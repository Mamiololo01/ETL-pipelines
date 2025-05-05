from datetime import datetime, timedelta
import os
from airflow import DAG # type: ignore
from airflow.operators.empty import EmptyOperator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# from airflow.legacy.providers.postgres.operators.postgres import PostgresOperator
# from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.amazon.aws.operators.redshift_data import RedshiftDataOperator
from airflow.operators import LoadFactOperator

import sql_scripts

default_args = {
    'owner': 'mamiololo',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 26),
    'email': ['jaycees20@yahoo.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'catchup': False,
}

dag = DAG('data_pipeline_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule='0 0 * * *'
)

start_operator = EmptyOperator(task_id='Begin_execution', dag=dag)

create_staging_events_table = SQLExecuteQueryOperator(
    task_id='create_staging_events_table',
    dag=dag,
    conn_id='redshift',
    sql=sql_scripts.CREATE_staging_events_TABLE_SQL
)

stage_events_to_redshift = RedshiftDataOperator(
    task_id='Stage_events_from_s3_to_redshift',
    dag=dag,
    database='dev',
    sql=f"""
        COPY staging_events
        FROM 's3://mamiololo-storage/log_data' 
        CREDENTIALS 'aws_iam_role=arn:aws:iam::YOUR_ACCOUNT_ID:role/ROLE_NAME' 
        JSON 's3://mamiololo-storage/log_json_path.json' 
    """,
    aws_conn_id='aws_credentials',
    region_name='us-west-2'
)

create_staging_songs_table = SQLExecuteQueryOperator(
    task_id="create_staging_songs_table",
    dag=dag,
    conn_id="redshift",
    sql=sql_scripts.CREATE_staging_songs_TABLE_SQL
)

stage_songs_to_redshift = RedshiftDataOperator(
    task_id='Stage_songs_from_s3_to_redshift',
    dag=dag,
    database='dev',
    sql=f"""
        COPY staging_events
        FROM 's3://mamiololo-storage/log_data' 
        CREDENTIALS 'aws_iam_role=arn:aws:iam::YOUR_ACCOUNT_ID:role/ROLE_NAME' 
        JSON 's3://mamiololo-storage/log_json_path.json' 
    """,
    aws_conn_id='aws_credentials',
    region_name='us-west-2'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    conn_id="redshift"   
)