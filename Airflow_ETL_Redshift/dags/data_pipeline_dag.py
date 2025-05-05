from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.empty import EmptyOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.postgres_operator import PostgresOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator


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

create_staging_events_table = PostgresOperator(
    task_id='create_staging_events_table',
    dag=dag,
    postgres_conn_id='redshift',
    sql=sql_statements.CREATE_staging_events_TABLE_SQL
)

