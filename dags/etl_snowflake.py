from datetime import datetime, timedelta
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from astronomer.providers.core.sensors.external_task import ExternalTaskSensorAsync
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator

default_args = {
    'owner': 'Cosmic Energy DE',
    'email': ['noreply@astronomer.io'],
    'email_on_failure': False,
    'sla': timedelta(minutes=30) ## applicable to only scheduled tasks ; relative to DAG Execution Date not Task Start Time
}

with DAG('etl_snowflake'
        , start_date=datetime(2022,8,18)
        , catchup=False
        , max_active_runs=1
        , schedule_interval='@daily'
        , default_args=default_args
        , tags = ['transform', 'daily', 'snowflake', 'lineage', 'sql'],
    ) as dag:

    task_check_ingestion_status=ExternalTaskSensorAsync(
        task_id='check_ingestion_staus',
        external_dag_id='ingest_dynamic_task_mapping',
        external_task_id='dbt_model_run'
    )

    task_load_snowflake_file=SnowflakeOperator(
        task_id='load_snowflake_sql_from_file_weather',
        sql='sql/etl_weather.sql',
        snowflake_conn_id='snowflake',
    )

    for file in ['countries', 'states']:
        task_load_snowflake_param=SnowflakeOperator(
            task_id=f'load_snowflake_sql_param_{file}',
            sql=f'''
                create or replace table aws_cosmic_energy.{file}
                as 
                select *
                from aws_raw_cosmic_energy.{file}
            ''',
            snowflake_conn_id='snowflake',
        )

        task_check_ingestion_status >> task_load_snowflake_file >> task_load_snowflake_param


     
