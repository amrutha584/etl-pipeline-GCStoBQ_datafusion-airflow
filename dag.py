from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.google.cloud.operators.datafusion import CloudDataFusionStartPipelineOperator

default_args = {
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('employee_data',
          default_args=default_args,
          description='Runs an external Python script',
          schedule_interval='@daily',
          catchup=False)

with dag:
    run_script_task = BashOperator(
        task_id='extract_data',
        bash_command='python /home/airflow/gcs/dags/scripts/extract.py',
    )

    
    start_pipeline = CloudDataFusionStartPipelineOperator(
    location="africa-south1",
    pipeline_name="gcpdatapipeline",
    instance_name="gcpdataproject1",
    task_id="start_datafusion_pipeline",
    pipeline_timeout=1000,  # increase timeout
    )

    run_script_task >> start_pipeline
