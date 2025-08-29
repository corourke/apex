from airflow import DAG
from airflow.utils.dates import days_ago
from airflow_providers_onehouse.operators.jobs import OnehouseRunJobOperator
from airflow_providers_onehouse.sensors.onehouse import OnehouseJobRunSensor
from datetime import datetime
from pytz import timezone

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'run_inv_delta_writer',
    default_args=default_args,
    description='Run the Inventory Delta Writer Job',
    schedule_interval='0 4 * * *',
    start_date=datetime(2025, 8, 29, tzinfo=timezone('America/Los_Angeles')),
    catchup=False,
    tags=['onehouse', 'spark'],
) as dag:

    job_name = "InventoryDeltaWriter"

    run_job = OnehouseRunJobOperator(
        task_id='run_inv_delta_writer',
        job_name=job_name,
        conn_id='Onehouse_API',
    )

    monitor_job = OnehouseJobRunSensor(
        task_id='monitor_inv_delta_writer',
        job_name=job_name,
        job_run_id="{{ ti.xcom_pull(task_ids='run_inv_delta_writer') }}",
        conn_id='Onehouse_API',
        poke_interval=60,  # Check every 60 seconds
        timeout=3600,  # Fail after 1 hour
    )

    run_job >> monitor_job