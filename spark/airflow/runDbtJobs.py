from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.dbt.cloud.operators.dbt import DbtCloudRunJobOperator
from datetime import datetime
from pytz import timezone

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    dag_id='dbt_cloud_job',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=datetime(2025, 8, 29, tzinfo=timezone('America/Los_Angeles')),
    catchup=False,
) as dag:
    begin = EmptyOperator(task_id='begin')
    end = EmptyOperator(task_id='end')

    # Run a dbt Cloud job
    run_dbt_job = DbtCloudRunJobOperator(
        task_id='run_dbt_job',
        dbt_cloud_conn_id='Onehouse_dbt',
        account_id=245630,  # Replace with your dbt Cloud account ID
        job_id=866688,      # Replace with your dbt Cloud job ID
        wait_for_termination=True,  # Wait for the job to complete
        timeout=3600,  # Timeout in seconds, one hour
    )

    # Define task dependencies
    begin >> run_dbt_job >> end