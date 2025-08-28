from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

def test_airflow():
    print("Airflow environment is working! This is a test task.")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    'test_airflow_env',
    default_args=default_args,
    description='A simple DAG to test the Airflow environment',
    schedule_interval=None,
    catchup=False,
    tags=['test', 'airflow'],
) as dag:

    test_task = PythonOperator(
        task_id='test_airflow_task',
        python_callable=test_airflow,
    )