from airflow import DAG
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
import json

url = "v1/resource/"

onehouse_project_uid = '81739d6b-cd92-4766-bbd5-481592321d2d'
onehouse_api_key = 'Zb++5r+RZO+6bvVicrQduQ=='
onehouse_api_secret = 'Yh9oFQRq055gJJaK1CfDIXlw7lWQUNRoiSuRX1TAPOw='
onehouse_link_uid = '7a00bf9c-c2f5-57c0-a620-9d739e4ba4a7'
onehouse_region = 'us-west-2'
onehouse_uuid = 'Rg3NcRpbwKa9c1kqoCLK7kBkMn82'

headers = {
  'x-onehouse-project-uid': onehouse_project_uid,
  'x-onehouse-api-key': onehouse_api_key,
  'x-onehouse-api-secret': onehouse_api_secret,
  'x-onehouse-link-uid': onehouse_link_uid,
  'x-onehouse-region': onehouse_region,
  'x-onehouse-uuid': onehouse_uuid
}
cluster_name = "Onehouse Spark Demo Cluster"
job_name = "InventoryDeltaWriter"


onehouse_sql = f"""RUN JOB {job_name}"""

payload = json.dumps({
    "statement": onehouse_sql
})

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

with DAG(
    'run_inv_delta_writer',
    default_args=default_args,
    description='Run the Inventory Delta Writer Job',
    schedule_interval=None,
) as dag:

    run_job_request = SimpleHttpOperator(
        task_id='inv_delta_writer',
        http_conn_id='Onehouse_API', # This is the name of the HTTP connection you created in Airflow
        endpoint=url,
        method='POST',
        data=payload,  # JSON payload
        headers=headers,
        log_response=True,
    )

    run_job_request