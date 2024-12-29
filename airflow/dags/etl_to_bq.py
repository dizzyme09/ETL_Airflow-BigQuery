import os

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

AIRFLOW_HOME = os.environ.get('AIRFLOW_HOME', '/opt/airflow/')

PROJECT_ID = os.environ.get('GCP_PROJECT_ID')
BUCKET = os.environ.get('GCP_GCS_BUCKET')
BIGQUERY_DATASET = os.environ.get('BIGQUERY_DATASET', 'ny_taxi_data')

URL_PREFIX = 'https://d37ci6vzurychx.cloudfront.net/trip-data'

def get_prev_month(logic_date):
    true_date = datetime.strptime(logic_date, '%Y-%m-%d %H:%M:%S%z')
    prev_month_date = true_date - relativedelta(months=1)
    return prev_month_date.strftime('%Y-%m')

def upload_to_gcs(bucket, object_name, file):
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(file)

default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),
    "depends_on_past": True,
    "retires": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id = 'etl_to_gcs_bq',
    schedule_interval='0 6 2 * *',
    default_args = default_args,
    catchup = True,
    tags = ['de-zoomcamp'],
) as dag:
    get_prev_month_task = PythonOperator(
        task_id = 'get_prev_month_date_task',
        python_callable = get_prev_month,
        op_kwargs = {'logic_date': '{{ logical_date }}'}
    )

    # variables and values for upcoming tasks
    prev_month_date = "{{ ti.xcom_pull(task_ids='get_prev_month_date_task') }}"
    URL_TEMPLATE = URL_PREFIX + f'/yellow_tripdata_{prev_month_date}.parquet'
    OUTPUT_FILE_NAME = f'output_{prev_month_date}.parquet'
    OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + '/' + OUTPUT_FILE_NAME
    TABLE_NAME_TEMPLATE = f'yellow_taxi_{prev_month_date}'

    ingest_task = BashOperator(
        task_id = 'ingest_data_task',
        bash_command = f'curl -sSL {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}'
    )
    
    upload_task = PythonOperator(
        task_id = 'upload_data_to_gcs_task',
        python_callable = upload_to_gcs,
        op_kwargs = {
            'bucket': BUCKET,
            'object_name': f'raw/{OUTPUT_FILE_NAME}',
            'file': f'{OUTPUT_FILE_TEMPLATE}',
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id='bigquery_external_table_task',
        table_resource={
            'tableReference': {
                'projectId': PROJECT_ID,
                'datasetId': BIGQUERY_DATASET,
                'tableId': TABLE_NAME_TEMPLATE,
            },
            'externalDataConfiguration': {
                'sourceFormat': 'PARQUET',
                'sourceUris': [f'gs://{BUCKET}/raw/{OUTPUT_FILE_NAME}'],
            },
        },
    )
    
get_prev_month_task >> ingest_task >> upload_task >> bigquery_external_table_task