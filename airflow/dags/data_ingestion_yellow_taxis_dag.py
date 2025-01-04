import os

from airflow import DAG
from airflow.utils.dates import days_ago
from datetime import timedelta
from datetime import datetime
from dateutil.relativedelta import relativedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from google.cloud import storage

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

URL_PREFIX = "https://d37ci6vzurychx.cloudfront.net/trip-data"


def get_prev_month(logic_date):
    """
    Get the previous month from the given date
    """
    true_date = datetime.strptime(logic_date, "%Y-%m-%d %H:%M:%S%z")
    prev_month_date = true_date - relativedelta(months=1)
    return prev_month_date.strftime("%Y-%m")


def upload_to_gcs(bucket, object_name, file):
    """
    Ref: https://cloud.google.com/storage/docs/uploading-objects#storage-upload-object-python
    :param bucket: GCS bucket name
    :param object_name: target path & file-name
    :param local_file: source path & file-name
    :return:
    """
    # WORKAROUND to prevent timeout for files > 6 MB on 800 kbps upload speed.
    # (Ref: https://github.com/googleapis/python-storage/issues/74)
    storage.blob._MAX_MULTIPART_SIZE = 5 * 1024 * 1024
    storage.blob._DEFAULT_CHUNKSIZE = 5 * 1024 * 1024
    # END OF WORKAROUND

    client = storage.Client()
    bucket = client.bucket(bucket)

    blob = bucket.blob(object_name)
    blob.upload_from_filename(file)


default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 1, 1),  # start task from 2024
    "depends_on_past": False,
    "retires": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="data_ingestion_gcs_yellow_taxis_dag",
    schedule_interval="0 6 2 * *",  # Run the task at 6:00 AM on the 2nd of every month
    default_args=default_args,
    catchup=True,
    max_active_runs=3,
) as dag:
    get_prev_month_task = PythonOperator(
        # task to get the previous month
        task_id="get_prev_month_date_task",
        python_callable=get_prev_month,
        op_kwargs={"logic_date": "{{ logical_date }}"},
    )

    # logic:
    # when the task runs on each 2nd of the month, it will get the previous
    # month's date and use it to download the last month data from the URL

    # variables and values for upcoming tasks
    prev_month_date = "{{ ti.xcom_pull(task_ids='get_prev_month_date_task') }}"
    URL_TEMPLATE = URL_PREFIX + f"/yellow_tripdata_{prev_month_date}.parquet"
    OUTPUT_FILE_NAME = f"output_{prev_month_date}.parquet"
    OUTPUT_FILE_TEMPLATE = AIRFLOW_HOME + "/" + OUTPUT_FILE_NAME

    ingest_task = BashOperator(
        # task to download the data
        task_id="ingest_data_task",
        # add -f flag to fail the task if the download fails
        # ex: when the data for the previous month is not available
        # the task will fail and not proceed to the next task
        bash_command=f"curl -sSLf {URL_TEMPLATE} > {OUTPUT_FILE_TEMPLATE}",
    )

    upload_task = PythonOperator(
        # task to upload the data to GCS
        task_id="upload_data_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{OUTPUT_FILE_NAME}",
            "file": f"{OUTPUT_FILE_TEMPLATE}",
        },
    )

    remove_file_task = BashOperator(
        # task to remove the downloaded file from the local system
        task_id="remove_file_task",
        bash_command=f"rm {OUTPUT_FILE_TEMPLATE}",
    )

    get_prev_month_task >> ingest_task >> upload_task >> remove_file_task
