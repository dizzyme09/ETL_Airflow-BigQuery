import os

from airflow import DAG
from airflow.utils.dates import days_ago

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateExternalTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator

AIRFLOW_HOME = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", "nyc_taxi_trips")

DATASET = "tripdata"
TAXI_TYPE = "yellow"
PARTITION_COLUMN = "tpep_pickup_datetime"

INPUT_PART = "raw"

default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retires": 1,
}

with DAG(
    dag_id="gcs_to_bq_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
) as dag:
    move_files_gcs_task = GCSToGCSOperator(
        # Move (copy) multiple files to the same destination bucket (data lake)
        task_id=f"move_{TAXI_TYPE}_{DATASET}_file_task",
        source_bucket=BUCKET,
        source_object=f"{INPUT_PART}/{TAXI_TYPE}_*",
        destination_bucket=BUCKET,
        destination_object=f"{TAXI_TYPE}/{TAXI_TYPE}_{DATASET}",
        move_object=False,
    )

    bq_external_table_task = BigQueryCreateExternalTableOperator(
        # Create an external table in BigQuery
        # from the files that were moved by the previous task
        task_id=f"bq_{TAXI_TYPE}_{DATASET}_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": f"{TAXI_TYPE}_{DATASET}_external_table",
            },
            "externalDataConfiguration": {
                "autodetect": "True",
                "sourceFormat": "PARQUET",
                "sourceUris": [
                    f"gs://{BUCKET}/{TAXI_TYPE}/*"
                ],  # All files in the bucket
            },
        },
    )

    # query for creating a partitioned table
    CREATE_BQ_TABLE_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{TAXI_TYPE}_{DATASET} \
            PARTITION BY DATE({PARTITION_COLUMN}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{TAXI_TYPE}_{DATASET}_external_table;"

    bq_create_partitioned_table_task = BigQueryInsertJobOperator(
        # Create a partitioned table in BigQuery
        task_id=f"bq_create_{TAXI_TYPE}_{DATASET}_partitioned_table_task",
        configuration={
            "query": {"query": CREATE_BQ_TABLE_QUERY, "useLegacySql": False}
        },
    )

    move_files_gcs_task >> bq_external_table_task >> bq_create_partitioned_table_task
