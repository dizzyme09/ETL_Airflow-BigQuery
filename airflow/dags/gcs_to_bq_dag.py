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
BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET")

DATASET = "tripdata"

# dictionary to map the TLC type to the datetime column name
# that will be used for partitioning
TLC_TYPES = {
    "yellow": "tpep_pickup_datetime",
    "green": "lpep_pickup_datetime",
    "fhv": "Pickup_datetime",
}

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

    # iterate through the TLC_TYPES dictionary
    for tlc_type, ds_col in TLC_TYPES.items():
        move_files_gcs_task = GCSToGCSOperator(
            # Move files from one GCS Bucket location to another
            task_id=f"move_{tlc_type}_{DATASET}_file_task",
            source_bucket=BUCKET,
            source_object=f"{INPUT_PART}/{tlc_type}_*",  # wildcard to move all files starting with tlc_type
            destination_bucket=BUCKET,
            destination_object=f"{tlc_type}/{tlc_type}_{DATASET}",  # move to tlc_type folder
            move_object=False,
        )

        bq_external_table_task = BigQueryCreateExternalTableOperator(
            # Create an external table in BigQuery
            task_id=f"bq_{tlc_type}_{DATASET}_external_table_task",
            table_resource={
                "tableReference": {
                    "projectId": PROJECT_ID,
                    "datasetId": BIGQUERY_DATASET,
                    "tableId": f"{tlc_type}_{DATASET}_external_table",
                },
                "externalDataConfiguration": {
                    "autodetect": "True",
                    "sourceFormat": "PARQUET",
                    "sourceUris": [f"gs://{BUCKET}/{tlc_type}/*"],
                },
            },
        )

        # a query to create a partitioned table in BigQuery
        CREATE_BQ_TABLE_QUERY = f"CREATE OR REPLACE TABLE {BIGQUERY_DATASET}.{tlc_type}_{DATASET} \
            PARTITION BY DATE({ds_col}) \
            AS \
            SELECT * FROM {BIGQUERY_DATASET}.{tlc_type}_{DATASET}_external_table;"

        bq_create_partitioned_table_task = BigQueryInsertJobOperator(
            # Create a partitioned table in BigQuery
            task_id=f"bq_create_{tlc_type}_{DATASET}_partitioned_table_task",
            configuration={
                "query": {"query": CREATE_BQ_TABLE_QUERY, "useLegacySql": False}
            },
        )

    move_files_gcs_task >> bq_external_table_task >> bq_create_partitioned_table_task
