# Data Pipeline using Apache Airflow to Google BigQuery

### Tools:

1. Google Cloud Platform [GCP](https://console.cloud.google.com/)
   - Compute Engine
   - Google BigQuery
   - Cloud Storage Bucket
2. Apache Airflow

### Dataset:

- NYC Yellow taxi trip data 2024 from [TLC Trip Record Data](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page)

## Setup

### Google Cloud Platform

1. Create a project in GCP
2. Enable BigQuery API
3. Create a service account and download the JSON key (you will need this key to authenticate your Airflow DAG)
4. Create a bucket in Cloud Storage
5. Create a VM instance in Compute Engine (optional, you can use your local machine)

### Setup Docker and Airflow

- Use official guidance to setup Airflow with Docker from [here](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html), or you can use the `docker-compose.yaml` but make sure to update the environment variables with your GCP credentials and then straight away run `docker-compose up airflow-init`, and then `docker-compose up` to start the Airflow server.
