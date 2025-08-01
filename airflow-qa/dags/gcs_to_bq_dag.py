from airflow import DAG
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from datetime import datetime

with DAG(
    dag_id="gcs_to_bq_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    load_data = GCSToBigQueryOperator(
        task_id="load_existing_bq_table",
        bucket="sp500-airflow-de-project",  # your GCS bucket name
        source_objects=["sp500-dataset/sp500.csv"],  # path inside bucket
        destination_project_dataset_table="target-de.sp500.t_sp500",  # full BQ table
        source_format="CSV",
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND",  # or WRITE_APPEND / WRITE_EMPTY
        gcp_conn_id="google_cloud_default",  # <-- THIS LINKS TO YOUR CONNECTION
    )
