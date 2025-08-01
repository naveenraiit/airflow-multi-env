from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitJobOperator
)
from airflow.utils.dates import days_ago

PROJECT_ID = "target-de"
REGION = "us-east1"
CLUSTER_NAME = "gcs-to-bq-cluster"

BUCKET = "sp500-airflow-de-project"
PYSPARK_FILE = "gs://sp500-airflow-de-project/sp500-dataproc/gcs_to_bq_dataproc.py".format(BUCKET)

with DAG(
    dag_id="dataproc_gcs_to_bq",
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    create_cluster = DataprocCreateClusterOperator(
        task_id="create_cluster",
        project_id=PROJECT_ID,
        cluster_config={
            "master_config": {"num_instances": 1, "machine_type_uri": "n1-standard-2"},
            "worker_config": {"num_instances": 2, "machine_type_uri": "n1-standard-2"},
        },
        region=REGION,
        cluster_name=CLUSTER_NAME,
    )

    pyspark_job = {
        "reference": {"project_id": PROJECT_ID},
        "placement": {"cluster_name": CLUSTER_NAME},
        "pyspark_job": {"main_python_file_uri": PYSPARK_FILE},
    }

    submit_job = DataprocSubmitJobOperator(
        task_id="submit_pyspark_job",
        job=pyspark_job,
        region=REGION,
        project_id=PROJECT_ID,
    )

    delete_cluster = DataprocDeleteClusterOperator(
        task_id="delete_cluster",
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=REGION,
        trigger_rule="all_done",  # delete even if job fails
    )

    create_cluster >> submit_job >> delete_cluster
