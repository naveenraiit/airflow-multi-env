from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bash_operator_example",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False
) as dag:
    t1 = BashOperator(
        task_id="list_files",
        bash_command="ls -l /opt/airflow/dags"
    )
