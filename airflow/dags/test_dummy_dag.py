from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="test_dummy_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    tags=["test"],
) as dag:

    test_airflow = BashOperator(
        task_id="test_airflow",
        bash_command="echo 'Airflow is working 🚀'"
    )

    test_venv = BashOperator(
        task_id="test_pipeline_venv",
        bash_command="/opt/pipeline-venv/bin/python -c \"print('Pipeline venv OK 🚀')\""
    )

    test_airflow >> test_venv