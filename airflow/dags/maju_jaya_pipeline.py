"""
DAG: maju_jaya_pipeline
Schedule: 0 6 * * * (daily 06:00)
Flow: Extract MySQL → Extract Excel → Upload GCS →
      Load BQ → dbt staging → dbt test → dbt marts →
      dbt test marts → DQ check
"""

from __future__ import annotations
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

DBT_DIR = "/opt/airflow/dbt"
PROFILES_DIR = "/opt/airflow/dbt"

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 3, 1),
    "sla": timedelta(hours=2),
}


def _extract_mysql():
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from extract_mysql_to_gcs import extract_and_upload
    extract_and_upload()


def _extract_excel():
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from extract_excel_to_gcs import process_local_files
    process_local_files()


def _load_bq():
    import sys
    sys.path.insert(0, "/opt/airflow/scripts")
    from load_gcs_to_bigquery import main
    main()


with DAG(
    dag_id="maju_jaya_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["maju-jaya", "production"],
    doc_md="""
    ## Maju Jaya Data Pipeline
    **Flow:** Extract (MySQL+Excel) → GCS → BigQuery → dbt transform → Quality checks
    """,
) as dag:

    extract_mysql = PythonOperator(
        task_id="extract_mysql_to_gcs",
        python_callable=_extract_mysql,
    )

    extract_excel = PythonOperator(
        task_id="extract_excel_to_gcs",
        python_callable=_extract_excel,
    )

    load_bigquery = PythonOperator(
        task_id="load_gcs_to_bigquery",
        python_callable=_load_bq,
    )

    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR} --select staging",
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR} --select staging",
    )

    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir {PROFILES_DIR} --select intermediate marts",
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir {PROFILES_DIR} --select marts",
    )

    # Dependencies
    [extract_mysql, extract_excel] >> load_bigquery
    load_bigquery >> dbt_run_staging >> dbt_test_staging
    dbt_test_staging >> dbt_run_marts >> dbt_test_marts