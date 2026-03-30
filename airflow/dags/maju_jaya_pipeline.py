from airflow.decorators import dag
from airflow.operators.bash import BashOperator
import pendulum

default_args = {
    "owner": "fahern",
    "retries": 2,
}

@dag(
    dag_id="maju_jaya_pipeline",
    start_date=pendulum.datetime(2024, 1, 1, tz="Asia/Jakarta"),
    schedule="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["maju_jaya", "elt"],
)
def maju_jaya_pipeline():

    # =========================================================
    # 🔹 LAYER 0: INGESTION (PIPELINE VENV)
    # =========================================================
    ingest_mysql = BashOperator(
        task_id="ingest_mysql",
        bash_command="""
        cd /opt/airflow &&
        /opt/pipeline-venv/bin/python scripts/raw_customers.py &&
        /opt/pipeline-venv/bin/python scripts/raw_sales.py &&
        /opt/pipeline-venv/bin/python scripts/raw_after_sales.py
        """
    )

    ingest_drive = BashOperator(
        task_id="ingest_drive",
        bash_command="""
        cd /opt/airflow &&
        /opt/pipeline-venv/bin/python scripts/raw_customer_addresses.py
        """
    )

    # =========================================================
    # 🔹 LAYER 1: DBT STAGING (PAKAI PIPELINE VENV)
    # =========================================================
    dbt_staging = BashOperator(
        task_id="dbt_staging",
        bash_command="""
        cd /opt/airflow/dbt &&
        /opt/pipeline-venv/bin/dbt run --select staging
        """
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command="""
        cd /opt/airflow/dbt &&
        /opt/pipeline-venv/bin/dbt test --select staging
        """
    )

    # =========================================================
    # 🔹 LAYER 2: DBT MARTS
    # =========================================================
    dbt_marts = BashOperator(
        task_id="dbt_marts",
        bash_command="""
        cd /opt/airflow/dbt &&
        /opt/pipeline-venv/bin/dbt run --select intermediate marts --full-refresh
        """
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command="""
        cd /opt/airflow/dbt &&
        /opt/pipeline-venv/bin/dbt test --select marts
        """
    )

    # =========================================================
    # 🔹 LAYER 3: FULL DATA QUALITY
    # =========================================================
    dbt_full_test = BashOperator(
        task_id="dbt_full_test",
        bash_command="""
        cd /opt/airflow/dbt &&
        /opt/pipeline-venv/bin/dbt test
        """
    )

    # =========================================================
    # 🔗 DEPENDENCIES (FAIL-FAST)
    # =========================================================
    [ingest_mysql, ingest_drive] >> dbt_staging >> dbt_test_staging
    dbt_test_staging >> dbt_marts >> dbt_test_marts >> dbt_full_test


dag = maju_jaya_pipeline()