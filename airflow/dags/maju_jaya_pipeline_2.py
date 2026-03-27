"""
DAG: maju_jaya_pipeline — PRODUCTION GRADE with Sensors

Design patterns applied:
  1. PythonSensor — verify data exists in BQ before proceeding
  2. Task retry with exponential backoff
  3. SLA monitoring
  4. Failure callbacks
  5. max_active_runs=1 (prevent race condition)
  6. Sensor mode="reschedule" (release worker slot while waiting)

Flow:
  [extract_mysql_direct]─────────────────────────────────┐
  [extract_excel_to_gcs] → [load_excel_gcs_to_bq]───────┤
                                                          ▼
                                              sensor_raw_data_ready
                                                          ▼
                                                    dbt_deps
                                                       ▼
                                              dbt_run_staging
                                                       ▼
                                          sensor_staging_ready
                                                       ▼
                                             dbt_test_staging
                                                       ▼
                                              dbt_run_marts
                                                       ▼
                                           sensor_marts_ready
                                                       ▼
                                             dbt_test_marts
                                                       ▼
                                            pipeline_complete
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.python import PythonSensor
from airflow.operators.empty import EmptyOperator

log = logging.getLogger(__name__)

DBT_DIR = "/opt/airflow/dbt"


# ══════════════════════════════════════════════════════════════
# SENSOR CALLABLES
# ══════════════════════════════════════════════════════════════

def _check_bq_table_has_rows(project, dataset, table, min_rows=1):
    """
    PythonSensor callable: return True jika table punya >= min_rows.
    Sensor akan retry sampai kondisi terpenuhi atau timeout.
    """
    from google.cloud import bigquery
    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"
    try:
        t = client.get_table(table_ref)
        has_data = t.num_rows >= min_rows
        log.info(f"Sensor check: {table_ref} = {t.num_rows} rows (need >= {min_rows}) → {'PASS' if has_data else 'WAIT'}")
        return has_data
    except Exception as e:
        log.warning(f"Sensor check: {table_ref} → {e}")
        return False


def _check_gcs_file_exists(bucket, path):
    """
    PythonSensor callable: return True jika file ada di GCS.
    Dipakai untuk verify Excel upload sebelum batch load.
    """
    from google.cloud import storage
    client = storage.Client()
    b = client.bucket(bucket)
    blob = b.blob(path)
    exists = blob.exists()
    log.info(f"GCS check: gs://{bucket}/{path} → {'EXISTS' if exists else 'NOT FOUND'}")
    return exists


# ══════════════════════════════════════════════════════════════
# INGESTION CALLABLES
# ══════════════════════════════════════════════════════════════

def _extract_mysql_direct(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/scripts")
    from extract_mysql_to_bigquery import extract_and_load
    extract_and_load()

def _extract_excel_to_gcs(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/scripts")
    from extract_excel_to_gcs import process_local_files
    process_local_files()

def _load_excel_from_gcs(**ctx):
    import sys; sys.path.insert(0, "/opt/airflow/scripts")
    from load_gcs_to_bigquery import load_all
    load_all()


# ══════════════════════════════════════════════════════════════
# FAILURE CALLBACK
# ══════════════════════════════════════════════════════════════

def _on_failure(context):
    """Log failure details. Replace with Slack/email in production."""
    ti = context["task_instance"]
    log.error(
        f"❌ PIPELINE FAILED\n"
        f"   DAG:  {ti.dag_id}\n"
        f"   Task: {ti.task_id}\n"
        f"   Date: {context['ds']}\n"
        f"   Log:  {ti.log_url}"
    )


# ══════════════════════════════════════════════════════════════
# DAG DEFINITION
# ══════════════════════════════════════════════════════════════

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,     # 2min → 4min → 8min
    "max_retry_delay": timedelta(minutes=15),
    "start_date": datetime(2026, 3, 1),
    "sla": timedelta(hours=2),
    "on_failure_callback": _on_failure,
}

GCP_PROJECT = "maju-jaya-platform"
BQ_RAW      = "raw_maju"
GCS_BUCKET  = "maju-jaya-raw-dev"

with DAG(
    dag_id="maju_jaya_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["maju-jaya", "production"],
    doc_md=__doc__,
) as dag:

    # ── START ────────────────────────────────────────────────
    start = EmptyOperator(task_id="start")

    # ── EXTRACT: MySQL → BigQuery direct ─────────────────────
    extract_mysql = PythonOperator(
        task_id="extract_mysql_direct_to_bq",
        python_callable=_extract_mysql_direct,
    )

    # ── EXTRACT: Excel → GCS ─────────────────────────────────
    extract_excel = PythonOperator(
        task_id="extract_excel_to_gcs",
        python_callable=_extract_excel_to_gcs,
    )

    # ── SENSOR: verify Excel landed in GCS before loading ────
    sensor_gcs_file = PythonSensor(
        task_id="sensor_gcs_excel_file",
        python_callable=_check_gcs_file_exists,
        op_kwargs={
            "bucket": GCS_BUCKET,
            "path": "raw/excel/addresses/ingestion_date={{ ds }}/addresses.parquet",
        },
        mode="reschedule",      # Release worker slot while waiting
        poke_interval=30,       # Check every 30 seconds
        timeout=600,            # Max wait 10 minutes
    )

    # ── LOAD: GCS → BigQuery (free batch load) ───────────────
    load_excel_bq = PythonOperator(
        task_id="load_excel_gcs_to_bq",
        python_callable=_load_excel_from_gcs,
    )

    # ── SENSOR: verify ALL raw tables have data in BigQuery ──
    sensor_raw_customers = PythonSensor(
        task_id="sensor_raw_customers",
        python_callable=_check_bq_table_has_rows,
        op_kwargs={"project": GCP_PROJECT, "dataset": BQ_RAW, "table": "customers", "min_rows": 1},
        mode="reschedule", poke_interval=30, timeout=300,
    )

    sensor_raw_sales = PythonSensor(
        task_id="sensor_raw_sales",
        python_callable=_check_bq_table_has_rows,
        op_kwargs={"project": GCP_PROJECT, "dataset": BQ_RAW, "table": "sales", "min_rows": 1},
        mode="reschedule", poke_interval=30, timeout=300,
    )

    sensor_raw_addresses = PythonSensor(
        task_id="sensor_raw_addresses",
        python_callable=_check_bq_table_has_rows,
        op_kwargs={"project": GCP_PROJECT, "dataset": BQ_RAW, "table": "customer_addresses", "min_rows": 1},
        mode="reschedule", poke_interval=30, timeout=300,
    )

    # ── DBT: deps ────────────────────────────────────────────
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=f"cd {DBT_DIR} && dbt deps --profiles-dir .",
    )

    # ── DBT: staging ─────────────────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --select staging",
    )

    # ── SENSOR: verify staging views are queryable ───────────
    sensor_staging = PythonSensor(
        task_id="sensor_staging_ready",
        python_callable=_check_bq_table_has_rows,
        op_kwargs={"project": GCP_PROJECT, "dataset": "stg_maju", "table": "stg_customers", "min_rows": 1},
        mode="reschedule", poke_interval=15, timeout=120,
    )

    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --select staging",
    )

    # ── DBT: marts (includes intermediate ephemeral) ─────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --profiles-dir . --select intermediate marts",
    )

    # ── SENSOR: verify mart tables populated ─────────────────
    sensor_mart_sales = PythonSensor(
        task_id="sensor_mart_fact_sales",
        python_callable=_check_bq_table_has_rows,
        op_kwargs={"project": GCP_PROJECT, "dataset": "mart_maju", "table": "fact_sales", "min_rows": 1},
        mode="reschedule", poke_interval=15, timeout=120,
    )

    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --profiles-dir . --select marts",
    )

    # ── END ──────────────────────────────────────────────────
    end = EmptyOperator(task_id="pipeline_complete")

    # ══════════════════════════════════════════════════════════
    # DEPENDENCIES — the full picture
    # ══════════════════════════════════════════════════════════

    # Ingestion (parallel paths)
    start >> extract_mysql                                           # Path A: MySQL direct
    start >> extract_excel >> sensor_gcs_file >> load_excel_bq       # Path B: Excel via GCS

    # Converge: both paths must complete, then verify raw data
    [extract_mysql, load_excel_bq] >> [sensor_raw_customers, sensor_raw_sales, sensor_raw_addresses]

    # dbt staging
    [sensor_raw_customers, sensor_raw_sales, sensor_raw_addresses] >> dbt_deps >> dbt_run_staging
    dbt_run_staging >> sensor_staging >> dbt_test_staging

    # dbt marts
    dbt_test_staging >> dbt_run_marts >> sensor_mart_sales >> dbt_test_marts

    # Done
    dbt_test_marts >> end