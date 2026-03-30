"""
MySQL → BigQuery (sales)
Raw layer: data as-is dari source, tidak ada cleaning.
Cleaning ada di dbt staging.
"""

import os
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from dotenv import load_dotenv
from google.oauth2 import service_account

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────────
GCP_PROJECT    = os.getenv("GCP_PROJECT_ID")
BQ_DATASET     = os.getenv("BQ_DATASET_RAW")
BQ_TABLE       = "raw_sales"
SOURCE_TABLE   = "sales_raw"
INGESTION_DATE = date.today().isoformat()

def get_mysql_host():
    """
    Auto switch:
    - Airflow (Docker) → mysql-source
    - Local → 127.0.0.1
    """
    return "mysql-source" if os.getenv("AIRFLOW_HOME") else "127.0.0.1"


DB_URL = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}"
    f"@{get_mysql_host()}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)


# ── PIPELINE ─────────────────────────────────────────────────────
def get_sa_path():
    is_airflow = os.getenv("AIRFLOW_HOME") is not None

    if is_airflow:
        path = os.getenv("AIRFLOW_GOOGLE_APPLICATION_CREDENTIALS")
    else:
        path = os.getenv("LOCAL_GOOGLE_APPLICATION_CREDENTIALS")

    if not path:
        raise ValueError("Credential env tidak ditemukan")

    return path


def gcp_creds():
    return service_account.Credentials.from_service_account_file(
        get_sa_path(),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

def read_from_mysql() -> pd.DataFrame:
    """Baca tabel dari MySQL as-is. Hanya tambah metadata."""
    engine = create_engine(DB_URL)
    df = pd.read_sql_table(SOURCE_TABLE, con=engine)

    df["_ingestion_date"] = INGESTION_DATE
    df["_loaded_at"]      = datetime.now().isoformat()
    df["_source"]         = "mysql"

    return df


def load_to_bigquery(df: pd.DataFrame) -> int:
    """DataFrame → BigQuery direct. WRITE_TRUNCATE = idempotent."""
    bq        = bigquery.Client(project=GCP_PROJECT, credentials=gcp_creds())
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job = bq.load_table_from_dataframe(
        df, table_ref,
        job_config=bigquery.LoadJobConfig(
            write_disposition="WRITE_TRUNCATE"
        ),
    )
    job.result()
    return bq.get_table(table_ref).num_rows


# ── MAIN ─────────────────────────────────────────────────────────

def run():
    log.info(f"Starting {SOURCE_TABLE} ingestion — {INGESTION_DATE}")

    df = read_from_mysql()
    log.info(f"Rows read: {len(df)}")

    rows = load_to_bigquery(df)
    log.info(f"BigQuery: {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE} ({rows} rows) ✅")


if __name__ == "__main__":
    run()