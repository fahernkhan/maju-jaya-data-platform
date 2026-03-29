"""
MySQL → BigQuery (after_sales)
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

load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────────
GCP_PROJECT    = os.getenv("GCP_PROJECT_ID")
BQ_DATASET     = os.getenv("BQ_DATASET_RAW")
BQ_TABLE       = "raw_after_sales"
SOURCE_TABLE   = "after_sales_raw"
INGESTION_DATE = date.today().isoformat()

DB_URL = (
    f"mysql+pymysql://{os.getenv('MYSQL_USER')}:{os.getenv('MYSQL_PASSWORD')}"
    f"@{os.getenv('MYSQL_HOST')}:{os.getenv('MYSQL_PORT')}/{os.getenv('MYSQL_DB')}"
)


# ── PIPELINE ─────────────────────────────────────────────────────

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
    bq        = bigquery.Client(project=GCP_PROJECT)
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