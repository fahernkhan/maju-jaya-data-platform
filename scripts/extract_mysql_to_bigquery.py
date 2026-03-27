"""MySQL → BigQuery LANGSUNG (Option C, tanpa GCS)."""
import os
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT = os.getenv("GCP_PROJECT_ID", "maju-jaya-platform")
BQ_DATASET  = os.getenv("BQ_DATASET_RAW", "raw_maju")
INGESTION_DATE = date.today().isoformat()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB", "maju_jaya")
MYSQL_USER = os.getenv("MYSQL_USER", "maju_jaya")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "maju_jaya_pass")
DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

TABLES = {"customers_raw":"customers","sales_raw":"sales","after_sales_raw":"after_sales"}

def extract_and_load():
    engine = create_engine(DB_URL)
    bq = bigquery.Client(project=GCP_PROJECT)
    print(f"\n📦 MySQL → BigQuery direct (ingestion_date={INGESTION_DATE})\n")
    for src, dest in TABLES.items():
        df = pd.read_sql_table(src, con=engine)
        df["_ingestion_date"] = INGESTION_DATE
        df["_source"] = "mysql"
        table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{dest}"
        job = bq.load_table_from_dataframe(
            df, table_ref,
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"))
        job.result()
        loaded = bq.get_table(table_ref)
        print(f"  ✅ {src:20s} → {table_ref} ({loaded.num_rows:,} rows)")
    print("\n🎉 MySQL extraction complete.")

if __name__ == "__main__":
    extract_and_load()