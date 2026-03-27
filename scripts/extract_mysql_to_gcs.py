"""
Extract data dari MySQL source → GCS (Parquet format).
Sama seperti Olist: upload_to_gcs.py

Usage:
  python scripts/extract_mysql_to_gcs.py
"""

import os
from datetime import date
import pandas as pd
from sqlalchemy import create_engine
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

# ── CONFIG ─────────────────────────────────────────────────────
PROJECT_ID     = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME    = os.getenv("GCS_BUCKET_RAW")
INGESTION_DATE = date.today().isoformat()

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB", "maju_jaya")
MYSQL_USER = os.getenv("MYSQL_USER", "maju_jaya")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "maju_jaya_pass")

DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

TABLES = ["customers_raw", "sales_raw", "after_sales_raw"]


def extract_and_upload():
    engine = create_engine(DB_URL)
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)

    for table_name in TABLES:
        # Extract dari MySQL
        df = pd.read_sql_table(table_name, con=engine)
        df["_ingestion_date"] = INGESTION_DATE

        # Convert ke Parquet
        short_name = table_name.replace("_raw", "")
        parquet_path = f"/tmp/{short_name}.parquet"
        df.to_parquet(parquet_path, index=False, engine="pyarrow")

        # Upload ke GCS (Hive-style partitioning)
        gcs_path = (
            f"raw/mysql/{short_name}/"
            f"ingestion_date={INGESTION_DATE}/"
            f"{short_name}.parquet"
        )
        blob = bucket.blob(gcs_path)
        blob.upload_from_filename(parquet_path)

        print(f"✅ {table_name:25s} → gs://{BUCKET_NAME}/{gcs_path}  ({len(df):,} rows)")

    print(f"\n🎉 All MySQL tables extracted to GCS — ingestion_date={INGESTION_DATE}")


if __name__ == "__main__":
    extract_and_upload()