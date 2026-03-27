"""
Load Parquet files dari GCS ke BigQuery raw dataset.
Pattern sama dengan Olist: load_to_bigquery.py
"""

from dotenv import load_dotenv
from pathlib import Path
from google.cloud import bigquery
import os

env_path = Path(__file__).resolve().parents[1] / ".env"
load_dotenv(dotenv_path=env_path)

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET     = os.getenv("GCS_BUCKET_RAW")
DATASET_ID = os.getenv("BQ_DATASET_RAW", "raw_maju")
INGESTION_DATE = "2026-03-26"
# INGESTION_DATE = "2026-03-28"  # atau date.today().isoformat()

client = bigquery.Client(project=PROJECT_ID)

# Mapping: (GCS path prefix, BQ table name)
TABLES = [
    (f"raw/mysql/customers/ingestion_date={INGESTION_DATE}/customers.parquet", "customers"),
    (f"raw/mysql/sales/ingestion_date={INGESTION_DATE}/sales.parquet", "sales"),
    (f"raw/mysql/after_sales/ingestion_date={INGESTION_DATE}/after_sales.parquet", "after_sales"),
    (f"raw/excel/addresses/ingestion_date={INGESTION_DATE}/addresses.parquet", "customer_addresses"),
]

job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.PARQUET,
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    autodetect=True,
)

for gcs_path, table_name in TABLES:
    uri = f"gs://{BUCKET}/{gcs_path}"
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

    load_job = client.load_table_from_uri(uri, table_ref, job_config=job_config)
    load_job.result()  # Wait for completion

    loaded = client.get_table(table_ref)
    print(f"✅ {table_name:25s} → {table_ref}  ({loaded.num_rows:,} rows)")

print("\n🎉 All tables loaded to BigQuery raw dataset.")