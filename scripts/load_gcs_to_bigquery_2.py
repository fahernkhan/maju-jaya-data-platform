"""GCS → BigQuery batch load (GRATIS). Hanya untuk file-based sources."""
import os
from datetime import date
from google.cloud import bigquery
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT    = os.getenv("GCP_PROJECT_ID", "maju-jaya-platform")
BUCKET         = os.getenv("GCS_BUCKET_RAW", "maju-jaya-raw-dev")
BQ_DATASET     = os.getenv("BQ_DATASET_RAW", "raw_maju")
INGESTION_DATE = date.today().isoformat()

FILE_SOURCES = [{
    "gcs_path": f"raw/excel/addresses/ingestion_date={INGESTION_DATE}/addresses.parquet",
    "bq_table": "customer_addresses",
}]

def load_all():
    client = bigquery.Client(project=GCP_PROJECT)
    cfg = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True)
    for src in FILE_SOURCES:
        uri = f"gs://{BUCKET}/{src['gcs_path']}"
        ref = f"{GCP_PROJECT}.{BQ_DATASET}.{src['bq_table']}"
        try:
            job = client.load_table_from_uri(uri, ref, job_config=cfg)
            job.result()
            t = client.get_table(ref)
            print(f"  ✅ {src['bq_table']:25s} → {ref} ({t.num_rows:,} rows)")
        except Exception as e:
            print(f"  ❌ {src['bq_table']:25s} → {e}")

if __name__ == "__main__":
    load_all()