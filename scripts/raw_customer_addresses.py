"""
Drive → GCS → BigQuery (Excel Ingestion)
Raw layer: data as-is dari source, tidak ada cleaning.
Cleaning ada di dbt staging.
"""

import io
import os
import logging
from datetime import date, datetime
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ── LOAD ENV ONLY FOR LOCAL ─────────────────────────────────────
if not os.getenv("AIRFLOW_HOME"):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path=Path(__file__).resolve().parents[1] / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# ── CONFIG ──────────────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET_RAW")
BQ_DATASET = os.getenv("BQ_DATASET_RAW")
BQ_TABLE = "raw_customer_addresses"

DRIVE_FOLDER = os.getenv("DRIVE_FOLDER_ID")

INGESTION_DATE = date.today().isoformat()
FILE_PATTERN = "customer_addresses_"

# ── AUTH (CLEAN & SIMPLE) ───────────────────────────────────────
def get_sa_path():
    """
    PRIORITY:
    1. Local (kalau bukan Airflow)
    2. Airflow (kalau di container)
    """

    is_airflow = os.getenv("AIRFLOW_HOME") is not None

    if is_airflow:
        path = os.getenv("AIRFLOW_GOOGLE_APPLICATION_CREDENTIALS")
    else:
        path = os.getenv("LOCAL_GOOGLE_APPLICATION_CREDENTIALS")

    if not path:
        raise ValueError("Credential env tidak ditemukan")

    path = str(Path(path).resolve())

    if not os.path.exists(path):
        raise FileNotFoundError(f"Credential tidak ditemukan di: {path}")

    return path


def drive_creds():
    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_file(
        get_sa_path(),
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )


def gcp_creds():
    from google.oauth2 import service_account

    return service_account.Credentials.from_service_account_file(
        get_sa_path(),
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )

# ── PIPELINE ────────────────────────────────────────────────────
def find_latest_file():
    from googleapiclient.discovery import build

    service = build("drive", "v3", credentials=drive_creds(), cache_discovery=False)

    result = service.files().list(
        q=(
            f"'{DRIVE_FOLDER}' in parents "
            f"and name contains '{FILE_PATTERN}' "
            f"and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' "
            f"and trashed=false"
        ),
        orderBy="modifiedTime desc",
        fields="files(id, name, modifiedTime)",
        pageSize=1,
    ).execute()

    files = result.get("files", [])
    if not files:
        raise FileNotFoundError("File customer_addresses tidak ditemukan di Drive")

    return service, files[0]


def download_to_memory(service, file_id):
    from googleapiclient.http import MediaIoBaseDownload

    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, service.files().get_media(fileId=file_id))

    done = False
    while not done:
        _, done = dl.next_chunk()

    buf.seek(0)
    return buf


def read_as_is(buf, filename):
    df = pd.read_excel(buf, engine="openpyxl", dtype=str)

    df.columns = df.columns.str.strip().str.lower()

    df["_source_file"] = filename
    df["_ingestion_date"] = INGESTION_DATE
    df["_loaded_at"] = datetime.now().isoformat()

    return df


def upload_to_gcs(df):
    from google.cloud import storage

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    buf.seek(0)

    gcs_path = f"raw/excel/addresses/ingestion_date={INGESTION_DATE}/addresses.parquet"

    storage.Client(project=GCP_PROJECT, credentials=gcp_creds()) \
        .bucket(GCS_BUCKET) \
        .blob(gcs_path) \
        .upload_from_file(buf)

    return gcs_path


def load_to_bigquery(gcs_path):
    from google.cloud import bigquery

    bq = bigquery.Client(project=GCP_PROJECT, credentials=gcp_creds())
    table_ref = f"{GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"

    job = bq.load_table_from_uri(
        f"gs://{GCS_BUCKET}/{gcs_path}",
        table_ref,
        job_config=bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=True,
        ),
    )
    job.result()

    return bq.get_table(table_ref).num_rows


# ── MAIN ────────────────────────────────────────────────────────
def run():
    log.info(f"Starting Excel ingestion — {INGESTION_DATE}")

    service, file = find_latest_file()
    log.info(f"File: {file['name']}")

    buf = download_to_memory(service, file["id"])
    df = read_as_is(buf, file["name"])
    log.info(f"Rows read: {len(df)}")

    gcs_path = upload_to_gcs(df)
    log.info(f"GCS: gs://{GCS_BUCKET}/{gcs_path}")

    rows = load_to_bigquery(gcs_path)
    log.info(f"BigQuery: {GCP_PROJECT}.{BQ_DATASET}.{BQ_TABLE} ({rows} rows) ✅")


if __name__ == "__main__":
    run()