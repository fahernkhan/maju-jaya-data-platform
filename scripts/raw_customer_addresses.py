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
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
load_dotenv(dotenv_path=PROJECT_ROOT / ".env")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s")
log = logging.getLogger(__name__)

# ── CONFIG ───────────────────────────────────────────────────────
GCP_PROJECT = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET = os.getenv("GCS_BUCKET_RAW")
BQ_DATASET = os.getenv("BQ_DATASET_RAW")
BQ_TABLE       = "raw_customer_addresses"

DRIVE_FOLDER = os.getenv("DRIVE_FOLDER_ID")

SA_CREDS_LOCAL = os.getenv("LOCAL_GOOGLE_APPLICATION_CREDENTIALS")
SA_CREDS_DOCKER = os.getenv("AIRFLOW_GOOGLE_APPLICATION_CREDENTIALS")

INGESTION_DATE = date.today().isoformat()
FILE_PATTERN = "customer_addresses_"


# ── AUTH ─────────────────────────────────────────────────────────

def get_sa_path() -> str:
    """Ambil path service account credentials yang tersedia.
    Support path absolute maupun relative ke root project.
    """
    candidates = []

    if SA_CREDS_LOCAL:
        local_path = Path(SA_CREDS_LOCAL)
        if local_path.is_absolute():
            candidates.append(local_path)
        else:
            candidates.append(PROJECT_ROOT / local_path)
            candidates.append(Path.cwd() / local_path)

    if SA_CREDS_DOCKER:
        docker_path = Path(SA_CREDS_DOCKER)
        candidates.append(docker_path)

    for path in candidates:
        if path.exists():
            return str(path.resolve())

    raise FileNotFoundError(
        "Service account credentials tidak ditemukan.\n"
        f"Checked candidates:\n" +
        "\n".join(f"- {p}" for p in candidates)
    )


def drive_creds():
    """Service Account credentials untuk Google Drive."""
    from google.oauth2 import service_account

    path = get_sa_path()
    return service_account.Credentials.from_service_account_file(
        path,
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )


def gcp_creds():
    """Service Account credentials untuk GCS + BigQuery."""
    from google.oauth2 import service_account

    path = get_sa_path()
    return service_account.Credentials.from_service_account_file(
        path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )


# ── PIPELINE ─────────────────────────────────────────────────────

def find_latest_file() -> tuple:
    """Cari file Excel terbaru di Google Drive. Return (service, file_meta)."""
    from googleapiclient.discovery import build

    service = build("drive", "v3", credentials=drive_creds(), cache_discovery=False)

    result = service.files().list(
        q=(
            f"'{DRIVE_FOLDER}' in parents"
            f" and name contains '{FILE_PATTERN}'"
            f" and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'"
            f" and trashed=false"
        ),
        orderBy="modifiedTime desc",
        fields="files(id, name, modifiedTime)",
        pageSize=1,
    ).execute()

    files = result.get("files", [])
    if not files:
        raise FileNotFoundError(f"Tidak ada '{FILE_PATTERN}*.xlsx' di folder Drive")

    return service, files[0]


def download_to_memory(service, file_id: str) -> io.BytesIO:
    """Stream file dari Drive ke RAM. Tidak ada disk write."""
    from googleapiclient.http import MediaIoBaseDownload

    buf = io.BytesIO()
    dl = MediaIoBaseDownload(buf, service.files().get_media(fileId=file_id))

    done = False
    while not done:
        _, done = dl.next_chunk()

    buf.seek(0)
    return buf


def read_as_is(buf: io.BytesIO, filename: str) -> pd.DataFrame:
    """
    Baca Excel as-is.
    Tambahkan metadata kolom untuk audit trail.
    """
    df = pd.read_excel(buf, engine="openpyxl", dtype=str)

    # Kalau mau benar-benar 100% raw, hapus baris normalisasi ini.
    # Kalau dipakai, ini hanya bantu stabilitas nama kolom.
    df.columns = df.columns.str.strip().str.lower()

    df["_source_file"] = filename
    df["_ingestion_date"] = INGESTION_DATE
    df["_loaded_at"] = datetime.now().isoformat()

    return df


def upload_to_gcs(df: pd.DataFrame) -> str:
    """DataFrame → Parquet in-memory → GCS."""
    from google.cloud import storage

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pandas(df), buf)
    buf.seek(0)

    gcs_path = f"raw/excel/addresses/ingestion_date={INGESTION_DATE}/addresses.parquet"

    storage.Client(project=GCP_PROJECT, credentials=gcp_creds()) \
        .bucket(GCS_BUCKET) \
        .blob(gcs_path) \
        .upload_from_file(buf, content_type="application/octet-stream")

    return gcs_path


def load_to_bigquery(gcs_path: str) -> int:
    """GCS Parquet → BigQuery. Batch load = gratis. WRITE_TRUNCATE = idempotent."""
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


# ── MAIN ─────────────────────────────────────────────────────────

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