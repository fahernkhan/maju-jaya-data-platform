"""
Download Excel dari Google Drive → GCS (Parquet format).

Dua mode:
  1. Local Excel file: python scripts/extract_excel_to_gcs.py --local data/excel/customer_addresses_20260301.xlsx
  2. Google Drive: python scripts/extract_excel_to_gcs.py --drive

Best practice:
  - Excel dibaca dengan openpyxl
  - Minimal cleaning di ingestion layer (Title Case city/province)
  - Disimpan ke GCS sebagai Parquet (columnar, compressed)
"""

import os
import re
import argparse
import glob
from datetime import date, datetime
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

PROJECT_ID     = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME    = os.getenv("GCS_BUCKET_RAW")
INGESTION_DATE = date.today().isoformat()
EXCEL_DIR      = os.getenv("EXCEL_DIR", "./data/excel")


def clean_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """Minimal cleaning — same as Olist clean_dataframe pattern."""
    df.columns = df.columns.str.strip().str.lower()
    df["_source_file"] = source_file
    df["_ingestion_date"] = INGESTION_DATE
    df["_loaded_at"] = datetime.now().isoformat()

    for col in ["city", "province"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    return df


def upload_to_gcs(df: pd.DataFrame, source_file: str):
    """Upload DataFrame ke GCS sebagai Parquet."""
    gcs_client = storage.Client(project=PROJECT_ID)
    bucket = gcs_client.bucket(BUCKET_NAME)

    parquet_path = "/tmp/customer_addresses.parquet"
    df.to_parquet(parquet_path, index=False, engine="pyarrow")

    gcs_path = (
        f"raw/excel/addresses/"
        f"ingestion_date={INGESTION_DATE}/"
        f"addresses.parquet"
    )
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(parquet_path)

    print(f"✅ {source_file} → gs://{BUCKET_NAME}/{gcs_path}  ({len(df):,} rows)")


def process_local_files(pattern: str = None):
    """Proses Excel files dari folder local."""
    if pattern:
        files = [pattern]
    else:
        files = sorted(glob.glob(os.path.join(EXCEL_DIR, "customer_addresses_*.xlsx")))

    if not files:
        print(f"⚠️  No Excel files found in {EXCEL_DIR}")
        return

    all_dfs = []
    for filepath in files:
        filename = os.path.basename(filepath)
        print(f"📖 Reading: {filename}")
        df = pd.read_excel(filepath, engine="openpyxl", dtype=str)
        df = clean_dataframe(df, filename)
        all_dfs.append(df)

    combined = pd.concat(all_dfs, ignore_index=True)
    upload_to_gcs(combined, f"{len(files)} file(s)")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--local", help="Path ke Excel file specific")
    args = parser.parse_args()

    process_local_files(args.local)