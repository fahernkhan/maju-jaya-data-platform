"""Excel → GCS sebagai Parquet (Option B, step 1)."""
import os, glob
from datetime import date, datetime
import pandas as pd
from google.cloud import storage
from dotenv import load_dotenv

load_dotenv()

GCP_PROJECT    = os.getenv("GCP_PROJECT_ID", "maju-jaya-platform")
BUCKET_NAME    = os.getenv("GCS_BUCKET_RAW", "maju-jaya-raw-dev")
INGESTION_DATE = date.today().isoformat()
EXCEL_DIR      = os.getenv("EXCEL_DIR", "./data/excel")

def clean_df(df, source_file):
    df.columns = df.columns.str.strip().str.lower()
    df["_source_file"] = source_file
    df["_ingestion_date"] = INGESTION_DATE
    df["_loaded_at"] = datetime.now().isoformat()
    for col in ["city","province"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()
    return df

def process_local_files(pattern=None):
    files = [pattern] if pattern and os.path.exists(pattern) else \
            sorted(glob.glob(os.path.join(EXCEL_DIR, "customer_addresses_*.xlsx")))
    if not files:
        print(f"⚠️ No files in {EXCEL_DIR}"); return

    all_dfs = []
    for fp in files:
        fn = os.path.basename(fp)
        df = pd.read_excel(fp, engine="openpyxl", dtype=str)
        all_dfs.append(clean_df(df, fn))
        print(f"  Read: {fn} ({len(df)} rows)")

    combined = pd.concat(all_dfs, ignore_index=True)
    parquet_path = "/tmp/customer_addresses.parquet"
    combined.to_parquet(parquet_path, index=False, engine="pyarrow")

    gcs = storage.Client(project=GCP_PROJECT)
    bucket = gcs.bucket(BUCKET_NAME)
    gcs_path = f"raw/excel/addresses/ingestion_date={INGESTION_DATE}/addresses.parquet"
    bucket.blob(gcs_path).upload_from_filename(parquet_path)
    print(f"  ✅ → gs://{BUCKET_NAME}/{gcs_path} ({len(combined)} rows)")

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--local")
    process_local_files(p.parse_args().local)