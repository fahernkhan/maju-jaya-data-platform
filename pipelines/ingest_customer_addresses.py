"""
Task 1: Daily ingestion pipeline
customer_addresses_yyyymmdd.csv → MySQL

Cara jalankan:
  python pipelines/ingest_customer_addresses.py --date 20260301
  python pipelines/ingest_customer_addresses.py              # semua file

Prinsip:
  - Idempotent: skip file yang sudah berhasil di-load
  - Audit trail: catat setiap run di pipeline_audit_log
  - Fail-safe: satu file gagal tidak stop file lain

Best Practice:
  - Raw layer = landing zone, minimal cleaning
  - Business logic TIDAK di sini, tapi di dbt staging/intermediate
"""

import os
import re
import argparse
import glob
import logging
from datetime import datetime

import pandas as pd
from sqlalchemy import create_engine, text

# ── CONFIG ────────────────────────────────────────────────────
MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB", "maju_jaya")
MYSQL_USER = os.getenv("MYSQL_USER", "maju_jaya")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "maju_jaya_pass")
FILE_DIR   = os.getenv("CSV_DIR", "./data")

DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

log = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-7s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def is_already_loaded(engine, filename: str) -> bool:
    """
    Idempotency check.
    Kenapa penting: kalau pipeline crash di tengah jalan lalu di-retry,
    file yang sudah sukses tidak di-load ulang → tidak ada duplikasi.
    """
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT COUNT(*) FROM pipeline_audit_log
            WHERE pipeline = 'customer_addresses'
              AND source_file = :filename
              AND status = 'success'
        """), {"filename": filename})
        return result.scalar() > 0


def validate_filename(filename: str) -> bool:
    """Validasi format: customer_addresses_yyyymmdd.csv"""
    return bool(re.match(r"^customer_addresses_\d{8}\.csv$", os.path.basename(filename)))


def clean_dataframe(df: pd.DataFrame, source_file: str) -> pd.DataFrame:
    """
    Minimal cleaning di landing layer.

    Best practice:
    - Hanya standardisasi format (Title Case, strip whitespace)
    - TIDAK ada business logic (itu tugas dbt intermediate)
    - Tambah metadata: source_file dan loaded_at untuk traceability
    """
    df.columns = df.columns.str.strip().str.lower()

    # Metadata columns
    df["source_file"] = source_file
    df["loaded_at"] = datetime.now()

    # Standardize: Title Case untuk kota & provinsi
    for col in ["city", "province"]:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip().str.title()

    # Parse datetime
    if "created_at" in df.columns:
        df["created_at"] = pd.to_datetime(df["created_at"], errors="coerce")

    return df


def load_file(engine, filepath: str) -> int:
    """Load satu CSV file ke MySQL."""
    filename = os.path.basename(filepath)
    df = pd.read_csv(filepath, dtype=str)  # dtype=str: hindari type inference salah
    df = clean_dataframe(df, filename)

    df.to_sql(
        "customer_addresses_raw",
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    return len(df)


def log_audit(engine, filename: str, status: str, rows: int, error: str = None):
    """
    Catat setiap run di audit log.
    ON DUPLICATE KEY UPDATE = kalau file sama diproses ulang, update status-nya.
    """
    with engine.begin() as conn:
        conn.execute(text("""
            INSERT INTO pipeline_audit_log
                (pipeline, source_file, status, rows_loaded, error_message)
            VALUES ('customer_addresses', :f, :s, :r, :e)
            ON DUPLICATE KEY UPDATE
                status = :s, rows_loaded = :r, error_message = :e,
                loaded_at = CURRENT_TIMESTAMP
        """), {"f": filename, "s": status, "r": rows, "e": error})


def run(date_str=None):
    """Main entry point."""
    engine = create_engine(DB_URL)

    # Determine which files to process
    if date_str:
        pattern = f"customer_addresses_{date_str}.csv"
    else:
        pattern = "customer_addresses_*.csv"

    files = sorted(glob.glob(os.path.join(FILE_DIR, pattern)))

    if not files:
        log.warning(f"No files found matching {pattern} in {FILE_DIR}")
        return

    log.info(f"Found {len(files)} file(s) to process")

    for filepath in files:
        filename = os.path.basename(filepath)

        # Validasi filename format
        if not validate_filename(filename):
            log.warning(f"SKIP - Invalid filename: {filename}")
            continue

        # Idempotency check
        if is_already_loaded(engine, filename):
            log.info(f"SKIP - Already loaded: {filename}")
            continue

        # Process file
        try:
            rows = load_file(engine, filepath)
            log_audit(engine, filename, "success", rows)
            log.info(f"OK   - {filename}: {rows:,} rows loaded")
        except Exception as e:
            log_audit(engine, filename, "failed", 0, str(e))
            log.error(f"FAIL - {filename}: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest customer_addresses CSV to MySQL")
    parser.add_argument("--date", help="yyyymmdd (optional, process specific date)")
    args = parser.parse_args()
    run(args.date)