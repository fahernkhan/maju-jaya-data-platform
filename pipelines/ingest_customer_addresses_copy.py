"""Task 1: Excel daily ingestion → MySQL landing table."""
import os, re, argparse, glob, logging
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

MYSQL_HOST = os.getenv("MYSQL_HOST","localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT","3306")
MYSQL_DB = os.getenv("MYSQL_DB","maju_jaya")
MYSQL_USER = os.getenv("MYSQL_USER","maju_jaya")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD","maju_jaya_pass")
EXCEL_DIR = os.getenv("EXCEL_DIR",os.getenv("CSV_DIR","./data/excel"))
DB_URL = f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)-7s | %(message)s")

def ensure_tables(engine):
    with engine.begin() as conn:
        conn.execute(text("""CREATE TABLE IF NOT EXISTS customer_addresses_raw (
            id INT, customer_id INT, address VARCHAR(255), city VARCHAR(100), province VARCHAR(100),
            created_at DATETIME, source_file VARCHAR(100) NOT NULL,
            loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP)"""))
        conn.execute(text("""CREATE TABLE IF NOT EXISTS pipeline_audit_log (
            id INT AUTO_INCREMENT PRIMARY KEY, pipeline VARCHAR(100), source_file VARCHAR(100),
            status VARCHAR(20), rows_loaded INT, error_message TEXT,
            loaded_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE KEY uq_pf (pipeline, source_file))"""))

def is_loaded(engine, fn):
    with engine.connect() as c:
        return c.execute(text("SELECT COUNT(*) FROM pipeline_audit_log WHERE pipeline='customer_addresses' AND source_file=:f AND status='success'"),{"f":fn}).scalar() > 0

def run(date_str=None):
    engine = create_engine(DB_URL)
    ensure_tables(engine)
    pats = [os.path.join(EXCEL_DIR,f"customer_addresses_{date_str}.xlsx")] if date_str else \
           sorted(glob.glob(os.path.join(EXCEL_DIR,"customer_addresses_*.xlsx")) +
                  glob.glob(os.path.join(EXCEL_DIR,"customer_addresses_*.csv")))
    files = [f for f in pats if os.path.exists(f)] if date_str else pats
    if not files: log.warning(f"No files in {EXCEL_DIR}"); return
    for fp in files:
        fn = os.path.basename(fp)
        if is_loaded(engine, fn): log.info(f"SKIP {fn}"); continue
        try:
            df = pd.read_excel(fp, engine="openpyxl", dtype=str) if fn.endswith('.xlsx') else pd.read_csv(fp, dtype=str)
            df.columns = df.columns.str.strip().str.lower()
            df["source_file"] = fn; df["loaded_at"] = datetime.now()
            for c in ["city","province"]:
                if c in df.columns: df[c] = df[c].astype(str).str.strip().str.title()
            df.to_sql("customer_addresses_raw",con=engine,if_exists="append",index=False,method="multi",chunksize=500)
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO pipeline_audit_log(pipeline,source_file,status,rows_loaded) VALUES('customer_addresses',:f,'success',:r) ON DUPLICATE KEY UPDATE status='success',rows_loaded=:r"),{"f":fn,"r":len(df)})
            log.info(f"OK   {fn}: {len(df)} rows")
        except Exception as e:
            with engine.begin() as conn:
                conn.execute(text("INSERT INTO pipeline_audit_log(pipeline,source_file,status,rows_loaded,error_message) VALUES('customer_addresses',:f,'failed',0,:e) ON DUPLICATE KEY UPDATE status='failed',error_message=:e"),{"f":fn,"e":str(e)})
            log.error(f"FAIL {fn}: {e}")

if __name__ == "__main__":
    p = argparse.ArgumentParser(); p.add_argument("--date")
    run(p.parse_args().date)