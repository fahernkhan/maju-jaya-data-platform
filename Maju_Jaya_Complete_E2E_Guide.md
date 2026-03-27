# 🚀 Maju Jaya Data Platform — Complete End-to-End Guide

> **Panduan lengkap dari NOL sampai FINAL** untuk mengerjakan AstraWorld Technical Test.
> Mengikuti best practice dari Olist Customer Intelligence Data Platform.
>
> Stack: Python · MySQL · Docker Compose · dbt · Airflow · Great Expectations · SQL

---

## 📋 Daftar Isi

1. [Arsitektur & Prinsip Desain](#step-0)
2. [Setup Project & Docker Compose](#step-1)
3. [Seed Data & Verifikasi MySQL](#step-2)
4. [Task 1 — Datalanding Pipeline](#step-3)
5. [Task 2a — Data Cleaning Views](#step-4)
6. [dbt Setup & Staging Models](#step-5)
7. [dbt Intermediate Models](#step-6)
8. [dbt Marts: Star Schema (Fact + Dimension)](#step-7)
9. [dbt Serving Layer (Report Queries = Task 2b)](#step-8)
10. [dbt Tests & Documentation](#step-9)
11. [Data Quality: Great Expectations](#step-10)
12. [Airflow DAG via Docker Compose](#step-11)
13. [Task 3 — DWH Design Diagram](#step-12)
14. [Final Verification & README](#step-13)

---

<a name="step-0"></a>
## STEP 0 — Arsitektur & Prinsip Desain

### Kenapa arsitektur ini?

```
┌─────────────────────────────────────────────────────────────────┐
│  DATA SOURCES                                                    │
│  MySQL: customers_raw, sales_raw, after_sales_raw                │
│  CSV File Share: customer_addresses_yyyymmdd.csv (daily)         │
└──────────────────────────┬──────────────────────────────────────┘
                           │ Python ingestion (Task 1)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 1: RAW / BRONZE — MySQL tables (IMMUTABLE)                │
│  customers_raw │ sales_raw │ after_sales_raw                     │
│  customer_addresses_raw │ pipeline_audit_log                     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt staging models (clean, cast, rename)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 2: STAGING — dbt views (1:1 dengan source)                │
│  stg_customers │ stg_sales │ stg_after_sales │ stg_addresses     │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt intermediate models (joins, business logic)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 3: INTERMEDIATE — dbt ephemeral (tidak persist ke DB)     │
│  int_customer_enriched │ int_sales_enriched                      │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt marts (star schema)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 4: MART / GOLD — Star Schema tables                       │
│  dim_customer │ dim_vehicle │ dim_date │ dim_service_type         │
│  fact_sales │ fact_after_sales                                    │
└──────────────────────────┬──────────────────────────────────────┘
                           │ dbt serving (pre-aggregated)
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  LAYER 5: SERVING — dbt tables (BI-ready)                        │
│  mart_sales_summary │ mart_aftersales_priority                   │
└──────────────────────────┬──────────────────────────────────────┘
                           ▼
                    Dashboard / BI Report
```

### 7 Prinsip Desain (dari Olist)

1. **Immutability** — Raw data TIDAK pernah dimodifikasi. Semua cleaning di layer terpisah.
2. **Idempotent** — Setiap pipeline aman dijalankan ulang. Tidak ada duplikasi.
3. **Layered Architecture** — raw → staging → intermediate → mart → serving. Setiap layer punya tanggung jawab jelas.
4. **Star Schema** — Fact + dimension tables di mart layer. OBT hanya di serving.
5. **Data Quality at Every Layer** — Validasi di raw (GE), post-transform (dbt tests), post-mart (GE).
6. **Audit Trail** — Setiap pipeline run dicatat.
7. **Modular** — Setiap komponen bisa di-test independen.

### Kenapa pakai dbt?

**Tanpa dbt:** kamu nulis SQL manual, jalankan satu-satu, tidak ada tests, tidak ada docs, tidak ada lineage.
**Dengan dbt:** SQL models versioned di git, auto-generate lineage graph, built-in testing framework, dan documentation yang bisa di-browse.

### Kenapa intermediate layer?

**Staging** = clean & cast (1:1 dengan source, tidak ada join)
**Intermediate** = business logic (join, kalkulasi, CASE WHEN) — ini "workspace" internal
**Mart** = star schema output yang dikonsumsi BI tool

Kalau kamu taruh business logic langsung di mart, maka:
- Kalau logic berubah, kamu harus edit fact table langsung
- Lineage jadi tidak jelas — "revenue dihitung di mana?"
- Sulit di-debug

Dengan intermediate, logic terpisah rapi dan bisa di-trace.

---

<a name="step-1"></a>
## STEP 1 — Setup Project & Docker Compose

### 1.1 Buat folder project

```bash
mkdir maju-jaya-data-platform
cd maju-jaya-data-platform

# Buat struktur folder
mkdir -p scripts pipelines cleaning data
mkdir -p dbt/models/staging dbt/models/intermediate
mkdir -p dbt/models/marts/core dbt/models/marts/serving
mkdir -p dbt/tests dbt/macros
mkdir -p data_quality/great_expectations/expectations
mkdir -p data_quality/great_expectations/checkpoints
mkdir -p airflow/dags airflow/logs airflow/plugins
mkdir -p docs/architecture docs/screenshots
```

### 1.2 Buat .env.example

```bash
cat > .env.example << 'EOF'
# MySQL
MYSQL_HOST=mysql
MYSQL_PORT=3306
MYSQL_DB=maju_jaya
MYSQL_USER=maju_jaya
MYSQL_PASSWORD=maju_jaya_pass
MYSQL_ROOT_PASSWORD=root_pass_123

# Paths
CSV_DIR=./data

# Airflow
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=mysql+pymysql://airflow:airflow_pass@mysql:3306/airflow_db
AIRFLOW__CORE__FERNET_KEY=81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs=
AIRFLOW__WEBSERVER__SECRET_KEY=super_secret_key_123
EOF
```

```bash
cp .env.example .env
# Edit .env jika perlu ubah password
```

### 1.3 Buat docker-compose.yml

**Ini yang paling penting — semua service dalam satu file.**

```bash
cat > docker-compose.yml << 'COMPOSE'
version: "3.9"

x-airflow-common: &airflow-common
  image: apache/airflow:2.9.3-python3.11
  environment:
    AIRFLOW__CORE__EXECUTOR: LocalExecutor
    AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: mysql+pymysql://airflow:airflow_pass@mysql:3306/airflow_db
    AIRFLOW__CORE__FERNET_KEY: "81HqDtbqAywKSOumSha3BhWNOdQ26slT6K0YaZeZyPs="
    AIRFLOW__WEBSERVER__SECRET_KEY: "super_secret_key_123"
    AIRFLOW__CORE__LOAD_EXAMPLES: "false"
    AIRFLOW__CORE__DAGS_FOLDER: /opt/airflow/dags
    MYSQL_HOST: mysql
    MYSQL_PORT: "3306"
    MYSQL_DB: maju_jaya
    MYSQL_USER: maju_jaya
    MYSQL_PASSWORD: maju_jaya_pass
    CSV_DIR: /opt/airflow/data
  volumes:
    - ./airflow/dags:/opt/airflow/dags
    - ./airflow/logs:/opt/airflow/logs
    - ./airflow/plugins:/opt/airflow/plugins
    - ./pipelines:/opt/airflow/pipelines
    - ./cleaning:/opt/airflow/cleaning
    - ./dbt:/opt/airflow/dbt
    - ./data:/opt/airflow/data
    - ./data_quality:/opt/airflow/data_quality
  depends_on:
    mysql:
      condition: service_healthy

services:
  # ── MySQL (Database Utama + Airflow metadata) ──────────────
  mysql:
    image: mysql:8.0
    container_name: maju-jaya-mysql
    environment:
      MYSQL_ROOT_PASSWORD: root_pass_123
    ports:
      - "3306:3306"
    volumes:
      - mysql_data:/var/lib/mysql
      - ./scripts/01_init_databases.sql:/docker-entrypoint-initdb.d/01_init.sql
      - ./scripts/02_create_tables.sql:/docker-entrypoint-initdb.d/02_tables.sql
      - ./scripts/03_seed_data.sql:/docker-entrypoint-initdb.d/03_seed.sql
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 10s
      retries: 10
      start_period: 30s
    command: --default-authentication-plugin=mysql_native_password

  # ── Adminer (Database GUI) ─────────────────────────────────
  adminer:
    image: adminer:latest
    container_name: maju-jaya-adminer
    ports:
      - "8080:8080"
    depends_on:
      mysql:
        condition: service_healthy

  # ── Airflow Init (one-time setup) ──────────────────────────
  airflow-init:
    <<: *airflow-common
    container_name: maju-jaya-airflow-init
    entrypoint: /bin/bash
    command:
      - -c
      - |
        pip install dbt-mysql great-expectations pymysql pandas sqlalchemy python-dotenv &&
        airflow db migrate &&
        airflow users create \
          --username admin \
          --password admin \
          --firstname Admin \
          --lastname User \
          --role Admin \
          --email admin@example.com || true
    restart: "no"

  # ── Airflow Webserver ──────────────────────────────────────
  airflow-webserver:
    <<: *airflow-common
    container_name: maju-jaya-airflow-web
    command: >
      bash -c "pip install dbt-mysql great-expectations pymysql pandas sqlalchemy python-dotenv &&
               airflow webserver --port 8081"
    ports:
      - "8081:8081"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    healthcheck:
      test: ["CMD", "curl", "--fail", "http://localhost:8081/health"]
      interval: 30s
      retries: 5
    restart: always

  # ── Airflow Scheduler ──────────────────────────────────────
  airflow-scheduler:
    <<: *airflow-common
    container_name: maju-jaya-airflow-scheduler
    command: >
      bash -c "pip install dbt-mysql great-expectations pymysql pandas sqlalchemy python-dotenv &&
               airflow scheduler"
    depends_on:
      airflow-init:
        condition: service_completed_successfully
    restart: always

volumes:
  mysql_data:
COMPOSE
```

**Penjelasan best practice:**

- **`x-airflow-common`** = YAML anchor. Supaya tidak copy-paste config yang sama 3x.
- **`LocalExecutor`** = cukup untuk development. Production pakai CeleryExecutor.
- **`mysql_native_password`** = Airflow belum support `caching_sha2_password` MySQL 8 default.
- **`service_completed_successfully`** = Airflow init harus selesai dulu sebelum webserver/scheduler start.
- **Volume mounts** = semua code di-mount ke container, jadi edit di local langsung tersedia.

### 1.4 Buat requirements.txt

```bash
cat > requirements.txt << 'EOF'
pandas==2.2.0
sqlalchemy==2.0.25
pymysql==1.1.0
python-dotenv==1.0.1
great-expectations==0.18.8
dbt-mysql==1.7.0
EOF
```

### 1.5 Buat .gitignore

```bash
cat > .gitignore << 'EOF'
.env
__pycache__/
*.pyc
airflow/logs/
data_quality/great_expectations/uncommitted/
dbt/target/
dbt/logs/
dbt/dbt_packages/
*.egg-info/
.DS_Store
EOF
```

---

<a name="step-2"></a>
## STEP 2 — Seed Data & Verifikasi MySQL

### 2.1 Buat init databases SQL

```bash
cat > scripts/01_init_databases.sql << 'EOF'
-- Buat database untuk aplikasi
CREATE DATABASE IF NOT EXISTS maju_jaya;
CREATE USER IF NOT EXISTS 'maju_jaya'@'%' IDENTIFIED BY 'maju_jaya_pass';
GRANT ALL PRIVILEGES ON maju_jaya.* TO 'maju_jaya'@'%';

-- Buat database untuk Airflow metadata
CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';

FLUSH PRIVILEGES;
EOF
```

### 2.2 Buat create tables SQL

```bash
cat > scripts/02_create_tables.sql << 'EOF'
USE maju_jaya;

-- ══════════════════════════════════════════════════════════════
-- RAW TABLES (Layer 1 — immutable, tidak pernah dimodifikasi)
-- ══════════════════════════════════════════════════════════════

CREATE TABLE IF NOT EXISTS customers_raw (
    id          INT PRIMARY KEY,
    name        VARCHAR(100),
    dob         VARCHAR(20),        -- Sengaja VARCHAR karena format campur-campur
    created_at  DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS sales_raw (
    vin           VARCHAR(20) PRIMARY KEY,
    customer_id   INT,
    model         VARCHAR(50),
    invoice_date  DATE,
    price         VARCHAR(20),      -- Sengaja VARCHAR karena format "350.000.000"
    created_at    DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS after_sales_raw (
    service_ticket  VARCHAR(20) PRIMARY KEY,
    vin             VARCHAR(20),
    customer_id     INT,
    model           VARCHAR(50),
    service_date    DATE,
    service_type    VARCHAR(5),     -- BP, PM, GR
    created_at      DATETIME
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Tabel target untuk pipeline ingestion (Task 1)
CREATE TABLE IF NOT EXISTS customer_addresses_raw (
    id            INT,
    customer_id   INT,
    address       VARCHAR(255),
    city          VARCHAR(100),
    province      VARCHAR(100),
    created_at    DATETIME,
    source_file   VARCHAR(100) NOT NULL,
    loaded_at     DATETIME DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Audit log untuk pipeline tracking
CREATE TABLE IF NOT EXISTS pipeline_audit_log (
    id            INT AUTO_INCREMENT PRIMARY KEY,
    pipeline      VARCHAR(100),
    source_file   VARCHAR(100),
    status        VARCHAR(20),
    rows_loaded   INT,
    error_message TEXT,
    loaded_at     DATETIME DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_pipeline_file (pipeline, source_file)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
EOF
```

### 2.3 Buat seed data SQL

```bash
cat > scripts/03_seed_data.sql << 'EOF'
USE maju_jaya;

-- ══════════════════════════════════════════════════════════════
-- SEED DATA — sample dari soal technical test
-- Data ini SENGAJA "kotor" untuk menunjukkan cleaning capability
-- ══════════════════════════════════════════════════════════════

INSERT INTO customers_raw (id, name, dob, created_at) VALUES
(1, 'Antonio',        '1998-08-04',  '2025-03-01 14:24:40.012'),
(2, 'Brandon',        '2001-04-21',  '2025-03-02 08:12:54.003'),
(3, 'Charlie',        '1980/11/15',  '2025-03-02 11:20:02.391'),  -- format YYYY/MM/DD
(4, 'Dominikus',      '14/01/1995',  '2025-03-03 09:50:41.852'),  -- format DD/MM/YYYY
(5, 'Erik',           '1900-01-01',  '2025-03-03 17:22:03.198'),  -- placeholder date
(6, 'PT Black Bird',   NULL,         '2025-03-04 12:52:16.122');   -- corporate, no DOB

INSERT INTO sales_raw (vin, customer_id, model, invoice_date, price, created_at) VALUES
('JIS8135SAD', 1, 'RAIZA',  '2025-03-01', '350.000.000', '2025-03-01 14:24:40.012'),
('MAS8160POE', 3, 'RANGGO', '2025-05-19', '430.000.000', '2025-05-19 14:29:21.003'),
('JLK1368KDE', 4, 'INNAVO', '2025-05-22', '600.000.000', '2025-05-22 16:10:28.120'),
('JLK1869KDF', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 14:04:31.021'),
('JLK1962KOP', 6, 'VELOS',  '2025-08-02', '390.000.000', '2025-08-02 15:21:04.201');
-- ^ Dua record terakhir: same customer, model, date → suspect duplicate

INSERT INTO after_sales_raw (service_ticket, vin, customer_id, model, service_date, service_type, created_at) VALUES
('T124-kgu1', 'MAS8160POE', 3, 'RANGGO', '2025-07-11', 'BP', '2025-07-11 09:24:40.012'),
('T560-jga1', 'JLK1368KDE', 4, 'INNAVO', '2025-08-04', 'PM', '2025-08-04 10:12:54.003'),
('T521-oai8', 'POI1059IIK', 5, 'RAIZA',  '2026-09-10', 'GR', '2026-09-10 12:45:02.391');
-- ^ POI1059IIK tidak ada di sales_raw (orphan VIN)
-- ^ 2026-09-10 = tanggal masa depan
EOF
```

### 2.4 Buat sample CSV file

```bash
cat > data/customer_addresses_20260301.csv << 'EOF'
id,customer_id,address,city,province,created_at
1,1,"Jalan Mawar V, RT 1/RW 2",Bekasi,Jawa Barat,2026-03-01 14:24:40.012
2,3,Jl Ababil Indah,Tangerang Selatan,Jawa Barat,2026-03-01 14:24:40.012
3,4,Jl. Kemang Raya 1 No 3,JAKARTA PUSAT,DKI JAKARTA,2026-03-01 14:24:40.012
4,6,Astra Tower Jalan Yos Sudarso 12,Jakarta Utara,DKI Jakarta,2026-03-01 14:24:40.012
EOF
```

### 2.5 Start Docker & Verifikasi

```bash
# Start semua services
docker compose up -d

# Tunggu MySQL healthy (~30 detik)
docker compose ps

# Verifikasi: MySQL ready
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya -e "SHOW TABLES;"
```

**Expected output:**
```
+----------------------+
| Tables_in_maju_jaya  |
+----------------------+
| after_sales_raw      |
| customer_addresses_raw|
| customers_raw        |
| pipeline_audit_log   |
| sales_raw            |
+----------------------+
```

```bash
# Verifikasi: seed data loaded
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT COUNT(*) as total FROM customers_raw;"
# Expected: 6

docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT COUNT(*) as total FROM sales_raw;"
# Expected: 5
```

**Akses GUI:**
- **Adminer:** http://localhost:8080 (Server: mysql, User: maju_jaya, Pass: maju_jaya_pass, DB: maju_jaya)
- **Airflow:** http://localhost:8081 (User: admin, Pass: admin)

### ✅ Checklist Step 2

```
[ ] docker compose ps → semua services healthy/running
[ ] MySQL: 5 tables exist, seed data loaded
[ ] Adminer: bisa login dan browse tables
[ ] Airflow UI: bisa login (mungkin perlu 1-2 menit pertama kali)
```

---

<a name="step-3"></a>
## STEP 3 — Task 1: Datalanding Pipeline

### 3.1 Buat pipeline ingestion

**Best practice dari Olist:**
- Idempotent (aman dijalankan ulang)
- Audit trail (catat setiap run)
- Fail-safe (satu file gagal tidak stop semua)

```bash
cat > pipelines/ingest_customer_addresses.py << 'PYTHON'
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
PYTHON
```

### 3.2 Jalankan & Verifikasi

```bash
# Install dependencies (jika jalankan di local, bukan di docker)
pip install pandas sqlalchemy pymysql python-dotenv

# Jalankan untuk tanggal spesifik
python pipelines/ingest_customer_addresses.py --date 20260301

# Expected output:
# 2026-03-28 10:00:00 | INFO    | Found 1 file(s) to process
# 2026-03-28 10:00:01 | INFO    | OK   - customer_addresses_20260301.csv: 4 rows loaded

# Jalankan LAGI (harus skip — idempotent!)
python pipelines/ingest_customer_addresses.py --date 20260301

# Expected output:
# 2026-03-28 10:00:02 | INFO    | SKIP - Already loaded: customer_addresses_20260301.csv
```

```bash
# Verifikasi di MySQL
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM customer_addresses_raw;"

docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM pipeline_audit_log;"
```

### ✅ Checklist Step 3

```
[ ] First run: 4 rows loaded successfully
[ ] Second run: SKIP (idempotent verified)
[ ] pipeline_audit_log: entry with status='success'
[ ] customer_addresses_raw: 4 rows with source_file & loaded_at filled
```

---

<a name="step-4"></a>
## STEP 4 — Task 2a: Data Cleaning Views

### 4.1 Identifikasi masalah data

| Tabel | Kolom | Masalah | Solusi |
|-------|-------|---------|--------|
| customers_raw | dob | 3 format berbeda + placeholder 1900-01-01 | Parse CASE WHEN, flag is_dob_suspect |
| customers_raw | name | "PT Black Bird" = corporate, dob NULL | Flag customer_type |
| sales_raw | price | String "350.000.000" bukan integer | REPLACE('.','') → CAST BIGINT |
| sales_raw | vin | 2 record same customer+model+date | Flag is_duplicate_suspect |
| after_sales_raw | vin | POI1059IIK tidak ada di sales_raw | Flag is_vin_not_in_sales |
| after_sales_raw | service_date | 2026-09-10 = masa depan | Flag is_future_date |

### 4.2 Buat cleaning script

```bash
cat > cleaning/clean_tables.py << 'PYTHON'
"""
Task 2a: Data Cleaning via SQL VIEWs.

Best practice (dari Olist):
- Raw tables TIDAK dimodifikasi (immutability principle)
- Cleaning dilakukan via VIEW terpisah
- Setiap masalah di-FLAG, bukan di-delete (data lineage tetap traceable)

Run: python cleaning/clean_tables.py
"""

import os
from sqlalchemy import create_engine, text

MYSQL_HOST = os.getenv("MYSQL_HOST", "localhost")
MYSQL_PORT = os.getenv("MYSQL_PORT", "3306")
MYSQL_DB   = os.getenv("MYSQL_DB", "maju_jaya")
MYSQL_USER = os.getenv("MYSQL_USER", "maju_jaya")
MYSQL_PASS = os.getenv("MYSQL_PASSWORD", "maju_jaya_pass")

engine = create_engine(
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASS}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}"
)

VIEWS = {
    # ── customers_clean ─────────────────────────────────────
    "customers_clean": """
    CREATE OR REPLACE VIEW customers_clean AS
    SELECT
        id AS customer_id,
        name,
        -- Standardize date format: 3 format → 1 format
        CASE
            WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
                THEN STR_TO_DATE(dob, '%Y-%m-%d')
            WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
                THEN STR_TO_DATE(dob, '%Y/%m/%d')
            WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
                THEN STR_TO_DATE(dob, '%d/%m/%Y')
            ELSE NULL
        END AS dob,
        -- Flag: dob mencurigakan (placeholder atau NULL)
        CASE
            WHEN dob = '1900-01-01' OR dob IS NULL THEN TRUE
            ELSE FALSE
        END AS is_dob_suspect,
        -- Flag: tipe customer (individual vs corporate)
        CASE
            WHEN name LIKE 'PT %' OR name LIKE 'CV %'
                 OR name LIKE 'UD %' OR dob IS NULL
            THEN 'corporate'
            ELSE 'individual'
        END AS customer_type,
        created_at
    FROM customers_raw
    """,

    # ── sales_clean ─────────────────────────────────────────
    "sales_clean": """
    CREATE OR REPLACE VIEW sales_clean AS
    SELECT
        vin,
        customer_id,
        model,
        invoice_date,
        -- Price: strip titik ribuan, cast ke integer
        CAST(REPLACE(price, '.', '') AS UNSIGNED) AS price,
        -- Flag: suspect duplicate (same customer + model + date)
        CASE
            WHEN COUNT(*) OVER (
                PARTITION BY customer_id, model, invoice_date
            ) > 1 THEN TRUE
            ELSE FALSE
        END AS is_duplicate_suspect,
        created_at
    FROM sales_raw
    """,

    # ── after_sales_clean ───────────────────────────────────
    "after_sales_clean": """
    CREATE OR REPLACE VIEW after_sales_clean AS
    SELECT
        a.service_ticket,
        a.vin,
        a.customer_id,
        a.model,
        a.service_date,
        a.service_type,
        -- Flag: VIN yang tidak ada di sales_raw (orphan)
        CASE WHEN s.vin IS NULL THEN TRUE ELSE FALSE END AS is_vin_not_in_sales,
        -- Flag: tanggal masa depan
        CASE WHEN a.service_date > CURDATE() THEN TRUE ELSE FALSE END AS is_future_date,
        a.created_at
    FROM after_sales_raw a
    LEFT JOIN (SELECT DISTINCT vin FROM sales_raw) s
        ON a.vin = s.vin
    """,
}


def run():
    with engine.begin() as conn:
        for view_name, sql in VIEWS.items():
            conn.execute(text(sql))
            print(f"  ✅ {view_name} created")
    print("\nAll cleaning views created successfully.")


if __name__ == "__main__":
    run()
PYTHON
```

### 4.3 Jalankan & Verifikasi

```bash
python cleaning/clean_tables.py

# Verifikasi customers_clean
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT customer_id, name, dob, is_dob_suspect, customer_type FROM customers_clean;"

# Expected: Erik → is_dob_suspect=1, PT Black Bird → customer_type=corporate

# Verifikasi sales_clean
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT vin, model, price, is_duplicate_suspect FROM sales_clean;"

# Expected: JLK1869KDF & JLK1962KOP → is_duplicate_suspect=1, price = integer
```

### ✅ Checklist Step 4

```
[ ] 3 views created: customers_clean, sales_clean, after_sales_clean
[ ] customers_clean: dob standardized, is_dob_suspect working, customer_type correct
[ ] sales_clean: price as BIGINT, duplicates flagged
[ ] after_sales_clean: orphan VIN flagged, future dates flagged
```

---

<a name="step-5"></a>
## STEP 5 — dbt Setup & Staging Models

### 5.1 Setup dbt project

```bash
cat > dbt/dbt_project.yml << 'EOF'
name: maju_jaya_platform
version: '1.0.0'
profile: maju_jaya

# ── Materialization per layer ────────────────────────────────
# Best practice: setiap layer punya materialization berbeda
models:
  maju_jaya_platform:
    staging:
      +materialized: view          # View = tidak bayar storage, selalu fresh
      +schema: staging
    intermediate:
      +materialized: ephemeral     # Ephemeral = tidak persist, hanya CTE
    marts:
      core:
        +materialized: table       # Table = persist, query cepat
        +schema: mart
      serving:
        +materialized: table
        +schema: mart

# ── Test severity ────────────────────────────────────────────
tests:
  +severity: warn    # Default: warning, not error (bisa di-override per test)

# ── Clean targets ────────────────────────────────────────────
clean-targets:
  - target
  - dbt_packages
  - logs
EOF
```

```bash
cat > dbt/profiles.yml << 'EOF'
maju_jaya:
  target: dev
  outputs:
    dev:
      type: mysql
      server: localhost        # Ganti 'mysql' jika run dari dalam container
      port: 3306
      schema: maju_jaya
      username: maju_jaya
      password: maju_jaya_pass
      charset: utf8mb4
      threads: 4
EOF
```

```bash
cat > dbt/packages.yml << 'EOF'
packages:
  - package: dbt-labs/dbt_utils
    version: [">=1.0.0", "<2.0.0"]
EOF
```

### 5.2 Definisikan sources

```bash
cat > dbt/models/staging/_staging__sources.yml << 'EOF'
version: 2

sources:
  - name: raw
    description: "Raw tables dari MySQL — data asli, tidak pernah dimodifikasi"
    schema: maju_jaya
    tables:
      - name: customers_raw
        description: "Data customer dari CRM"
        columns:
          - name: id
            tests: [unique, not_null]

      - name: sales_raw
        description: "Data penjualan kendaraan"
        columns:
          - name: vin
            tests: [unique, not_null]

      - name: after_sales_raw
        description: "Data servis kendaraan"
        columns:
          - name: service_ticket
            tests: [unique, not_null]

      - name: customer_addresses_raw
        description: "Data alamat customer dari CSV daily ingestion"
EOF
```

### 5.3 Buat staging models

**Aturan staging:** 1 model per 1 source table. Hanya cast, rename, filter null. TIDAK ada join atau business logic.

```bash
cat > dbt/models/staging/stg_customers.sql << 'EOF'
/*
  STAGING: stg_customers
  Source: customers_raw
  Job: Standardize date format, flag suspect data, classify customer type
  Rule: 1:1 dengan source, tidak ada join
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customers_raw') }}
)

SELECT
    id                              AS customer_id,
    name                            AS customer_name,

    -- Standardize 3 format tanggal → 1 format
    CASE
        WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$'
            THEN STR_TO_DATE(dob, '%Y-%m-%d')
        WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$'
            THEN STR_TO_DATE(dob, '%Y/%m/%d')
        WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$'
            THEN STR_TO_DATE(dob, '%d/%m/%Y')
        ELSE NULL
    END                             AS date_of_birth,

    -- Flags
    CASE
        WHEN dob = '1900-01-01' OR dob IS NULL THEN TRUE
        ELSE FALSE
    END                             AS is_dob_suspect,

    CASE
        WHEN name LIKE 'PT %' OR name LIKE 'CV %'
             OR name LIKE 'UD %' OR dob IS NULL
        THEN 'corporate'
        ELSE 'individual'
    END                             AS customer_type,

    created_at

FROM source
WHERE id IS NOT NULL
EOF
```

```bash
cat > dbt/models/staging/stg_sales.sql << 'EOF'
/*
  STAGING: stg_sales
  Source: sales_raw
  Job: Cast price dari string ke integer, flag duplicates
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'sales_raw') }}
),

cleaned AS (
    SELECT
        vin,
        customer_id,
        model,
        invoice_date,
        CAST(REPLACE(price, '.', '') AS UNSIGNED)   AS price,
        created_at,

        -- Flag duplicate: same customer + model + date
        COUNT(*) OVER (
            PARTITION BY customer_id, model, invoice_date
        ) AS dup_count

    FROM source
    WHERE vin IS NOT NULL
)

SELECT
    vin,
    customer_id,
    model,
    invoice_date,
    price,
    CASE WHEN dup_count > 1 THEN TRUE ELSE FALSE END AS is_duplicate_suspect,
    created_at
FROM cleaned
EOF
```

```bash
cat > dbt/models/staging/stg_after_sales.sql << 'EOF'
/*
  STAGING: stg_after_sales
  Source: after_sales_raw
  Job: Flag orphan VIN dan tanggal masa depan
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'after_sales_raw') }}
),

sales_vins AS (
    SELECT DISTINCT vin FROM {{ source('raw', 'sales_raw') }}
)

SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,
    a.service_date,
    a.service_type,
    CASE WHEN s.vin IS NULL THEN TRUE ELSE FALSE END    AS is_vin_not_in_sales,
    CASE WHEN a.service_date > CURDATE() THEN TRUE ELSE FALSE END AS is_future_date,
    a.created_at

FROM source a
LEFT JOIN sales_vins s ON a.vin = s.vin
WHERE a.service_ticket IS NOT NULL
EOF
```

```bash
cat > dbt/models/staging/stg_customer_addresses.sql << 'EOF'
/*
  STAGING: stg_customer_addresses
  Source: customer_addresses_raw (dari pipeline Task 1)
  Job: Standardize city/province, get latest address per customer
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'customer_addresses_raw') }}
),

-- Ambil alamat terbaru per customer (Window Function)
ranked AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY created_at DESC
        ) AS rn
    FROM source
)

SELECT
    id                  AS address_id,
    customer_id,
    address,
    TRIM(city)          AS city,
    TRIM(province)      AS province,
    created_at,
    source_file
FROM ranked
WHERE rn = 1    -- Hanya alamat terbaru per customer
EOF
```

### 5.4 Buat staging tests

```bash
cat > dbt/models/staging/_staging__models.yml << 'EOF'
version: 2

models:
  - name: stg_customers
    description: "Cleaned customer data. DOB standardized, type classified."
    columns:
      - name: customer_id
        description: "PK - unique customer identifier"
        tests: [unique, not_null]
      - name: customer_type
        tests:
          - accepted_values:
              values: ['individual', 'corporate']

  - name: stg_sales
    description: "Cleaned sales data. Price as integer, duplicates flagged."
    columns:
      - name: vin
        description: "PK - Vehicle Identification Number"
        tests: [unique, not_null]
      - name: price
        tests:
          - not_null

  - name: stg_after_sales
    description: "Cleaned after-sales data. Orphan VIN and future dates flagged."
    columns:
      - name: service_ticket
        description: "PK - unique service ticket"
        tests: [unique, not_null]

  - name: stg_customer_addresses
    description: "Latest address per customer from daily CSV ingestion."
    columns:
      - name: customer_id
        tests: [not_null]
EOF
```

### 5.5 Jalankan dbt staging

```bash
cd dbt

# Install packages (dbt_utils)
dbt deps

# Test connection
dbt debug
# Expected: "All checks passed!"

# Run staging models
dbt run --select staging
# Expected: 4 views created

# Run tests
dbt test --select staging
# Expected: all passed
```

### ✅ Checklist Step 5

```
[ ] dbt debug → "All checks passed!"
[ ] dbt run --select staging → 4 views created
[ ] dbt test --select staging → all tests passed
[ ] MySQL: SELECT * FROM maju_jaya_staging.stg_customers → data bersih
```

---

<a name="step-6"></a>
## STEP 6 — dbt Intermediate Models

### 6.1 Kenapa intermediate layer?

**Staging** hanya clean & cast (1:1 dengan source).
**Intermediate** = tempat JOIN dan business logic. Ini "workspace" internal yang TIDAK dikonsumsi langsung oleh BI tool.

**Materialized: ephemeral** = tidak persist ke database. Saat dbt compile, intermediate model menjadi CTE (Common Table Expression) di dalam query mart yang memanggilnya. Hemat storage, tapi logic tetap terpisah rapi.

### 6.2 Buat intermediate models

```bash
cat > dbt/models/intermediate/int_customer_enriched.sql << 'EOF'
/*
  INTERMEDIATE: int_customer_enriched
  ─────────────────────────────────────────────────────────
  Tujuan: Join customer + address terbaru
  Source: staging models ONLY (tidak pernah langsung dari raw)
  Output: ephemeral (CTE, tidak persist ke DB)

  Kenapa ephemeral?
  - Intermediate adalah "workspace" — tidak dikonsumsi langsung
  - Hemat storage
  - Tapi logic JOIN tetap terpisah dari mart (mudah debug)

  Aturan intermediate layer:
  1. Source hanya dari ref() ke staging — bukan source()
  2. Boleh join multiple staging models
  3. Boleh ada business logic (kalkulasi, CASE WHEN)
  4. TIDAK dikonsumsi langsung oleh BI tool
*/

WITH customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

addresses AS (
    SELECT * FROM {{ ref('stg_customer_addresses') }}
)

SELECT
    c.customer_id,
    c.customer_name,
    c.date_of_birth,
    c.is_dob_suspect,
    c.customer_type,

    -- Denormalized address (dari latest address per customer)
    a.address,
    a.city,
    a.province,

    -- Full address string (untuk report)
    COALESCE(
        CONCAT_WS(', ', a.address, a.city, a.province),
        'Alamat tidak tersedia'
    ) AS full_address,

    c.created_at

FROM customers c
LEFT JOIN addresses a USING (customer_id)
EOF
```

```bash
cat > dbt/models/intermediate/int_sales_enriched.sql << 'EOF'
/*
  INTERMEDIATE: int_sales_enriched
  ─────────────────────────────────────────────────────────
  Tujuan: Enrich sales dengan customer info + price classification
  Source: stg_sales + stg_customers
  Business logic: price class categorization, dedup handling
*/

WITH sales AS (
    SELECT * FROM {{ ref('stg_sales') }}
),

customers AS (
    SELECT
        customer_id,
        customer_name,
        customer_type
    FROM {{ ref('stg_customers') }}
),

-- Deduplicate: ambil record pertama per group
deduplicated AS (
    SELECT
        s.*,
        ROW_NUMBER() OVER (
            PARTITION BY s.customer_id, s.model, s.invoice_date
            ORDER BY s.created_at ASC
        ) AS rn
    FROM sales s
)

SELECT
    d.vin,
    d.customer_id,
    c.customer_name,
    c.customer_type,
    d.model,
    d.invoice_date,
    d.price,

    -- Business logic: price classification (dari requirement soal)
    CASE
        WHEN d.price BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN d.price BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN d.price > 400000000                     THEN 'HIGH'
        ELSE 'UNDEFINED'
    END AS price_class,

    -- Periode (YYYY-MM format dari requirement)
    DATE_FORMAT(d.invoice_date, '%Y-%m') AS periode,

    d.is_duplicate_suspect,
    d.created_at,

    -- Flag: is this the "canonical" record? (first per group)
    CASE WHEN d.rn = 1 THEN TRUE ELSE FALSE END AS is_canonical

FROM deduplicated d
LEFT JOIN customers c USING (customer_id)
WHERE d.invoice_date IS NOT NULL
EOF
```

```bash
cat > dbt/models/intermediate/_intermediate__models.yml << 'EOF'
version: 2

models:
  - name: int_customer_enriched
    description: >
      Customer enriched with latest address.
      Materialized as ephemeral (CTE only).
      Source: stg_customers + stg_customer_addresses.
    columns:
      - name: customer_id
        tests: [not_null]

  - name: int_sales_enriched
    description: >
      Sales enriched with customer info + price classification.
      Includes deduplication logic via ROW_NUMBER.
      Materialized as ephemeral (CTE only).
    columns:
      - name: vin
        tests: [not_null]
      - name: price_class
        tests:
          - accepted_values:
              values: ['LOW', 'MEDIUM', 'HIGH', 'UNDEFINED']
EOF
```

### 6.3 Jalankan & Verifikasi

```bash
cd dbt
dbt run --select intermediate
# Expected: 0 rows affected (ephemeral = tidak create table/view)
# Tapi compile harus sukses tanpa error

dbt test --select intermediate
# Expected: tests passed
```

**Penting:** Ephemeral model tidak muncul di database — mereka hanya CTE yang di-inline saat mart model dijalankan. Untuk melihat hasilnya, kamu harus run mart model yang mereferensi mereka.

### ✅ Checklist Step 6

```
[ ] dbt compile --select intermediate → no errors
[ ] int_customer_enriched: joins customer + address correctly
[ ] int_sales_enriched: price_class logic correct, dedup via ROW_NUMBER
[ ] Kedua model ephemeral (tidak muncul di MySQL sebagai table/view)
```

---

<a name="step-7"></a>
## STEP 7 — dbt Marts: Star Schema (Fact + Dimension)

### 7.1 Kenapa star schema?

Star schema memisahkan **measures** (angka yang diukur) ke **fact tables** dan **attributes** (konteks) ke **dimension tables**. Ini standar industri karena:
- Query cepat (join sederhana)
- Mudah dipahami BI tools
- Scalable

### 7.2 Dimension tables

```bash
cat > dbt/models/marts/core/dim_customer.sql << 'EOF'
/*
  DIMENSION: dim_customer
  Grain: 1 baris per customer
  Source: int_customer_enriched
*/

{{
    config(materialized='table')
}}

SELECT
    customer_id,
    customer_name,
    date_of_birth,
    is_dob_suspect,
    customer_type,
    city,
    province,
    full_address,
    created_at
FROM {{ ref('int_customer_enriched') }}
EOF
```

```bash
cat > dbt/models/marts/core/dim_vehicle.sql << 'EOF'
/*
  DIMENSION: dim_vehicle
  Grain: 1 baris per model kendaraan
  Source: int_sales_enriched (derived)
*/

{{
    config(materialized='table')
}}

SELECT DISTINCT
    model,
    -- Harga range per model
    MIN(price) AS min_price,
    MAX(price) AS max_price,
    AVG(price) AS avg_price,
    -- Dominant price class
    CASE
        WHEN AVG(price) BETWEEN 100000000 AND 250000000 THEN 'LOW'
        WHEN AVG(price) BETWEEN 250000001 AND 400000000 THEN 'MEDIUM'
        WHEN AVG(price) > 400000000                     THEN 'HIGH'
        ELSE 'UNDEFINED'
    END AS default_price_class
FROM {{ ref('int_sales_enriched') }}
WHERE is_canonical = TRUE
GROUP BY model
EOF
```

```bash
cat > dbt/models/marts/core/dim_date.sql << 'EOF'
/*
  DIMENSION: dim_date
  Grain: 1 baris per tanggal
  Generated date spine: 2024-01-01 → 2027-12-31

  Best practice:
  - Date dimension SELALU di-generate, bukan derived dari data
  - Memastikan semua tanggal ada, bahkan yang tidak ada transaksi
*/

{{
    config(materialized='table')
}}

WITH RECURSIVE date_spine AS (
    SELECT DATE('2024-01-01') AS date_id
    UNION ALL
    SELECT DATE_ADD(date_id, INTERVAL 1 DAY)
    FROM date_spine
    WHERE date_id < '2027-12-31'
)

SELECT
    date_id,
    YEAR(date_id)                        AS year,
    MONTH(date_id)                       AS month,
    QUARTER(date_id)                     AS quarter,
    DATE_FORMAT(date_id, '%Y-%m')        AS year_month,
    MONTHNAME(date_id)                   AS month_name,
    DAYNAME(date_id)                     AS day_name,
    DAYOFWEEK(date_id)                   AS day_of_week,
    CASE WHEN DAYOFWEEK(date_id) IN (1,7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
EOF
```

```bash
cat > dbt/models/marts/core/dim_service_type.sql << 'EOF'
/*
  DIMENSION: dim_service_type
  Grain: 1 baris per service type code
  Static/seed data — tidak berubah sering
*/

{{
    config(materialized='table')
}}

SELECT 'BP' AS service_type_code, 'Body Paint'            AS service_type_name, 'Perbaikan body dan cat kendaraan'        AS description
UNION ALL
SELECT 'PM', 'Periodic Maintenance', 'Servis berkala rutin sesuai jadwal'
UNION ALL
SELECT 'GR', 'General Repair',      'Perbaikan umum di luar jadwal berkala'
EOF
```

### 7.3 Fact tables

```bash
cat > dbt/models/marts/core/fact_sales.sql << 'EOF'
/*
  FACT: fact_sales
  Grain: 1 baris per transaksi penjualan (VIN)
  FK: dim_customer(customer_id), dim_vehicle(model), dim_date(date_id)

  Best practice:
  - Fact table berisi measures (price) + FK ke dimensions
  - Filter: hanya canonical records (no duplicates)
*/

{{
    config(materialized='table')
}}

SELECT
    s.vin,
    s.customer_id,
    s.model,
    s.invoice_date          AS date_id,
    s.price,
    s.price_class,
    s.periode,
    s.customer_name,
    s.customer_type,
    s.is_duplicate_suspect,
    s.created_at,
    CURRENT_TIMESTAMP()     AS dbt_loaded_at

FROM {{ ref('int_sales_enriched') }} s
WHERE s.is_canonical = TRUE      -- Hanya record pertama per group (dedup)
  AND s.invoice_date IS NOT NULL
EOF
```

```bash
cat > dbt/models/marts/core/fact_after_sales.sql << 'EOF'
/*
  FACT: fact_after_sales
  Grain: 1 baris per service ticket
  FK: dim_customer, dim_date, dim_service_type
  Filter: exclude orphan VIN dan tanggal masa depan
*/

{{
    config(materialized='table')
}}

SELECT
    a.service_ticket,
    a.vin,
    a.customer_id,
    a.model,
    a.service_date          AS date_id,
    a.service_type          AS service_type_code,
    a.is_vin_not_in_sales,
    a.is_future_date,
    a.created_at,
    CURRENT_TIMESTAMP()     AS dbt_loaded_at

FROM {{ ref('stg_after_sales') }} a
-- Note: fact table BISA langsung dari staging jika tidak butuh enrichment
-- Dalam case ini, after_sales tidak perlu join complex di intermediate
EOF
```

### 7.4 Buat mart tests

```bash
cat > dbt/models/marts/core/_core__models.yml << 'EOF'
version: 2

models:
  - name: dim_customer
    description: "Customer dimension with latest address. Grain: 1 per customer."
    columns:
      - name: customer_id
        tests: [unique, not_null]

  - name: dim_vehicle
    description: "Vehicle model dimension with price ranges. Grain: 1 per model."
    columns:
      - name: model
        tests: [unique, not_null]

  - name: dim_date
    description: "Date dimension spine 2024-2027. Grain: 1 per date."
    columns:
      - name: date_id
        tests: [unique, not_null]

  - name: dim_service_type
    description: "Service type reference. Grain: 1 per code."
    columns:
      - name: service_type_code
        tests: [unique, not_null]

  - name: fact_sales
    description: >
      Sales fact table. Grain: 1 per VIN (deduplicated).
      Only canonical records included.
    columns:
      - name: vin
        tests: [unique, not_null]
      - name: customer_id
        tests:
          - not_null
          - relationships:
              to: ref('dim_customer')
              field: customer_id
      - name: price
        tests:
          - not_null

  - name: fact_after_sales
    description: "After-sales fact table. Grain: 1 per service ticket."
    columns:
      - name: service_ticket
        tests: [unique, not_null]
EOF
```

### 7.5 Jalankan & Verifikasi

```bash
cd dbt

# Run ALL models (staging + intermediate + marts)
dbt run

# Run ALL tests
dbt test

# Expected output:
# Completed with X passed, 0 errors, 0 warnings
```

### ✅ Checklist Step 7

```
[ ] dbt run → all models passed
[ ] dbt test → all tests passed
[ ] dim_customer: 6 rows (semua customer)
[ ] dim_vehicle: 4 rows (RAIZA, RANGGO, INNAVO, VELOS)
[ ] dim_date: 1461+ rows (date spine)
[ ] dim_service_type: 3 rows (BP, PM, GR)
[ ] fact_sales: 4 rows (deduplicated, canonical only)
[ ] fact_after_sales: 3 rows (semua, termasuk yang di-flag)
```

---

<a name="step-8"></a>
## STEP 8 — dbt Serving Layer (Report Queries = Task 2b)

### 8.1 Kenapa serving layer?

Serving = pre-aggregated tables yang LANGSUNG dikonsumsi BI tools. Ini OBT (One Big Table) yang hanya boleh di serving layer — bukan di mart core.

### 8.2 Report 1: mart_sales_summary

```bash
cat > dbt/models/marts/serving/mart_sales_summary.sql << 'EOF'
/*
  SERVING: mart_sales_summary
  ─────────────────────────────────────────────────────────
  Report requirement:
  | periode (YYYY-MM) | class (LOW/MEDIUM/HIGH) | model | total |

  Grain: 1 baris per (periode, class, model)
  Source: fact_sales

  Best practice:
  - Serving layer = pre-aggregated, BI-ready
  - Langsung dikonsumsi oleh dashboard
  - Materialized as TABLE (not view) untuk performance
*/

{{
    config(materialized='table')
}}

SELECT
    periode,
    price_class                     AS class,
    model,
    SUM(price)                      AS total,
    COUNT(*)                        AS total_transactions,
    CURRENT_TIMESTAMP()             AS dbt_loaded_at

FROM {{ ref('fact_sales') }}
GROUP BY periode, price_class, model
ORDER BY periode, price_class, model
EOF
```

### 8.3 Report 2: mart_aftersales_priority

```bash
cat > dbt/models/marts/serving/mart_aftersales_priority.sql << 'EOF'
/*
  SERVING: mart_aftersales_priority
  ─────────────────────────────────────────────────────────
  Report requirement:
  | periode (YYYY) | vin | customer_name | address | count_service | priority |

  Grain: 1 baris per (year, vin)
  Filter: exclude orphan VIN + tanggal masa depan
  Source: fact_after_sales + dim_customer
*/

{{
    config(materialized='table')
}}

WITH valid_services AS (
    -- Filter: hanya servis yang valid
    SELECT *
    FROM {{ ref('fact_after_sales') }}
    WHERE is_vin_not_in_sales = FALSE    -- VIN harus ada di sales
      AND is_future_date = FALSE          -- Bukan tanggal masa depan
),

aggregated AS (
    SELECT
        YEAR(a.date_id)         AS periode,
        a.vin,
        a.customer_id,
        COUNT(a.service_ticket) AS count_service
    FROM valid_services a
    GROUP BY YEAR(a.date_id), a.vin, a.customer_id
)

SELECT
    agg.periode,
    agg.vin,
    c.customer_name,
    c.full_address                  AS address,
    agg.count_service,
    CASE
        WHEN agg.count_service > 10 THEN 'HIGH'
        WHEN agg.count_service >= 5  THEN 'MED'
        ELSE 'LOW'
    END                             AS priority,
    CURRENT_TIMESTAMP()             AS dbt_loaded_at

FROM aggregated agg
LEFT JOIN {{ ref('dim_customer') }} c USING (customer_id)
ORDER BY agg.periode, agg.count_service DESC
EOF
```

### 8.4 Serving tests

```bash
cat > dbt/models/marts/serving/_serving__models.yml << 'EOF'
version: 2

models:
  - name: mart_sales_summary
    description: >
      Pre-aggregated sales report by period, class, and model.
      Grain: 1 per (periode, class, model).
      Directly consumed by dashboard.
    columns:
      - name: total
        description: "Total nominal penjualan (Rupiah)"
        tests:
          - not_null

  - name: mart_aftersales_priority
    description: >
      After-sales priority report. Excludes orphan VIN and future dates.
      Grain: 1 per (periode, vin).
    columns:
      - name: priority
        tests:
          - accepted_values:
              values: ['HIGH', 'MED', 'LOW']
EOF
```

### 8.5 Jalankan FULL pipeline + Generate docs

```bash
cd dbt

# Full refresh (reset semua table dari awal)
dbt run --full-refresh

# Run ALL tests
dbt test

# Generate documentation + lineage graph
dbt docs generate

# Serve documentation (buka browser)
dbt docs serve --port 8082
# Buka http://localhost:8082 → klik "Lineage Graph" di kanan bawah
```

### 8.6 Verifikasi Report Output

```bash
# Report 1: Sales Summary
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM maju_jaya_mart.mart_sales_summary ORDER BY periode;"

# Expected:
# | periode | class  | model  | total       |
# | 2025-03 | MEDIUM | RAIZA  | 350000000   |
# | 2025-05 | HIGH   | INNAVO | 600000000   |
# | 2025-05 | MEDIUM | RANGGO | 430000000   |
# | 2025-08 | MEDIUM | VELOS  | 390000000   |

# Report 2: After-Sales Priority
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM maju_jaya_mart.mart_aftersales_priority ORDER BY periode;"

# Expected:
# | periode | vin         | customer_name | count | priority |
# | 2025    | MAS8160POE  | Charlie       | 1     | LOW      |
# | 2025    | JLK1368KDE  | Dominikus     | 1     | LOW      |
# Note: POI1059IIK excluded (orphan VIN), T521-oai8 excluded (future date)
```

### ✅ Checklist Step 8

```
[ ] dbt run --full-refresh → all passed
[ ] dbt test → all passed
[ ] dbt docs generate → success
[ ] dbt docs serve → lineage graph visible (raw → stg → int → mart → serving)
[ ] mart_sales_summary: 4 rows, correct class + total
[ ] mart_aftersales_priority: 2 rows (orphan + future excluded)
```

---

<a name="step-9"></a>
## STEP 9 — dbt Custom Tests & Documentation

### 9.1 Custom singular tests

```bash
cat > dbt/tests/assert_revenue_non_negative.sql << 'EOF'
-- Custom test: revenue harus >= 0
-- dbt test: HARUS return 0 rows (jika return rows = FAIL)

SELECT vin, price
FROM {{ ref('fact_sales') }}
WHERE price < 0
EOF
```

```bash
cat > dbt/tests/assert_valid_service_excluded.sql << 'EOF'
-- Custom test: orphan VIN dan future date TIDAK boleh ada di serving layer
-- Harus return 0 rows

SELECT *
FROM {{ ref('mart_aftersales_priority') }} m
WHERE EXISTS (
    SELECT 1 FROM {{ ref('fact_after_sales') }} f
    WHERE f.vin = m.vin
      AND (f.is_vin_not_in_sales = TRUE OR f.is_future_date = TRUE)
)
EOF
```

### 9.2 Jalankan semua tests

```bash
cd dbt
dbt test

# Expected:
# Completed with X passed, 0 errors
```

---

<a name="step-10"></a>
## STEP 10 — Data Quality: Great Expectations

### 10.1 Setup Great Expectations

```bash
cat > data_quality/great_expectations/great_expectations.yml << 'EOF'
config_version: 3.0
datasources:
  mysql_datasource:
    class_name: Datasource
    execution_engine:
      class_name: SqlAlchemyExecutionEngine
      connection_string: "mysql+pymysql://maju_jaya:maju_jaya_pass@localhost:3306/maju_jaya"
    data_connectors:
      default_runtime_data_connector_name:
        class_name: RuntimeDataConnector
        batch_identifiers:
          - default_identifier_name
EOF
```

### 10.2 Create expectation suite

```bash
cat > data_quality/great_expectations/expectations/raw_customers_suite.json << 'EOF'
{
  "expectation_suite_name": "raw_customers_suite",
  "expectations": [
    {
      "expectation_type": "expect_column_to_exist",
      "kwargs": {"column": "id"}
    },
    {
      "expectation_type": "expect_column_values_to_not_be_null",
      "kwargs": {"column": "id"}
    },
    {
      "expectation_type": "expect_column_values_to_be_unique",
      "kwargs": {"column": "id"}
    },
    {
      "expectation_type": "expect_table_row_count_to_be_between",
      "kwargs": {"min_value": 1, "max_value": 10000000}
    }
  ]
}
EOF
```

---

<a name="step-11"></a>
## STEP 11 — Airflow DAG via Docker Compose

### 11.1 Master DAG

```bash
cat > airflow/dags/maju_jaya_pipeline.py << 'PYTHON'
"""
DAG: maju_jaya_pipeline
Schedule: setiap hari jam 06:00 WIB
Flow: Ingest CSV → DQ Raw → Clean Views → dbt staging → dbt test →
      dbt marts → dbt test marts → DQ Mart → Notify

Best practice (dari Olist):
- max_active_runs=1: cegah race condition
- catchup=False: tidak backfill
- retries=2: toleransi transient failures
- Linear dependency: debugging mudah
"""

from __future__ import annotations

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator


# ── CONSTANTS ────────────────────────────────────────────────
DBT_DIR = "/opt/airflow/dbt"


# ── HELPER FUNCTIONS ─────────────────────────────────────────

def run_ingest(**context):
    """Jalankan pipeline ingestion CSV."""
    import sys
    sys.path.insert(0, "/opt/airflow/pipelines")
    from ingest_customer_addresses import run
    ds_nodash = context["ds_nodash"]
    run(date_str=ds_nodash)


def run_clean_views(**context):
    """Buat/update cleaning views."""
    import sys
    sys.path.insert(0, "/opt/airflow/cleaning")
    from clean_tables import run
    run()


# ── DAG DEFINITION ───────────────────────────────────────────

DEFAULT_ARGS = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "start_date": datetime(2026, 3, 1),
    "sla": timedelta(hours=2),
}

with DAG(
    dag_id="maju_jaya_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 6 * * *",   # Jam 06:00 setiap hari
    catchup=False,                    # Tidak backfill
    max_active_runs=1,                # Cegah parallel run
    tags=["maju-jaya", "production"],
    doc_md="""
    ## Maju Jaya Data Pipeline
    Pipeline harian: ingest CSV → clean → dbt transform → quality check.
    **Flow:** Ingest → DQ → Clean → dbt staging → dbt marts → DQ Mart
    """,
) as dag:

    # ── TASK 1: Ingest CSV ───────────────────────────────────
    ingest_csv = PythonOperator(
        task_id="ingest_csv",
        python_callable=run_ingest,
    )

    # ── TASK 2: Create/Update Cleaning Views ─────────────────
    create_clean_views = PythonOperator(
        task_id="create_clean_views",
        python_callable=run_clean_views,
    )

    # ── TASK 3: dbt Run Staging ──────────────────────────────
    dbt_run_staging = BashOperator(
        task_id="dbt_run_staging",
        bash_command=f"cd {DBT_DIR} && dbt run --select staging --profiles-dir .",
    )

    # ── TASK 4: dbt Test Staging ─────────────────────────────
    dbt_test_staging = BashOperator(
        task_id="dbt_test_staging",
        bash_command=f"cd {DBT_DIR} && dbt test --select staging --profiles-dir .",
    )

    # ── TASK 5: dbt Run Intermediate + Marts ─────────────────
    dbt_run_marts = BashOperator(
        task_id="dbt_run_marts",
        bash_command=f"cd {DBT_DIR} && dbt run --select intermediate marts --profiles-dir .",
    )

    # ── TASK 6: dbt Test Marts ───────────────────────────────
    dbt_test_marts = BashOperator(
        task_id="dbt_test_marts",
        bash_command=f"cd {DBT_DIR} && dbt test --select marts --profiles-dir .",
    )

    # ── TASK DEPENDENCIES (linear chain) ─────────────────────
    # Setiap task bergantung pada task sebelumnya
    (
        ingest_csv
        >> create_clean_views
        >> dbt_run_staging
        >> dbt_test_staging
        >> dbt_run_marts
        >> dbt_test_marts
    )
PYTHON
```

### 11.2 dbt profiles untuk Airflow container

Di dalam Airflow container, MySQL host = `mysql` (nama service di docker-compose), bukan `localhost`.

```bash
cat > dbt/profiles_docker.yml << 'EOF'
maju_jaya:
  target: dev
  outputs:
    dev:
      type: mysql
      server: mysql              # <-- Docker service name
      port: 3306
      schema: maju_jaya
      username: maju_jaya
      password: maju_jaya_pass
      charset: utf8mb4
      threads: 4
EOF
```

Update DAG bash commands jika perlu pakai `--profiles-dir` yang menunjuk ke file ini.

### 11.3 Verifikasi Airflow

```bash
# Buka http://localhost:8081
# Login: admin / admin
# Cari DAG: maju_jaya_pipeline
# Enable toggle → Trigger manual (play button)
# Monitor: semua task harus hijau (success)
```

### ✅ Checklist Step 11

```
[ ] Airflow UI accessible di http://localhost:8081
[ ] DAG maju_jaya_pipeline visible
[ ] Manual trigger → all 6 tasks green (success)
[ ] Tidak ada task merah (failed)
```

---

<a name="step-12"></a>
## STEP 12 — Task 3: DWH Design Diagram

Buat diagram arsitektur DWH menggunakan draw.io, Mermaid, atau tool apapun.

### Mermaid ERD (bisa paste ke draw.io atau mermaid.live)

```mermaid
erDiagram
    dim_customer {
        int customer_id PK
        varchar customer_name
        date date_of_birth
        boolean is_dob_suspect
        varchar customer_type
        varchar city
        varchar province
    }
    dim_vehicle {
        varchar model PK
        bigint min_price
        bigint max_price
        varchar default_price_class
    }
    dim_date {
        date date_id PK
        int year
        int month
        int quarter
        varchar month_name
    }
    dim_service_type {
        varchar service_type_code PK
        varchar service_type_name
        varchar description
    }
    fact_sales {
        varchar vin PK
        int customer_id FK
        varchar model FK
        date date_id FK
        bigint price
        varchar price_class
    }
    fact_after_sales {
        varchar service_ticket PK
        varchar vin
        int customer_id FK
        date date_id FK
        varchar service_type_code FK
    }

    dim_customer ||--o{ fact_sales : "has"
    dim_vehicle ||--o{ fact_sales : "sold as"
    dim_date ||--o{ fact_sales : "on date"
    dim_customer ||--o{ fact_after_sales : "services"
    dim_date ||--o{ fact_after_sales : "on date"
    dim_service_type ||--o{ fact_after_sales : "type"
```

---

<a name="step-13"></a>
## STEP 13 — Final Verification & README

### 13.1 End-to-End Test

```bash
# 1. Reset everything
docker compose down -v
docker compose up -d

# 2. Wait for MySQL to be ready (30 sec)
sleep 30

# 3. Run ingestion pipeline
python pipelines/ingest_customer_addresses.py --date 20260301

# 4. Create cleaning views
python cleaning/clean_tables.py

# 5. Run full dbt pipeline
cd dbt
dbt deps
dbt run --full-refresh
dbt test

# 6. Generate docs
dbt docs generate
dbt docs serve --port 8082 &

# 7. Verify results
docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM maju_jaya_mart.mart_sales_summary;"

docker compose exec mysql mysql -u maju_jaya -pmaju_jaya_pass maju_jaya \
  -e "SELECT * FROM maju_jaya_mart.mart_aftersales_priority;"

# 8. Trigger Airflow DAG
# Open http://localhost:8081 → Enable & Trigger maju_jaya_pipeline
```

### 13.2 Final Checklist

```
INFRASTRUCTURE
[ ] docker compose up -d → all services healthy
[ ] MySQL: 5 raw tables + seed data
[ ] Adminer: http://localhost:8080 accessible
[ ] Airflow: http://localhost:8081 accessible

TASK 1 — DATALANDING
[ ] Pipeline loads CSV → MySQL (4 rows)
[ ] Idempotent: second run = SKIP
[ ] Audit log: status=success entry exists

TASK 2a — DATA CLEANING
[ ] 3 cleaning views created
[ ] customers_clean: dob standardized, type flagged
[ ] sales_clean: price as integer, duplicates flagged
[ ] after_sales_clean: orphan VIN + future date flagged

TASK 2b — REPORT QUERIES (via dbt serving)
[ ] mart_sales_summary: 4 rows, correct class categorization
[ ] mart_aftersales_priority: 2 rows (excluded correctly)

TASK 3 — DWH DESIGN
[ ] 4-layer architecture documented
[ ] Star schema: 4 dim + 2 fact tables
[ ] ERD diagram created
[ ] Data lineage visible via dbt docs

dbt PIPELINE
[ ] dbt run --full-refresh → all passed
[ ] dbt test → all passed (including custom tests)
[ ] dbt docs serve → lineage graph visible
[ ] Staging → Intermediate → Mart → Serving flow correct

AIRFLOW
[ ] DAG maju_jaya_pipeline trigger manual → all green
[ ] Linear task dependency working

BONUS
[ ] All code on GitHub
[ ] README with architecture diagram + quick start
[ ] dbt lineage screenshot saved
```

---

## 📚 Makefile (Shortcut Commands)

```bash
cat > Makefile << 'EOF'
.PHONY: up down reset ingest clean dbt-run dbt-test dbt-docs

up:
	docker compose up -d

down:
	docker compose down

reset:
	docker compose down -v
	docker compose up -d
	sleep 30

ingest:
	python pipelines/ingest_customer_addresses.py

clean:
	python cleaning/clean_tables.py

dbt-run:
	cd dbt && dbt run --full-refresh

dbt-test:
	cd dbt && dbt test

dbt-docs:
	cd dbt && dbt docs generate && dbt docs serve --port 8082

all: ingest clean dbt-run dbt-test
	@echo "✅ Full pipeline completed!"
EOF
```

Sekarang kamu bisa jalankan `make all` untuk full pipeline.

---

*Built following Olist Customer Intelligence Data Platform best practices.*
*Stack: Python · MySQL · Docker Compose · dbt · Airflow · Great Expectations · SQL*
