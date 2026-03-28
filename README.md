# Maju Jaya Data Platform

End-to-end data engineering platform untuk perusahaan retail otomotif **Maju Jaya**. Proyek ini membangun pipeline otomatis yang mengkonsolidasikan data dari MySQL dan Excel (Google Drive) ke BigQuery, menerapkan 4-layer transformation dengan dbt, dan menghasilkan report bisnis secara daily via Airflow.

**Tech Stack:** Python · Terraform · Docker · Apache Airflow · Google Cloud Storage · BigQuery · dbt · Metabase

---
![alt text](<Layering Data Maju Jaya Motors.png>)
## Table of Contents

- [Latar Belakang](#latar-belakang)
- [Arsitektur](#arsitektur)
- [Kenapa Arsitektur Ini](#kenapa-arsitektur-ini)
- [Alur Data Layer-by-Layer](#alur-data-layer-by-layer)
- [Star Schema Design](#star-schema-design)
- [Data Quality](#data-quality)
- [Airflow Pipeline](#airflow-pipeline)
- [Infrastruktur](#infrastruktur)
- [Cara Menjalankan](#cara-menjalankan)
- [Project Structure](#project-structure)
- [dbt Lineage Graph](#dbt-lineage-graph)
- [Keputusan Arsitektur](#keputusan-arsitektur)
- [Apa yang Bisa Dipelajari dari Project Ini](#apa-yang-bisa-dipelajari-dari-project-ini)
- [Referensi](#referensi)

---

## Latar Belakang

Maju Jaya adalah perusahaan retail otomotif yang menjual kendaraan (RAIZA, RANGGO, INNAVO, VELOS) dan menyediakan layanan after-sales (Body Paint, Periodic Maintenance, General Repair).

Saat ini data tersebar di dua tempat:

- **MySQL** — database transaksional yang menyimpan data customer (6 records), penjualan (5 records), dan servis kendaraan (3 records). Data ini "kotor": tanggal lahir dalam 3 format berbeda, harga sebagai string, dan ada record duplicate suspect.

- **Excel di Google Drive** — file `customer_addresses_yyyymmdd.xlsx` yang diupdate harian. Berisi alamat customer (4 records) dengan casing yang tidak konsisten (JAKARTA PUSAT vs Jakarta Utara).

Reporting dilakukan manual — rawan error, tidak bisa di-audit, dan tidak scalable. Management butuh 2 report: ringkasan penjualan per periode/kelas/model, dan prioritas customer berdasarkan frekuensi servis.

Platform ini membangun seluruh pipeline dari nol: extract data dari kedua source, load ke BigQuery, transform lewat 4 layer dbt, validate dengan tests, dan orchestrate daily dengan Airflow. Hasilnya: 2 report yang bisa diakses langsung dari BI tool, berjalan otomatis setiap hari, dan setiap transformasi bisa di-trace lewat dbt lineage graph.

---

## Arsitektur

```
MySQL (OLTP)                          Excel (Google Drive)
3 tables                              daily .xlsx files
     │                                      │
     │ Python                               │ Python
     │ pandas + sqlalchemy                  │ openpyxl + google-api
     │                                      │
     │ ┌─────────────────┐                  │ ┌──────────────────────┐
     │ │ Direct to BQ    │                  │ │ GCS bucket staging   │
     │ │ (Option C)      │                  │ │ (Parquet format)     │
     │ │ No GCS needed   │                  │ │ Free BQ batch load   │
     │ └────────┬────────┘                  │ └──────────┬───────────┘
     │          │                           │            │
     ▼          ▼                           ▼            ▼
┌─────────────────────────────────────────────────────────────────┐
│                        BigQuery                                  │
│                                                                  │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ raw_maju (native tables)                                 │    │
│  │ customers │ sales │ after_sales │ customer_addresses     │    │
│  └──────────────────────────┬──────────────────────────────┘    │
│                              │                                   │
│  ════════════ BOUNDARY ══════╪═══════════════════════════════    │
│  Di atas : Python + Airflow  │  (Extract + Load)                │
│  Di bawah: dbt               │  (Transform)                     │
│                              ▼                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ stg_maju (VIEWS — $0 storage, selalu fresh)             │    │
│  │ stg_customers │ stg_sales │ stg_after_sales │ stg_addr  │    │
│  └──────────────────────────┬──────────────────────────────┘    │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ intermediate (EPHEMERAL — CTE, tidak persist)            │    │
│  │ int_customer_enriched │ int_sales_enriched               │    │
│  └──────────────────────────┬──────────────────────────────┘    │
│                              │                                   │
│  ┌─────────────────────────────────────────────────────────┐    │
│  │ mart_maju (TABLES — star schema, partitioned+clustered) │    │
│  │ dim_customer │ dim_vehicle │ dim_date │ dim_service_type │    │
│  │ fact_sales (partitioned) │ fact_after_sales              │    │
│  │ mart_sales_summary │ mart_aftersales_priority            │    │
│  └─────────────────────────────────────────────────────────┘    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
         │
         ▼
    Dashboard / BI tool (Metabase)
```

---

## Kenapa Arsitektur Ini

Tiga keputusan yang membedakan platform ini dari "default" tutorial:

### 1. MySQL langsung ke BigQuery, tanpa lewat GCS

Kebanyakan tutorial mengajarkan: source → GCS (data lake) → BigQuery. Tapi untuk database source yang sudah structured, GCS tidak menambah value:

- BigQuery storage = **$0.020/GB/month**
- GCS Standard = **$0.020/GB/month**
- Harga **sama**. GCS hanya menambah satu hop yang bisa gagal.

Google Cloud sendiri menyediakan Datastream for BigQuery yang langsung MySQL → BigQuery. Rittman Analytics, Eagle AI, dan pattern dari Juan Ramos (Towards Analytics Engineering) semuanya load database sources langsung ke BigQuery.

Script: `scripts/extract_mysql_to_bigquery.py` — menggunakan `bq.load_table_from_dataframe()`, WRITE_TRUNCATE (idempotent).

### 2. Excel lewat GCS dulu, baru ke BigQuery

Berbeda dengan MySQL, file-based sources memang sebaiknya lewat GCS karena:

- BigQuery batch load dari GCS = **GRATIS** ($0 ingestion cost). Ini official Google pricing.
- File Parquet asli tetap tersimpan di GCS sebagai **backup** kalau perlu replay.
- Parquet = columnar compression. Excel 10MB → Parquet ~2MB.
- Hive-style partitioning: `gs://bucket/raw/excel/addresses/ingestion_date=2026-03-28/addresses.parquet`

Flow: Google Drive → `download_from_gdrive.py` → local → `extract_excel_to_gcs.py` → GCS → `load_gcs_to_bigquery.py` → BigQuery.

### 3. BigQuery sebagai single source of truth, bukan GCS

Google Cloud Lakehouse whitepaper (2024) menyatakan: "Without a cost premium on BigQuery storage vs blob storage, there is no longer a required cost-based justification for separate storage."

GCS hanya diperlukan untuk:
- Data unstructured (gambar, video, PDF)
- Volume >10TB rarely-queried (Coldline 5x lebih murah)
- Multi-engine processing (Spark, Vertex AI)
- Regulatory compliance (immutable file locks)

Untuk project ini (structured data, <1GB), BigQuery sudah cukup sebagai warehouse sekaligus "lake."

---

## Alur Data Layer-by-Layer

Setiap layer punya **satu tanggung jawab**. Kalau kamu campur (misal join + aggregate di staging), debugging jadi nightmare karena kamu tidak tahu masalah di langkah mana. Bayangkan seperti dapur restoran: cuci bahan, masak, plating — setiap station punya tugas sendiri.

### Layer 0: Raw — data mentah dari source

Diisi oleh Python/Airflow di Fase 2. dbt **tidak menyentuh** layer ini. dbt hanya mendeklarasikan raw tables sebagai `source()` di YAML, lalu membaca.

Kenapa dbt tidak buat raw tables? Karena dbt = **T** (Transform) dalam ELT. dbt Labs sendiri bilang: "dbt does not extract or load data." Boundary yang jelas: kalau ingestion gagal → debug Python. Kalau transform gagal → debug dbt. Tidak campur.

```
raw_maju.customers          6 rows   DOB 3 format, NULL, placeholder 1900-01-01
raw_maju.sales              5 rows   price as string "350.000.000", duplicate suspect
raw_maju.after_sales        3 rows   orphan VIN (POI1059IIK), future date (2026-09-10)
raw_maju.customer_addresses 4 rows   city casing inconsistent (JAKARTA PUSAT vs Jakarta Utara)
```

### Layer 1: Staging — cuci bahan mentah (dbt VIEWS)

**Aturan keras:**
- 1 source = 1 staging model. Tidak boleh 2 source di 1 model.
- HANYA rename, cast, filter null, flag masalah.
- TIDAK ADA join, aggregate, atau business logic.
- Materialized = **VIEW** → $0 storage, selalu fresh.

Kenapa VIEW? dbt Labs merekomendasikan: "establishing a staging layer mainly composed of views, not tables." Views tidak menyimpan data, hanya query definition. Kalau raw berubah, staging otomatis reflect tanpa re-run.

| Model | Apa yang dilakukan | Contoh |
|-------|-------------------|--------|
| `stg_customers` | DOB 3 format → 1 DATE. Flag `is_dob_suspect`, classify `customer_type` | `'14/01/1995'` → `DATE 1995-01-14` |
| `stg_sales` | Price string → INT64. Detect duplicate suspects via window function | `'350.000.000'` → `350000000` |
| `stg_after_sales` | Flag orphan VIN dan future dates | `POI1059IIK` → `is_vin_not_in_sales = TRUE` |
| `stg_customer_addresses` | Latest address per customer (ROW_NUMBER dedup). INITCAP casing | `JAKARTA PUSAT` → `Jakarta Pusat` |

**Prinsip penting: flag, bukan delete.** Data asli tidak dibuang. is_dob_suspect, is_duplicate_suspect, is_vin_not_in_sales — semua boolean flag. Analyst bisa decide sendiri mau filter atau include. Dashboard bisa tunjukkan "X% data punya masalah." Serving layer yang filter; raw dan staging preserve semuanya.

### Layer 2: Intermediate — masak setengah jadi (dbt EPHEMERAL)

Tempat **join** dan **business logic**. Materialized sebagai ephemeral = CTE (Common Table Expression) yang dikompilasi di dalam query mart. Tidak persist ke BigQuery. $0 storage.

Kenapa intermediate ada? Tanpa intermediate:
- `dim_customer` butuh JOIN customer + address → tulis JOIN
- `mart_aftersales_priority` juga butuh customer name + address → akses lewat dim_customer
- `fact_sales` butuh price_class + dedup → tulis CASE WHEN + ROW_NUMBER
- `dim_vehicle` juga butuh price_class dari sales → tulis lagi

Dengan intermediate, logic ditulis **sekali**. Semua mart tinggal `ref()`. Kalau logic berubah (misalnya range harga LOW dari 100-250jt jadi 100-200jt), update 1 file → semua downstream konsisten.

| Model | Input | Output | Consumers |
|-------|-------|--------|-----------|
| `int_customer_enriched` | stg_customers + stg_addresses | Customer + latest address + full_address | dim_customer (1 consumer, tapi forward-looking: mart baru tinggal `ref()` ke sini) |
| `int_sales_enriched` | stg_sales + stg_customers | Sales + customer info + price_class (LOW/MEDIUM/HIGH) + dedup (is_canonical) + periode (YYYY-MM) | fact_sales, dim_vehicle (2 consumers) |

**Detail business logic di `int_sales_enriched`:**

Price classification (dari requirement soal):
```
100jt - 250jt  → LOW
250jt - 400jt  → MEDIUM
> 400jt        → HIGH
```

Deduplication strategy:
- Staging: **FLAG** duplicates (`is_duplicate_suspect = TRUE`)
- Intermediate: **DECIDE** — ROW_NUMBER by `(customer_id, model, invoice_date)`, ORDER BY `created_at ASC`
- `is_canonical = TRUE` → record yang "menang" (pertama masuk)
- `is_canonical = FALSE` → record yang "kalah" (tetap ada untuk audit)
- Fact table nanti filter `WHERE is_canonical = TRUE`

Kenapa ROW_NUMBER bukan DISTINCT? Row 4 dan 5 di sales_raw **tidak identik** — VIN berbeda (JLK1869KDF vs JLK1962KOP). DISTINCT tidak bisa handle ini. ROW_NUMBER pilih 1 winner berdasarkan logika explicit.

### Layer 3: Marts — makanan siap saji (dbt TABLES)

Layer yang langsung dikonsumsi BI tool dan analyst. Materialized sebagai **TABLE** karena:
- BI tools query mart ratusan kali/hari. TABLE = pre-computed, instant.
- BigQuery TABLE bisa di-**partition** by date (scan hanya bulan yang diquery).
- BigQuery TABLE bisa di-**cluster** by column (skip data blocks yang tidak relevan).

Marts dibagi dua subfolder: **core** (star schema) dan **serving** (pre-aggregated reports).

---

## Star Schema Design

Star schema dipilih karena BI tools (Metabase, Looker, Tableau) optimal dengan pattern ini: query hanya butuh 1 JOIN (fact → dim), bukan 5+ JOIN normalisasi. Analyst bisa drag-and-drop tanpa pahami relasi kompleks.

```
                          ┌─────────────────┐
                          │  dim_customer    │
                          │  PK: customer_id │
                          │  name, type,     │
                          │  city, address   │
                          └────────┬────────┘
                                   │
┌─────────────────┐  ┌─────────────┴────────────┐  ┌────────────────────┐
│  dim_vehicle    │  │     fact_sales            │  │ fact_after_sales   │
│  PK: model      │──│     PK: vin              │  │ PK: service_ticket │
│  min/max/avg    │  │     FK: customer_id       │  │ FK: customer_id    │
│  price_class    │  │     FK: model, date       │  │ FK: date, type     │
└─────────────────┘  │     price, price_class    │  │ is_vin_not_in_sales│
                     │     periode               │  │ is_future_date     │
┌─────────────────┐  └──────────────────────────┘  └─────────┬──────────┘
│  dim_date       │                                           │
│  PK: date_id    │──── generated 2024-2027 ─────────────────│
│  year, month,   │                                           │
│  quarter, name  │                              ┌────────────┴──────────┐
│  is_weekend     │                              │ dim_service_type      │
└─────────────────┘                              │ PK: service_type_code │
                                                 │ BP, PM, GR            │
                                                 └───────────────────────┘
```

### Dimensions (4)

| Dimension | Grain | Penjelasan |
|-----------|-------|------------|
| `dim_customer` | 1 per customer | Dari `int_customer_enriched`. Termasuk latest address (dari Excel daily). |
| `dim_vehicle` | 1 per model | **Derived** dari sales data (bukan master table — soal tidak kasih). Min/max/avg price. |
| `dim_date` | 1 per tanggal | **Generated** pakai `GENERATE_DATE_ARRAY`. Kenapa generate, bukan derive dari data? Karena hari tanpa penjualan harus tetap ada di dimension. |
| `dim_service_type` | 1 per kode | **Static seed** (BP/PM/GR). Soal tidak kasih master table, jadi di-hardcode. Di production bisa jadi dbt seed CSV. |

### Facts (2)

| Fact | Grain | BigQuery optimization |
|------|-------|-----------------------|
| `fact_sales` | 1 per VIN (deduplicated via `is_canonical`) | **Partitioned** by `invoice_date` (monthly) — query bulan tertentu scan 1/12 data. **Clustered** by `customer_id + price_class` — ~30-50% cost reduction tambahan. |
| `fact_after_sales` | 1 per service ticket | DQ flags preserved (`is_vin_not_in_sales`, `is_future_date`) — fact = complete data untuk audit. |

### Serving reports (2)

| Report | Grain | Menjawab pertanyaan bisnis |
|--------|-------|---------------------------|
| `mart_sales_summary` | 1 per (periode, class, model) | "Berapa total penjualan RAIZA class HIGH di bulan Mei 2025?" |
| `mart_aftersales_priority` | 1 per (year, vin) | "Customer mana yang sering servis dan perlu perhatian khusus?" |

Kenapa serving terpisah dari fact? Fact = grain per transaksi. Report = grain per aggregation. Kalau BI tool harus GROUP BY setiap query → lambat dan bisa inconsistent antar dashboard. Serving = pre-aggregated TABLE. BI tool cuma `SELECT *`.

`mart_aftersales_priority` meng-**exclude** orphan VIN dan future dates (`WHERE is_vin_not_in_sales = FALSE AND is_future_date = FALSE`). Fact table preserve semuanya; serving layer yang filter. Separation of concerns: fact preserve, serving present.

---

## Data Quality

### 6 masalah data yang ditangani

| # | Masalah | Tabel | Di mana ditangani | Cara |
|---|---------|-------|-------------------|------|
| 1 | DOB dalam 3 format berbeda | customers | stg_customers | `CASE WHEN REGEXP_CONTAINS` + `SAFE.PARSE_DATE` per format |
| 2 | DOB placeholder 1900-01-01 | customers | stg_customers | Flag `is_dob_suspect = TRUE` |
| 3 | Corporate entity tanpa DOB | customers | stg_customers | Classify `customer_type = 'corporate'` (heuristic: prefix PT/CV/UD atau DOB NULL) |
| 4 | Price sebagai string "350.000.000" | sales | stg_sales | `REPLACE('.','')` hapus titik → `SAFE_CAST AS INT64` |
| 5 | Suspect duplicate (same customer+model+date, beda VIN) | sales | stg_sales (flag) → int_sales_enriched (dedup) | `COUNT(*) OVER()` untuk flag, `ROW_NUMBER()` untuk pick winner |
| 6 | Orphan VIN + future service date | after_sales | stg_after_sales (flag) → mart_aftersales_priority (exclude) | `LEFT JOIN sales` untuk orphan check, `> CURRENT_DATE()` untuk future |

### dbt tests (26 total)

**Built-in tests (24 deklarasi di YAML):**
- `unique` + `not_null` pada semua primary key (customer_id, vin, service_ticket, date_id, model, service_type_code)
- `accepted_values` pada customer_type, price_class, service_type_code, priority
- `relationships` — FK integrity: `fact_sales.customer_id` harus ada di `dim_customer.customer_id`

**Custom SQL tests (2 file):**
- `assert_revenue_non_negative` — semua `price` di fact_sales harus >= 0
- `assert_serving_excludes_invalid` — orphan VIN dan future dates tidak boleh muncul di serving layer

Convention dbt: custom test query harus return **0 rows** untuk pass. Kalau return rows = test GAGAL.

---

## Airflow Pipeline

### DAG: `maju_jaya_pipeline`

Daily jam 06:00 WIB. 15 tasks. 4 sensor gates. 3 phases.

```
Phase 1 — INGESTION (parallel)
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
start ─┬─► extract_mysql_to_bq ───────────────────────────────┐
       │                                                       │
       └─► download_excel_from_gdrive                          │
              └─► extract_excel_to_gcs                         │
                    └─► SENSOR: gcs_parquet_exists              │
                          └─► load_excel_gcs_to_bq ────────────┤
                                                                │
           GATE 1: SENSOR raw_layer_ready (4 tables) ◄─────────┘

Phase 2 — DBT STAGING
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
           dbt_deps
              └─► dbt_run_staging (4 views)
                    └─► GATE 2: SENSOR staging_layer_ready
                          └─► dbt_test_staging

Phase 3 — DBT MARTS
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
                          dbt_run_marts (intermediate + marts)
                             └─► GATE 3: SENSOR mart_layer_ready
                                   └─► dbt_test_marts
                                         └─► pipeline_complete ✓
```

### 4 sensor gates — kenapa dan bagaimana

| Gate | Checks | Kenapa ada |
|------|--------|------------|
| `sensor_gcs_parquet_exists` | File Parquet sudah di GCS | Batch load butuh file dulu. Tanpa sensor: load job gagal dengan "file not found" |
| `sensor_raw_layer_ready` | 4 raw BQ tables punya data | dbt butuh data untuk transform. Tanpa sensor: staging views query empty tables → silent wrong results |
| `sensor_staging_layer_ready` | 4 staging views queryable | Mart models `ref()` ke staging. Tanpa sensor: mart error "view not found" |
| `sensor_mart_layer_ready` | fact + serving tables exist | dbt test butuh materialized table. Tanpa sensor: test gagal "table not found" |

Semua sensor pakai `PythonSensor` dengan `mode="reschedule"`. Kenapa mode ini? Sensor tanpa reschedule **menahan worker slot** selama sleep. Kalau 4 sensor sleep bersamaan = 4 workers terpakai = worker pool habis = pipeline deadlock. Dengan reschedule, sensor **melepas** worker slot antara checks.

### Design patterns

| Pattern | Implementasi | Benefit |
|---------|-------------|---------|
| Exponential backoff | `retry_delay=2min, retry_exponential_backoff=True` | 2min → 4min → 8min. Transient failures (network, API quota) recover otomatis |
| `max_active_runs=1` | Hanya 1 DAG run paralel | Cegah race condition: 2 runs menulis ke table yang sama |
| SLA 2h | Alert jika pipeline kelamaan | Monitoring: kalau biasanya 15 menit tapi hari ini 2 jam → something wrong |
| `on_failure_callback` | Structured error logging | Debug: DAG apa, task apa, tanggal apa, log URL |

### Modular code structure

DAG file bukan satu file giant 200+ baris. Dipisah by concern:

```
airflow/dags/
  maju_jaya_pipeline.py        ← DAG wiring only (import + define tasks + set dependencies)
  test_connection.py            ← environment verification DAG
  common/
    config.py                   ← semua konstanta: project ID, datasets, sensor settings
    sensors.py                  ← 3 reusable sensor functions (BQ table check, GCS file check, multi-table check)
    tasks_ingestion.py          ← 4 ingestion callables (wraps scripts dari Fase 2)
    callbacks.py                ← on_failure + on_success hooks
```

Kenapa dipisah? Kalau sensor logic berubah → edit `sensors.py`, bukan cari di tengah DAG file. Kalau GCP project ID berubah → edit `config.py`, bukan find-replace di 10 tempat. DAG file sendiri bisa dibaca dalam 30 detik — hanya imports, task definitions, dan dependency wiring.

---

## Infrastruktur

### Terraform resources

Satu `terraform apply` dan semuanya siap. Reproducible, version-controlled.

| Resource | Nama | Tujuan |
|----------|------|--------|
| GCS bucket | `maju-jaya-raw-dev` | File staging (Excel → Parquet). Auto-move ke Nearline setelah 90 hari. |
| BigQuery dataset | `raw_maju` | Raw tables — ingestion target. Label: `layer=raw` |
| BigQuery dataset | `stg_maju` | Staging views — dbt output. Label: `layer=staging` |
| BigQuery dataset | `mart_maju` | Mart tables + serving — dbt output. Label: `layer=mart` |
| Service account | `sa-pipeline-dev` | Pipeline runner. Roles: `bigquery.dataEditor`, `bigquery.user`, `storage.objectAdmin` |

### Docker services (6 containers)

| Container | Image | Port | Fungsi |
|-----------|-------|------|--------|
| `postgres` | postgres:15-alpine | 5432 | Airflow metadata DB |
| `mysql-source` | mysql:8.0 | 3306 | OLTP simulation (seed data otomatis via `01_init.sql`) |
| `adminer` | adminer:4.8.1 | 8080 | MySQL GUI untuk browse data |
| `airflow-init` | custom | — | One-time: `airflow db migrate` + create admin user |
| `airflow-web` | custom | 8081 | Airflow UI |
| `airflow-scheduler` | custom | — | DAG scheduler |

**3 design decisions yang mencegah Docker errors:**

1. **Custom Dockerfile** — semua pip packages (18) di-install saat `docker compose build`, bukan saat runtime. Tanpa ini: Airflow boot sebelum pip install selesai → `ModuleNotFoundError` → DAG tidak muncul di UI.

2. **PostgreSQL sebagai Airflow backend** — bukan MySQL. MySQL sebagai Airflow metadata sering error: lock contention pada scheduler tables, utf8mb4 charset migration issues. PostgreSQL proven stable di production Airflow.

3. **`service_completed_successfully` dependency** — `airflow-web` dan `airflow-scheduler` TIDAK start sampai `airflow-init` selesai. Tanpa ini: webserver boot sebelum DB migrate → crash → restart loop.

---

## Cara Menjalankan

### Prerequisites

```bash
python3 --version    # >= 3.11
docker --version     # >= 24.0
gcloud --version     # >= 450.0
terraform --version  # >= 1.6
```

### Step-by-step

```bash
# ── 1. GCP project ──────────────────────────────────────────
gcloud auth login
gcloud config set project maju-jaya-platform
gcloud services enable bigquery.googleapis.com storage.googleapis.com iam.googleapis.com

# ── 2. Terraform (GCS + BigQuery + IAM) ─────────────────────
cd terraform && terraform init && terraform apply && cd ..

# Service account key
gcloud iam service-accounts keys create credentials/sa-pipeline-dev.json \
  --iam-account=sa-pipeline-dev@maju-jaya-platform.iam.gserviceaccount.com

# ── 3. Docker (MySQL + Airflow) ─────────────────────────────
docker compose build        # ~3 menit pertama kali
docker compose up -d
sleep 30                    # tunggu MySQL seed data
make mysql-check            # → customers_raw: 6, sales_raw: 5, after_sales_raw: 3

# ── 4. Verify environment ───────────────────────────────────
# Adminer: http://localhost:8080 (server: mysql-source, user: maju_jaya)
# Airflow: http://localhost:8081 (admin/admin) → trigger test_connection DAG → all green

# ── 5. Ingestion ────────────────────────────────────────────
python scripts/extract_mysql_to_bigquery.py     # MySQL → BigQuery direct
python scripts/extract_excel_to_gcs.py          # Excel → GCS (Parquet)
python scripts/load_gcs_to_bigquery.py          # GCS → BigQuery (free)

# ── 6. dbt ──────────────────────────────────────────────────
cd dbt
dbt deps                     # install dbt_utils
dbt debug                    # "All checks passed!"
dbt run --full-refresh       # staging views + mart tables created
dbt test                     # 26 tests passed
dbt docs generate            # lineage graph
dbt docs serve --port 8082   # open http://localhost:8082 → screenshot lineage!

# ── 7. Production pipeline ──────────────────────────────────
# Airflow UI → enable maju_jaya_pipeline → trigger → all 15 tasks green
```

### Google Drive setup (untuk Excel source)

```bash
# 1. Enable Drive API
gcloud services enable drive.googleapis.com

# 2. Share folder ke service account
#    Google Drive → folder → Share
#    → sa-pipeline-dev@maju-jaya-platform.iam.gserviceaccount.com (Viewer)

# 3. Set folder ID di .env
#    URL: https://drive.google.com/drive/folders/ABC123...
#    .env: GDRIVE_FOLDER_ID=ABC123...
```

### Services

| Service | URL | Login |
|---------|-----|-------|
| Adminer (MySQL) | http://localhost:8080 | server: mysql-source, user: maju_jaya |
| Airflow | http://localhost:8081 | admin / admin |
| dbt docs | http://localhost:8082 | — |

---

## Project Structure

```
maju-jaya-data-platform/
│
│   ── FASE 1: Foundation ─────────────────────────────────────
│
├── terraform/
│   ├── main.tf                            GCS + 3 BigQuery datasets + SA + IAM (with outputs)
│   └── variables.tf                       Configurable defaults (project_id, region, bucket)
│
├── docker-compose.yml                     6 services with health checks + init dependency
├── Dockerfile.airflow                     Custom image: packages pre-installed at build time
├── requirements.txt                       Local dev: 19 packages (Airflow NOT here — Docker only)
├── requirements-docker.txt                Airflow container: 18 packages
├── Makefile                               Shortcut commands (build, up, reset, mysql-check, dbt-debug)
├── .env                                   Config (GCP project, MySQL creds, GDrive folder ID)
├── .gitignore                             Protects: credentials/, .env, *.json, .terraform/
│
│   ── FASE 2: Ingestion ──────────────────────────────────────
│
├── scripts/
│   ├── 01_init.sql                        MySQL seed data (persis dari soal: 6+5+3 rows)
│   ├── download_from_gdrive.py            Google Drive API → local data/excel/ (idempotent)
│   ├── extract_mysql_to_bigquery.py       MySQL → BigQuery direct, WRITE_TRUNCATE
│   ├── extract_excel_to_gcs.py            Excel → Parquet → GCS (supports --from-drive)
│   ├── load_gcs_to_bigquery.py            GCS → BigQuery free batch load
│   └── verify_all.sh                      Pre-push verification (52 checks)
│
├── pipelines/
│   └── ingest_customer_addresses.py       Task 1: daily Excel → MySQL (idempotent + audit trail)
│
├── cleaning/
│   └── clean_tables.py                    Task 2a: 3 MySQL cleaning views (flag, don't delete)
│
├── data/excel/
│   └── customer_addresses_20260301.xlsx   Sample data (4 rows, persis dari soal)
│
│   ── FASE 3: Transformation (dbt) ───────────────────────────
│
├── dbt/
│   ├── dbt_project.yml                    Materialization config: view/ephemeral/table per layer
│   ├── profiles.yml                       BigQuery connection (service account)
│   ├── packages.yml                       dbt_utils dependency
│   │
│   ├── models/staging/                    LAYER 1 — clean, cast, flag (4 VIEWS)
│   │   ├── _staging__sources.yml              source() declarations + raw data tests
│   │   ├── _staging__models.yml               model documentation + tests
│   │   ├── stg_customers.sql                  DOB 3 formats → 1 DATE, customer_type
│   │   ├── stg_sales.sql                      price string → INT64, duplicate detection
│   │   ├── stg_after_sales.sql                orphan VIN flag, future date flag
│   │   └── stg_customer_addresses.sql         ROW_NUMBER dedup, INITCAP casing
│   │
│   ├── models/intermediate/               LAYER 2 — join, business logic (2 EPHEMERAL)
│   │   ├── _intermediate__models.yml          model documentation + tests
│   │   ├── int_customer_enriched.sql          customer + latest address → full_address
│   │   └── int_sales_enriched.sql             price class, dedup, periode, customer info
│   │
│   ├── models/marts/
│   │   ├── core/                          LAYER 3a — star schema (4 DIM + 2 FACT TABLES)
│   │   │   ├── _core__models.yml              documentation + tests + relationship checks
│   │   │   ├── dim_customer.sql               from int_customer_enriched
│   │   │   ├── dim_vehicle.sql                derived from sales (no master table)
│   │   │   ├── dim_date.sql                   generated 2024-2027 (GENERATE_DATE_ARRAY)
│   │   │   ├── dim_service_type.sql           static seed BP/PM/GR
│   │   │   ├── fact_sales.sql                 partitioned + clustered, deduplicated
│   │   │   └── fact_after_sales.sql           DQ flags preserved for audit
│   │   │
│   │   └── serving/                       LAYER 3b — pre-aggregated reports (2 TABLES)
│   │       ├── _serving__models.yml           documentation + tests
│   │       ├── mart_sales_summary.sql         Task 2b Report 1 (periode, class, model, total)
│   │       └── mart_aftersales_priority.sql   Task 2b Report 2 (priority HIGH/MED/LOW)
│   │
│   └── tests/                             CUSTOM SQL ASSERTIONS (2)
│       ├── assert_revenue_non_negative.sql    price >= 0 in fact_sales
│       └── assert_serving_excludes_invalid.sql  no orphan/future in serving
│
│   ── FASE 4: Orchestration ──────────────────────────────────
│
├── airflow/dags/
│   ├── maju_jaya_pipeline.py              Production DAG: 15 tasks, 4 sensors (wiring only)
│   ├── test_connection.py                 Verify: packages, MySQL, dbt debug
│   └── common/                            Modular shared modules
│       ├── __init__.py
│       ├── config.py                          All constants: GCP, datasets, sensor settings
│       ├── sensors.py                         3 reusable callables (BQ table, GCS file, multi-table)
│       ├── tasks_ingestion.py                 4 ingestion wrappers (MySQL, GDrive, Excel, GCS→BQ)
│       └── callbacks.py                       on_failure + on_success hooks
│
│   ── FASE 5: Documentation ──────────────────────────────────
│
├── docs/architecture/
│   ├── ARCHITECTURE_DECISION.md           10 decisions with industry references
│   └── DATA_DICTIONARY.md                 Column-level definitions per layer
│
└── README.md                              This file
```

**52 files total:** 13 Python · 17 SQL (1011 lines) · 9 YAML · 3 Markdown · 10 config/other

---

## dbt Lineage Graph

```
                    ┌─────────────────── raw_maju (source, not dbt) ───────────────────┐
                    │                                                                    │
          ┌─────────┼──────────┬────────────────┐                                       │
          ▼         ▼          ▼                ▼                                       │
   stg_customers  stg_sales  stg_after_sales  stg_customer_addresses                   │
          │         │          │                │                                       │
          │         │          │                │         ┌─────────────────────────────┘
          ▼         ▼          │                ▼         │
   int_customer_enriched   int_sales_enriched  │         │
          │                    │          │     │         │
          ▼                    ▼          ▼     ▼         │
   dim_customer         fact_sales   dim_vehicle          │
          │                │                              │
          │                ▼                              │
          │         mart_sales_summary                    │
          │                                               │
          ▼                                               ▼
   mart_aftersales_priority  ◄──────────────── fact_after_sales
```

Chain lengkap: `raw` → `stg` (views) → `int` (ephemeral CTE) → `mart` (tables) → `serving` (tables).

Untuk melihat lineage interaktif dengan clickable nodes:
```bash
cd dbt && dbt docs generate && dbt docs serve --port 8082
```

---

## Keputusan Arsitektur

| # | Keputusan | Pilihan | Alasan singkat |
|---|-----------|---------|----------------|
| 1 | MySQL → ? | BigQuery direct | BQ storage = GCS price. Tidak ada benefit lewat GCS untuk structured data. |
| 2 | Excel → ? | GCS → BigQuery | Batch load gratis. File backup di GCS. |
| 3 | Raw layer by? | Python/Airflow | dbt = Transform only. Boundary jelas. |
| 4 | Staging mat? | VIEW | $0 storage, selalu fresh. dbt Labs recommendation. |
| 5 | Intermediate mat? | EPHEMERAL | CTE only. Logic terpisah tapi tidak persist. |
| 6 | Marts mat? | TABLE | Fast reads. Partitioned by date, clustered by filter cols. |
| 7 | GCS role? | File staging only | BigQuery = single source of truth. |
| 8 | Airflow backend? | PostgreSQL | MySQL lock contention issues. |
| 9 | DAG structure? | Modular common/ | Testable, DRY. DAG file = wiring only. |
| 10 | Data quality? | dbt tests (26) | Built-in + custom SQL. Sufficient tanpa Great Expectations. |

Detail lengkap dengan referensi industry: **[docs/architecture/ARCHITECTURE_DECISION.md](docs/architecture/ARCHITECTURE_DECISION.md)**

---

## Apa yang Bisa Dipelajari dari Project Ini

Kalau kamu belajar data engineering dari project ini, ini konsep-konsep yang diterapkan:

**ELT Pattern:** Extract dan Load dulu ke warehouse (BigQuery), baru Transform di dalam warehouse (dbt). Berbeda dengan ETL tradisional yang transform di luar warehouse. ELT memanfaatkan compute power warehouse yang sudah scalable.

**Medallion Architecture:** Data melewati tahap bronze (raw) → silver (staging + intermediate) → gold (marts). Setiap tahap menambah kualitas. Konsep ini dari Databricks tapi diadaptasi ke dbt layers.

**Separation of Concerns:** Ingestion (Python) dan transformation (dbt) punya boundary yang jelas di dataset `raw_maju`. Kalau ingestion gagal → debug Python. Kalau transform gagal → debug dbt. Tidak campur.

**Idempotency:** Setiap komponen aman dijalankan ulang. BigQuery `WRITE_TRUNCATE` menghapus dan mengganti. dbt `--full-refresh` rebuild dari nol. Airflow retry otomatis. Pipeline `ingest_customer_addresses.py` skip file yang sudah di-load.

**Star Schema:** Fact tables menyimpan events (penjualan, servis). Dimension tables menyimpan entities (customer, kendaraan, tanggal). BI tools optimal dengan pattern ini — query hanya 1 JOIN.

**Materialization Strategy:** Tiap layer punya materialization yang tepat: view (gratis, fresh) untuk staging, ephemeral (CTE) untuk intermediate, table (fast, partitioned) untuk marts. Bukan random pilih — ada alasan cost dan performance.

**Sensor-Gated Pipeline:** Setiap layer transition punya sensor yang verifikasi data ada sebelum lanjut. Ini mencegah silent failures: pipeline "sukses" tapi data kosong.

**Flag, Don't Delete:** Data quality issues di-flag dengan boolean columns, bukan dihapus. Raw data preserved. Serving layer yang filter. Ini best practice untuk traceability dan audit.

---

## Referensi

| Source | Apa yang direferensikan |
|--------|------------------------|
| Google Cloud Lakehouse whitepaper | BQ storage = GCS price, no separate storage justification |
| Google Cloud BigQuery pricing | Batch load from GCS = free |
| Google Cloud BigQuery ingestion guide | GCS recommended for batch file sources |
| dbt Labs — Best practice workflows | Raw layer outside dbt, source() vs ref() |
| dbt Labs — Staging models best practices | Staging as views, 1 source = 1 model |
| dbt Labs — How we structure our dbt projects | 3-layer: staging, intermediate, marts |
| Astronomer — ELT reference architecture | Airflow + BigQuery + dbt, PostgreSQL backend |
| Astrafy — BigQuery + dbt modeling | Materialization per layer, dataset naming |
| ModelDock — Medallion architecture with dbt | Bronze/silver/gold mapping to dbt layers |
| Eagle AI — Spark vs BigQuery | BigQuery as main data layer (migrated from GCS) |
| Towards Analytics Engineering | dbt project configuration in BigQuery |

---

## Author

**Fahern Khan** · [GitHub](https://github.com/fahernkhan) · [LinkedIn](https://linkedin.com/in/fahernkhan)

## License

MIT