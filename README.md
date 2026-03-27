# Maju Jaya Data Platform

![alt text](<Layering Data Maju Jaya Motors.png>)

> End-to-end data engineering platform untuk retail otomotif "Maju Jaya".
> Migrasi dari Excel-based reporting ke modern data warehouse dengan BigQuery.

**Stack:** Python · Terraform · Docker · Airflow · GCS · BigQuery · dbt · Metabase

---

## Daftar Isi

1. [Ringkasan Proyek](#1-ringkasan-proyek)
2. [Arsitektur Sistem](#2-arsitektur-sistem)
3. [Tech Stack & Alasan Pemilihan](#3-tech-stack--alasan-pemilihan)
4. [Data Sources](#4-data-sources)
5. [Ingestion Layer](#5-ingestion-layer)
6. [Transformation Layer (dbt)](#6-transformation-layer-dbt)
7. [Data Warehouse Design](#7-data-warehouse-design)
8. [Orchestration (Airflow)](#8-orchestration-airflow)
9. [Data Quality](#9-data-quality)
10. [Infrastructure as Code](#10-infrastructure-as-code)
11. [Keputusan Arsitektur & Referensi](#11-keputusan-arsitektur--referensi)
12. [Cara Menjalankan](#12-cara-menjalankan)
13. [Interview Reference](#13-interview-reference)

---

## 1. Ringkasan Proyek

Maju Jaya adalah perusahaan retail otomotif yang sedang membangun pusat data. Saat ini data tersebar di MySQL (transaksi penjualan dan servis) dan Excel/file share (alamat customer harian). Tujuan proyek ini adalah membangun data platform yang:

- Mengkonsolidasikan semua data ke satu tempat (BigQuery)
- Menerapkan data cleaning yang konsisten dan traceable
- Menghasilkan report penjualan dan after-sales secara otomatis
- Bisa dijalankan ulang tanpa efek samping (idempotent)
- Terdokumentasi dengan lineage graph otomatis (dbt docs)

### Apa yang dibangun

| Komponen | Output |
|----------|--------|
| Pipeline ingestion | MySQL → BigQuery, Excel → GCS → BigQuery |
| Data cleaning | 6 masalah data teridentifikasi dan di-flag |
| Report 1 | Penjualan per periode, class, model |
| Report 2 | After-sales priority per customer |
| DWH design | Star schema: 4 dimensi + 2 fakta |
| Orchestration | Airflow DAG dengan sensors |
| Documentation | dbt docs lineage graph |

---

## 2. Arsitektur Sistem

```
┌─────────────────────────────────────────────────────────────────────┐
│                      Maju Jaya Data Platform                        │
│                                                                     │
│  Tech: Python · Terraform · Docker · Airflow                       │
│                                                                     │
│  ┌──────────────────────── Data Sources ────────────────────────┐   │
│  │                                                               │   │
│  │  ┌─────────────┐                    ┌─────────────┐          │   │
│  │  │ Excel File  │                    │ MySQL OLTP  │          │   │
│  │  │ Google Drive │                    │ 3 tables    │          │   │
│  │  └──────┬──────┘                    └──────┬──────┘          │   │
│  │         │                                  │                  │   │
│  └─────────┼──────────────────────────────────┼──────────────────┘   │
│            │ Python                           │ Python               │
│            ▼                                  │                      │
│  ┌──────────────────┐                         │                      │
│  │ Google Cloud     │                         │                      │
│  │ Storage (GCS)    │                         │                      │
│  │ Parquet staging  │                         │                      │
│  └────────┬─────────┘                         │                      │
│           │ free batch load                   │ direct load          │
│           ▼                                   ▼                      │
│  ┌──────────────────────────────────────────────────────────────┐   │
│  │                     BigQuery                                  │   │
│  │                                                               │   │
│  │  ┌───────────┐   ┌───────────┐   ┌───────────────────────┐  │   │
│  │  │ raw_maju  │──▶│ stg_maju  │──▶│      mart_maju        │  │   │
│  │  │ (tables)  │   │ (views)   │   │ (dim + fact + serving) │  │   │
│  │  └───────────┘   └───────────┘   └───────────────────────┘  │   │
│  │                                                               │   │
│  │                        dbt                                    │   │
│  └──────────────────────────────────────────────────────────────┘   │
│            │                                                        │
│            ▼                                                        │
│  ┌──────────────────┐                                               │
│  │    Metabase       │                                               │
│  │    Dashboard      │                                               │
│  └──────────────────┘                                               │
└─────────────────────────────────────────────────────────────────────┘
```

### Alur data dalam satu kalimat

MySQL dan Excel adalah sumber data mentah. Python scripts (dijalankan Airflow) meng-extract data dan menaruhnya di BigQuery sebagai raw tables. dbt kemudian mentransformasi raw → staging (clean) → intermediate (join + logic) → mart (star schema + reports). Metabase membaca langsung dari mart untuk dashboard.

---

## 3. Tech Stack & Alasan Pemilihan

### 3.1 Python (ingestion scripts)

Python dipakai untuk extract data dari MySQL dan Excel, lalu load ke BigQuery. Bukan dbt — karena dbt hanya transform, bukan extract/load.

Libraries utama:
- `pandas` + `sqlalchemy` — extract dari MySQL
- `openpyxl` — baca Excel (.xlsx)
- `google-cloud-bigquery` — load langsung ke BigQuery
- `google-cloud-storage` — upload Parquet ke GCS

### 3.2 Google Cloud Storage (GCS)

GCS dipakai HANYA untuk file-based sources (Excel/CSV). MySQL TIDAK lewat GCS.

Kenapa? Karena BigQuery batch load dari GCS itu gratis — zero ingestion cost. File asli juga tersimpan di GCS sebagai backup kalau perlu replay.

> **Referensi:** Google Cloud documentation: "For batch use cases, Cloud Storage is the recommended place to land incoming data." Batch loads from GCS to BigQuery are free of charge.
>
> — [BigQuery Data Ingestion Guide](https://cloud.google.com/blog/topics/developers-practitioners/bigquery-explained-data-ingestion)

### 3.3 BigQuery (data warehouse)

BigQuery adalah single source of truth untuk semua data — raw, staging, dan mart. Semua transformasi terjadi di sini via dbt.

Kenapa BigQuery, bukan MySQL sebagai warehouse?
- Serverless — tidak perlu manage server
- Columnar storage — query analytics jauh lebih cepat
- Native partitioning dan clustering — optimasi cost otomatis
- dbt-bigquery adapter mature dan well-documented

> **Referensi:** Google Cloud Lakehouse whitepaper: "Without a cost premium on BigQuery storage vs blob storage, there is no longer a required cost-based justification for separate storage."
>
> — [Building a Data Lakehouse](https://services.google.com/fh/files/misc/building-a-data-lakehouse.pdf)

### 3.4 dbt (transformation)

dbt menangani semua transformasi di dalam BigQuery. dbt TIDAK melakukan extract atau load — hanya transform (T dalam ELT).

Kenapa dbt?
- SQL-first — semua engineer bisa baca dan review
- Lineage graph otomatis — `dbt docs serve` visualisasi dependency
- Built-in testing — unique, not_null, accepted_values, custom SQL
- Version control — semua model di git, bisa code review

> **Referensi:** dbt Labs best practice guide: "Your dbt project will depend on raw data stored in your database. Since this data is normally loaded by third parties, the structure of it can change over time — dbt should not be responsible for loading raw data."
>
> — [dbt Best Practices](https://docs.getdbt.com/best-practices/best-practice-workflows)

### 3.5 Apache Airflow (orchestration)

Airflow mengatur urutan eksekusi: extract → sensor verify → load → dbt run → dbt test. Dijalankan via Docker dengan PostgreSQL sebagai metadata database.

Kenapa Airflow, bukan cron?
- DAG dependency — task B jalan hanya setelah task A sukses
- Retry with backoff — kalau gagal, coba lagi otomatis
- Sensors — verifikasi data ada sebelum lanjut
- UI monitoring — lihat status pipeline di browser

Kenapa PostgreSQL backend, bukan MySQL?
- MySQL sebagai Airflow metadata sering error karena lock contention
- PostgreSQL proven stable untuk Airflow (ini pattern dari Olist project)

> **Referensi:** Astronomer reference architecture: "ELT with BigQuery, dbt, and Apache Airflow for eCommerce" uses PostgreSQL backend and GCS-to-BigQuery batch loads.
>
> — [Astronomer ELT Reference](https://www.astronomer.io/docs/learn/reference-architecture-elt-bigquery-dbt)

### 3.6 Terraform (infrastructure)

Terraform membuat semua GCP resources secara reproducible: GCS bucket, BigQuery datasets, service account, IAM bindings. Satu `terraform apply` dan semuanya siap.

### 3.7 Docker (containerization)

Docker Compose menjalankan MySQL source, PostgreSQL (Airflow metadata), Adminer (database GUI), Airflow webserver, dan Airflow scheduler. Semua dalam satu `docker compose up -d`.

Custom Dockerfile untuk Airflow: semua pip packages di-install saat build, bukan saat runtime. Ini mencegah error startup yang sering terjadi kalau install packages saat container boot.

### 3.8 Metabase (visualization)

Metabase membaca langsung dari `mart_maju` dataset di BigQuery. Tidak perlu ETL tambahan — mart tables sudah pre-aggregated dan siap query.

---

## 4. Data Sources

### 4.1 MySQL OLTP (3 tables)

Database transaksional yang sudah ada di perusahaan.

**customers_raw** (6 rows):

| id | name | dob | created_at |
|----|------|-----|------------|
| 1 | Antonio | 1998-08-04 | 2025-03-01 14:24:40 |
| 2 | Brandon | 2001-04-21 | 2025-03-02 08:12:54 |
| 3 | Charlie | 1980/11/15 | 2025-03-02 11:20:02 |
| 4 | Dominikus | 14/01/1995 | 2025-03-03 09:50:41 |
| 5 | Erik | 1900-01-01 | 2025-03-03 17:22:03 |
| 6 | PT Black Bird | NULL | 2025-03-04 12:52:16 |

Masalah data: DOB punya 3 format berbeda, placeholder 1900-01-01, NULL untuk corporate.

**sales_raw** (5 rows):

| vin | customer_id | model | invoice_date | price |
|-----|-------------|-------|--------------|-------|
| JIS8135SAD | 1 | RAIZA | 2025-03-01 | 350.000.000 |
| MAS8160POE | 3 | RANGGO | 2025-05-19 | 430.000.000 |
| JLK1368KDE | 4 | INNAVO | 2025-05-22 | 600.000.000 |
| JLK1869KDF | 6 | VELOS | 2025-08-02 | 390.000.000 |
| JLK1962KOP | 6 | VELOS | 2025-08-02 | 390.000.000 |

Masalah data: price sebagai string dengan titik ribuan, 2 record terakhir suspect duplicate (same customer + model + date).

**after_sales_raw** (3 rows):

| service_ticket | vin | customer_id | model | service_date | service_type |
|----------------|-----|-------------|-------|--------------|--------------|
| T124-kgu1 | MAS8160POE | 3 | RANGGO | 2025-07-11 | BP |
| T560-jga1 | JLK1368KDE | 4 | INNAVO | 2025-08-04 | PM |
| T521-oai8 | POI1059IIK | 5 | RAIZA | 2026-09-10 | GR |

Masalah data: VIN POI1059IIK tidak ada di sales_raw (orphan), tanggal 2026-09-10 adalah masa depan.

### 4.2 Excel file share (daily)

File `customer_addresses_yyyymmdd.xlsx` keluar harian di folder. Berisi alamat terbaru customer.

| id | customer_id | address | city | province |
|----|-------------|---------|------|----------|
| 1 | 1 | Jalan Mawar V, RT 1/RW 2 | Bekasi | Jawa Barat |
| 2 | 3 | Jl Ababil Indah | Tangerang Selatan | Jawa Barat |
| 3 | 4 | Jl. Kemang Raya 1 No 3 | JAKARTA PUSAT | DKI JAKARTA |
| 4 | 6 | Astra Tower Jalan Yos Sudarso 12 | Jakarta Utara | DKI Jakarta |

Masalah data: city/province tidak konsisten (uppercase vs title case).

---

## 5. Ingestion Layer

### 5.1 Prinsip desain

Tiga prinsip yang diikuti di ingestion layer:

1. **Idempotent** — pipeline bisa dijalankan ulang tanpa duplikasi data. BigQuery `WRITE_TRUNCATE` menghapus dan mengganti partisi, bukan menambah.

2. **Audit trail** — setiap run dicatat di `pipeline_audit_log` MySQL table. Kalau file sudah pernah sukses di-load, pipeline skip.

3. **Minimal transformation** — di ingestion, hanya tambah metadata (`_ingestion_date`, `_source`). Semua business logic ada di dbt, bukan di ingestion scripts.

### 5.2 MySQL → BigQuery (direct load)

```
MySQL  ──[pandas.read_sql_table]──▶  DataFrame  ──[bq.load_table_from_dataframe]──▶  BigQuery raw_maju
```

MySQL di-load LANGSUNG ke BigQuery tanpa lewat GCS. Script: `scripts/extract_mysql_to_bigquery.py`.

**Kenapa direct, bukan lewat GCS?**

BigQuery storage costs $0.020/GB/month. GCS Standard costs $0.020-0.023/GB/month. Harga sama. Untuk database source yang sudah structured (SQL rows), GCS tidak menambah value — hanya menambah satu hop yang bisa gagal.

> **Referensi:** Eagle AI case study: "BigQuery became our main data layer — not just a warehouse but the backbone where all our raw and processed data lives." They migrated away from GCS-first to BigQuery-direct.
>
> — [Spark vs BigQuery, Eagle AI](https://eagleeye.com/blog/spark-vs-bigquery-eagle-ai)

> **Referensi:** Juan Ramos (Towards Analytics Engineering): pattern menggunakan Fivetran/Stitch loading directly into `company-raw` BigQuery project, bukan lewat GCS.
>
> — [How to configure dbt projects in BigQuery](https://towardsanalyticsengineering.substack.com/p/how-to-configure-dbt-projects-in)

### 5.3 Excel → GCS → BigQuery (batch load)

```
Excel  ──[openpyxl]──▶  DataFrame  ──[to_parquet]──▶  GCS bucket  ──[bq.load_table_from_uri]──▶  BigQuery raw_maju
```

File Excel di-convert ke Parquet, upload ke GCS, lalu batch load ke BigQuery. Scripts: `scripts/extract_excel_to_gcs.py` + `scripts/load_gcs_to_bigquery.py`.

**Kenapa lewat GCS untuk Excel?**

Batch load dari GCS ke BigQuery itu GRATIS — zero ingestion cost. Plus file Parquet asli tetap tersimpan di GCS sebagai backup. Kalau ada masalah, bisa replay tanpa minta ulang file dari source.

> **Referensi:** Google Cloud BigQuery pricing: "Loading data from Cloud Storage is free for batch loads." Streaming inserts have costs, but batch loads from GCS do not.
>
> — [BigQuery Pricing](https://cloud.google.com/bigquery/pricing)

### 5.4 Boundary: ingestion vs transformation

Ini boundary paling penting di seluruh arsitektur:

| | Ingestion | Transformation |
|---|---|---|
| Tool | Python + Airflow | dbt |
| Baca dari | MySQL, Excel | BigQuery `raw_maju` |
| Tulis ke | BigQuery `raw_maju` | BigQuery `stg_maju`, `mart_maju` |
| Scope | Extract + Load | Transform |
| Boleh join? | Tidak | Ya (di intermediate layer) |
| Boleh business logic? | Tidak | Ya (di intermediate/mart layer) |

dbt TIDAK PERNAH membuat raw tables. dbt hanya mendeklarasikan raw tables sebagai `sources` di YAML, lalu membaca via `{{ source() }}`.

> **Referensi:** dbt Labs: "dbt does not extract or load data. It focuses on the transformation step of ELT."
>
> — [What is dbt?](https://docs.getdbt.com/docs/introduction)

---

## 6. Transformation Layer (dbt)

### 6.1 Layer architecture

dbt models diorganisir dalam 4 layer, mengikuti Medallion Architecture yang diadaptasi:

```
Medallion    │ dbt layer     │ BigQuery dataset │ Materialization │ Fungsi
─────────────┼───────────────┼──────────────────┼─────────────────┼─────────────────
Bronze       │ (not dbt)     │ raw_maju         │ native tables   │ Data mentah, untouched
Silver       │ staging       │ stg_maju         │ VIEW            │ Clean, cast, rename, flag
Silver       │ intermediate  │ (ephemeral/CTE)  │ EPHEMERAL       │ Join, business logic
Gold         │ marts         │ mart_maju        │ TABLE           │ Star schema + reports
```

> **Referensi:** dbt Labs blog: "We continue to encourage dbt users to follow this structure, especially establishing a staging layer mainly composed of views, not tables."
>
> — [Staging Models Best Practices](https://www.getdbt.com/blog/staging-models-best-practices-and-limiting-view-runs)

> **Referensi:** ModelDock: "The Medallion Architecture maps directly to dbt's staging, intermediate, and marts pattern. Bronze = raw sources, Silver = staging + intermediate, Gold = marts."
>
> — [Medallion Architecture with dbt](https://modeldock.run/blog/medallion-architecture-dbt)

### 6.2 Staging layer (views)

Aturan: 1 source table = 1 staging model. Hanya rename, cast, filter null. TIDAK ada join.

| Model | Source | Cleaning yang dilakukan |
|-------|--------|------------------------|
| `stg_customers` | raw_maju.customers | DOB: 3 format → 1 DATE. Flag: is_dob_suspect, customer_type |
| `stg_sales` | raw_maju.sales | Price: string → INT64. Flag: is_duplicate_suspect |
| `stg_after_sales` | raw_maju.after_sales | Flag: is_vin_not_in_sales, is_future_date |
| `stg_customer_addresses` | raw_maju.customer_addresses | Latest address per customer via ROW_NUMBER |

**Kenapa VIEW, bukan TABLE?**

Views tidak menyimpan data — hanya query definition. Zero storage cost. Selalu fresh: kalau raw berubah, staging otomatis reflect perubahan. Staging hanya rename/cast — view cukup cepat untuk ini.

> **Referensi:** Astrafy (Google Cloud Partner): "Use views for staging models. They don't incur storage costs and always reflect the latest source data."
>
> — [Data Modeling Best Practices with BigQuery and dbt](https://medium.astrafy.io/data-modeling-best-practices-with-bigquery-and-dbt-329b37faf229)

### 6.3 Intermediate layer (ephemeral)

Intermediate = tempat join dan business logic. Materialized sebagai ephemeral (CTE — tidak persist ke BigQuery).

| Model | Sources | Logic |
|-------|---------|-------|
| `int_customer_enriched` | stg_customers + stg_addresses | Join customer + latest address |
| `int_sales_enriched` | stg_sales + stg_customers | Price class (LOW/MEDIUM/HIGH), dedup via ROW_NUMBER, periode YYYY-MM |

**Kenapa EPHEMERAL?**

Tidak ada BI tool yang query intermediate — hanya mart models yang referensi mereka. Hemat storage. Logic tetap terpisah di file sendiri, jadi lineage graph tetap menunjukkan dependency chain yang jelas.

Trade-off: kalau query mart lambat karena CTE terlalu complex, upgrade ke TABLE.

> **Referensi:** Alejandro Aboy: "What Medallion calls Bronze is sometimes mistaken for dbt staging. The main difference is that dbt staging already does basic processing, while Bronze is truly raw."
>
> — [SQL to dbt guide: How Data Layers Flow](https://thepipeandtheline.substack.com/p/sql-to-dbt-guide-how-data-layers)

### 6.4 Mart layer (tables)

Mart berisi star schema (fact + dimension) dan serving models (pre-aggregated reports).

**Dimensions:**

| Model | Grain | Kolom utama |
|-------|-------|-------------|
| `dim_customer` | 1 per customer | customer_id, name, type, city, province, full_address |
| `dim_vehicle` | 1 per model | model, min/max/avg price, default_price_class |
| `dim_date` | 1 per tanggal | date_id, year, month, quarter, day_name, is_weekend |
| `dim_service_type` | 1 per code | service_type_code (BP/PM/GR), name, description |

**Facts:**

| Model | Grain | Partitioned | Clustered |
|-------|-------|-------------|-----------|
| `fact_sales` | 1 per VIN (deduplicated) | invoice_date (monthly) | customer_id, price_class |
| `fact_after_sales` | 1 per service_ticket | — | — |

**Serving (reports dari soal):**

| Model | Grain | Sesuai requirement |
|-------|-------|--------------------|
| `mart_sales_summary` | 1 per (periode, class, model) | Report 1: periode, class, model, total |
| `mart_aftersales_priority` | 1 per (year, vin) | Report 2: periode, vin, customer, address, count, priority |

**Kenapa TABLE?**

BI tools query mart ratusan kali per hari. TABLE = pre-computed, instant response. Bisa di-partition by date (scan hanya partisi yang dibutuhkan) dan cluster by column (reduce bytes scanned).

> **Referensi:** Laxminarayana Likki: "Mart layer should always be materialized as tables. These are the models that business users and BI tools directly consume."
>
> — [Staging, Intermediate & Mart Layers in dbt](https://medium.com/@likkilaxminarayana/21-staging-intermediate-mart-layers-in-dbt-a-complete-guide-0d4fcb2ccfc6)

---

## 7. Data Warehouse Design

### 7.1 Star schema

```
                    ┌────────────────┐
                    │  dim_customer  │
                    │  PK: customer_id│
                    └───────┬────────┘
                            │
  ┌──────────────┐  ┌───────┴────────┐  ┌──────────────────┐
  │ dim_vehicle  │  │  fact_sales    │  │ fact_after_sales │
  │ PK: model    │──│  PK: vin      │  │ PK: ticket       │
  └──────────────┘  │  FK: customer  │  │ FK: customer     │
                    │  FK: model     │  │ FK: date         │
  ┌──────────────┐  │  FK: date      │  │ FK: service_type │
  │  dim_date    │──│  price         │  └──────────────────┘
  │  PK: date_id │  │  price_class   │          │
  └──────────────┘  └────────────────┘  ┌───────┴──────────┐
                                        │ dim_service_type │
                                        │ PK: code         │
                                        └──────────────────┘
```

### 7.2 Data lineage (dbt docs)

```
raw_maju.customers ──▶ stg_customers ──┐
                                        ├──▶ int_customer_enriched ──▶ dim_customer ──┐
raw_maju.customer_addresses ──▶ stg_addresses ──┘                                     │
                                                                                       ├──▶ mart_aftersales_priority
raw_maju.sales ──▶ stg_sales ──┐                                                      │
                                ├──▶ int_sales_enriched ──▶ fact_sales ──▶ mart_sales_summary
raw_maju.customers ──▶ stg_customers ──┘                        │
                                                                ▼
                                                           dim_vehicle

raw_maju.after_sales ──▶ stg_after_sales ──▶ fact_after_sales ──▶ mart_aftersales_priority
```

Lineage ini bisa dilihat secara interaktif via `dbt docs serve --port 8082`.

---

## 8. Orchestration (Airflow)

### 8.1 DAG design

DAG `maju_jaya_pipeline` berjalan daily jam 06:00 WIB. Design patterns yang diterapkan:

| Pattern | Implementasi | Alasan |
|---------|-------------|--------|
| PythonSensor | Verify data di BQ/GCS sebelum lanjut | Cegah dbt run pada data kosong |
| mode=reschedule | Sensor lepas worker slot saat menunggu | Cegah worker deadlock |
| Exponential backoff | retry_delay 2min → 4min → 8min | Transient failures recover otomatis |
| max_active_runs=1 | Hanya 1 DAG run paralel | Cegah race condition |
| SLA=2h | Alert jika pipeline kelamaan | Monitoring |
| on_failure_callback | Log error details | Debugging |

### 8.2 Task flow

```
start
  ├──▶ extract_mysql_direct_to_bq ──────────────────────┐
  └──▶ extract_excel_to_gcs ──▶ sensor_gcs ──▶ load_bq ─┤
                                                         ▼
                              [sensor_raw_customers, sensor_raw_sales, sensor_raw_addresses]
                                                         ▼
                                                     dbt_deps
                                                         ▼
                                                  dbt_run_staging
                                                         ▼
                                                  sensor_staging
                                                         ▼
                                                 dbt_test_staging
                                                         ▼
                                                  dbt_run_marts
                                                         ▼
                                                   sensor_mart
                                                         ▼
                                                 dbt_test_marts
                                                         ▼
                                                 pipeline_complete
```

Total: 17 tasks, 6 sensors.

### 8.3 Docker setup

Airflow berjalan di Docker dengan 6 containers:

| Container | Image | Port | Fungsi |
|-----------|-------|------|--------|
| postgres | postgres:15-alpine | 5432 | Airflow metadata |
| mysql-source | mysql:8.0 | 3306 | OLTP source (seed data otomatis) |
| adminer | adminer:4.8.1 | 8080 | Database GUI |
| airflow-init | custom | — | One-time: migrate DB + create user |
| airflow-web | custom | 8081 | Airflow UI |
| airflow-scheduler | custom | — | DAG scheduler |

Custom Dockerfile: packages pre-installed saat build (bukan runtime). Ini mencegah error startup yang terjadi kalau Airflow boot sebelum pip install selesai.

---

## 9. Data Quality

### 9.1 Strategi

Data quality diterapkan di dua titik:

| Titik | Tool | Jenis validasi |
|-------|------|----------------|
| Post-load (raw) | Airflow PythonSensor | Table exists, row count > 0 |
| Post-transform (mart) | dbt tests | unique, not_null, accepted_values, relationships, custom SQL |

### 9.2 Masalah data yang teridentifikasi

| Tabel | Kolom | Masalah | Solusi di staging |
|-------|-------|---------|-------------------|
| customers_raw | dob | 3 format berbeda (YYYY-MM-DD, YYYY/MM/DD, DD/MM/YYYY) | CASE WHEN + PARSE_DATE |
| customers_raw | dob | Placeholder 1900-01-01 | Flag `is_dob_suspect = true` |
| customers_raw | name | "PT Black Bird" = corporate, dob NULL | Flag `customer_type = 'corporate'` |
| sales_raw | price | String "350.000.000" bukan integer | REPLACE('.','') + CAST INT64 |
| sales_raw | vin | 2 record same customer+model+date | Flag `is_duplicate_suspect`, dedup via ROW_NUMBER |
| after_sales_raw | vin | POI1059IIK tidak ada di sales_raw | Flag `is_vin_not_in_sales` |
| after_sales_raw | service_date | 2026-09-10 = masa depan | Flag `is_future_date` |

Prinsip: raw data TIDAK dimodifikasi. Semua masalah di-FLAG, bukan di-delete. Data lineage tetap traceable.

### 9.3 dbt tests

```yaml
# Built-in tests
- unique, not_null (pada semua PK)
- accepted_values: customer_type ['individual','corporate'], price_class ['LOW','MEDIUM','HIGH']
- relationships: fact_sales.customer_id → dim_customer.customer_id

# Custom SQL tests
- assert_revenue_non_negative: fact_sales.price >= 0
- assert_serving_excludes_invalid: orphan VIN dan future date tidak muncul di serving
```

### 9.4 Kenapa tidak pakai Great Expectations?

Soal tidak meminta data quality framework khusus. dbt tests sudah mencakup semua validasi yang diperlukan. Great Expectations bisa ditambahkan sebagai future improvement untuk pre-load raw validation, tapi untuk scope project ini, dbt tests cukup dan lebih pragmatis.

---

## 10. Infrastructure as Code

### 10.1 Terraform resources

| Resource | Nama | Tujuan |
|----------|------|--------|
| GCS bucket | maju-jaya-raw-dev | Staging area untuk file sources |
| BigQuery dataset | raw_maju | Raw tables (ingestion target) |
| BigQuery dataset | stg_maju | Staging views (dbt) |
| BigQuery dataset | mart_maju | Mart tables (dbt) |
| Service account | sa-pipeline-dev | Pipeline runner (BQ editor + GCS admin) |
| IAM binding | bigquery.dataEditor | SA bisa write ke BQ |
| IAM binding | bigquery.user | SA bisa run queries |
| IAM binding | storage.objectAdmin | SA bisa write ke GCS |

### 10.2 Apply

```bash
cd terraform && terraform init && terraform apply
```

Satu command, semua GCP resources terbuat. Reproducible, version-controlled, reviewable.

---

## 11. Keputusan Arsitektur & Referensi

### 11.1 Ringkasan keputusan

| Keputusan | Pilihan | Alternatif yang tidak dipilih | Alasan |
|-----------|---------|-------------------------------|--------|
| MySQL routing | Direct ke BigQuery | Via GCS dulu | BQ storage = GCS price, no benefit |
| Excel routing | Via GCS, batch load | Direct ke BigQuery | Batch load GRATIS, file backup di GCS |
| Raw layer ownership | Python/Airflow | dbt | dbt = T dalam ELT, bukan E atau L |
| Staging materialization | VIEW | TABLE | $0 storage, always fresh |
| Intermediate materialization | EPHEMERAL | TABLE | $0 storage, logic tetap terpisah |
| Mart materialization | TABLE | VIEW | Fast reads, partitioning, clustering |
| GCS sebagai data lake | Hanya file staging | Full data lake | BQ storage = GCS price, no cost benefit |
| Airflow metadata DB | PostgreSQL | MySQL | PG lebih stable untuk Airflow |
| Data quality | dbt tests | Great Expectations | Soal tidak minta, dbt tests cukup |

### 11.2 Referensi lengkap

| Source | Apa yang direferensikan | URL |
|--------|------------------------|-----|
| Google Cloud Blog | BigQuery data ingestion best practices | cloud.google.com/blog/.../bigquery-explained-data-ingestion |
| Google Cloud Whitepaper | BigQuery storage vs GCS pricing parity | services.google.com/fh/files/misc/building-a-data-lakehouse.pdf |
| BigQuery Pricing | Batch load from GCS = free | cloud.google.com/bigquery/pricing |
| dbt Labs | Best practice workflows, raw layer outside dbt | docs.getdbt.com/best-practices/best-practice-workflows |
| dbt Labs Blog | Staging models as views best practice | getdbt.com/blog/staging-models-best-practices |
| Astronomer | ELT reference architecture (Airflow + BigQuery + dbt) | astronomer.io/docs/learn/reference-architecture-elt-bigquery-dbt |
| Towards Analytics Engineering | dbt project configuration in BigQuery | towardsanalyticsengineering.substack.com |
| ModelDock | Medallion architecture mapping to dbt | modeldock.run/blog/medallion-architecture-dbt |
| Astrafy | Data modeling best practices BigQuery + dbt | medium.astrafy.io |
| Eagle AI | BigQuery as main data layer (migrated from GCS) | eagleeye.com/blog/spark-vs-bigquery-eagle-ai |

---

## 12. Cara Menjalankan

### Quick start

```bash
# 0. Prerequisites
gcloud auth login && gcloud config set project maju-jaya-platform

# 1. Infrastructure
cd terraform && terraform init && terraform apply && cd ..

# 2. Docker (MySQL + Airflow)
docker compose build && docker compose up -d && sleep 30

# 3. Ingestion
python scripts/extract_mysql_to_bigquery.py
python scripts/extract_excel_to_gcs.py
python scripts/load_gcs_to_bigquery.py

# 4. dbt
cd dbt && dbt deps && dbt run --full-refresh && dbt test
dbt docs generate && dbt docs serve --port 8082

# 5. Airflow
# http://localhost:8081 → trigger maju_jaya_pipeline → all green
```

### Service URLs

| Service | URL | Login |
|---------|-----|-------|
| Adminer (MySQL GUI) | http://localhost:8080 | server: mysql-source, user: maju_jaya |
| Airflow | http://localhost:8081 | admin / admin |
| dbt docs | http://localhost:8082 | — |

---

## 13. Interview Reference

### One-liner untuk CV

> Built an end-to-end data platform for automotive retail: ingested MySQL and Excel sources into BigQuery, transformed data using dbt with a 4-layer architecture (staging → intermediate → marts), designed a star schema warehouse, and orchestrated daily pipelines with Airflow using PythonSensors for data verification.

### Pertanyaan yang mungkin ditanya

**"Kenapa MySQL tidak lewat GCS?"**
> Karena BigQuery storage dan GCS Standard harganya sama ($0.020/GB/month). Untuk structured database data, GCS hanya menambah complexity tanpa benefit. Google Cloud sendiri menyediakan Datastream yang langsung MySQL → BigQuery.

**"Kenapa staging pakai view?"**
> dbt Labs merekomendasikan staging layer sebagai views. Zero storage cost, selalu fresh. Staging hanya rename/cast — tidak ada heavy computation yang butuh pre-materialization.

**"Kenapa intermediate ephemeral, bukan table?"**
> Intermediate adalah workspace internal — tidak ada BI tool yang query langsung. Ephemeral = CTE di query mart, hemat storage. Tapi logic tetap di file terpisah, jadi lineage traceable. Kalau performance jadi masalah, bisa upgrade ke table.

**"Kenapa pakai dbt, bukan raw SQL?"**
> dbt memberikan lineage graph otomatis, built-in testing (unique, not_null, accepted_values), documentation via `dbt docs serve`, dan modular SQL yang versioned di git. Raw SQL bisa achieve hal yang sama, tapi butuh tooling sendiri untuk testing, docs, dan lineage.

**"Gimana handle data quality?"**
> Dua layer: sensors di Airflow verify data exists sebelum dbt jalan, dan dbt tests validate integrity setelah transform. Custom SQL assertions compare mart vs raw untuk detect data loss.

**"Kenapa Airflow pakai PostgreSQL backend?"**
> MySQL sebagai Airflow metadata database sering error karena lock contention pada metadata tables dan utf8mb4 charset issues. PostgreSQL proven stable dan ini pattern yang dipakai di production Airflow deployments.

---

## License

MIT License. Built for AstraWorld Data Engineer technical test.

## Author

**Fahern Khan** · [GitHub](https://github.com/fahernkhan) · [LinkedIn](https://linkedin.com/in/fahernkhan)