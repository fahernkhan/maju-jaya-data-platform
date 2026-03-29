docker build -f Dockerfile.airflow -t maju-airflow:latest .

# Pastikan di root project
cd ~/Documents/data_engineer/maju-jaya-data-platform

# ── LAYER 0: INGESTION ──────────────────────────────────────
echo "=== [1/5] INGESTION: MySQL → BigQuery ==="
python scripts/raw_customers.py
python scripts/raw_sales.py
python scripts/raw_after_sales.py

echo "=== [2/5] INGESTION: Drive → GCS → BigQuery ==="
python scripts/raw_customer_addresses.py

# ── LAYER 1: STAGING ────────────────────────────────────────
echo "=== [3/5] DBT STAGING ==="
cd dbt
dbt run --select staging

echo "=== [3/5] TEST STAGING ==="
dbt test --select staging

# ── LAYER 2: MARTS ──────────────────────────────────────────
echo "=== [4/5] DBT MARTS ==="
dbt run --select intermediate marts --full-refresh

echo "=== [4/5] TEST MARTS ==="
dbt test --select marts

# ── LAYER 3: FULL TEST ──────────────────────────────────────
echo "=== [5/5] FULL TEST SUITE ==="
dbt test