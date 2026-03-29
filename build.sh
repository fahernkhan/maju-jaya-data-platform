docker build -f Dockerfile.airflow -t maju-airflow:latest .

##raw
# Pastikan di root project dulu
cd ~/Documents/data_engineer/maju-jaya-data-platform

# Run semua ingestion dengan nama tabel yang sudah benar
python scripts/raw_customers.py
python scripts/raw_sales.py
python scripts/raw_after_sales.py
python scripts/raw_customer_addresses.py

