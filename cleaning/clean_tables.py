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
