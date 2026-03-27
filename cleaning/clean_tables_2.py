"""Task 2a: Create cleaned views on MySQL source (immutability principle)."""
import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(
    f"mysql+pymysql://{os.getenv('MYSQL_USER','maju_jaya')}:{os.getenv('MYSQL_PASSWORD','maju_jaya_pass')}"
    f"@{os.getenv('MYSQL_HOST','localhost')}:{os.getenv('MYSQL_PORT','3306')}/{os.getenv('MYSQL_DB','maju_jaya')}")

VIEWS = {
    "customers_clean": """CREATE OR REPLACE VIEW customers_clean AS
        SELECT id AS customer_id, name,
            CASE WHEN dob REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(dob,'%Y-%m-%d')
                 WHEN dob REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(dob,'%Y/%m/%d')
                 WHEN dob REGEXP '^[0-9]{2}/[0-9]{2}/[0-9]{4}$' THEN STR_TO_DATE(dob,'%d/%m/%Y')
                 ELSE NULL END AS dob,
            CASE WHEN dob='1900-01-01' OR dob IS NULL THEN TRUE ELSE FALSE END AS is_dob_suspect,
            CASE WHEN name LIKE 'PT %' OR name LIKE 'CV %' OR name LIKE 'UD %' OR dob IS NULL
                 THEN 'corporate' ELSE 'individual' END AS customer_type, created_at
        FROM customers_raw""",
    "sales_clean": """CREATE OR REPLACE VIEW sales_clean AS
        SELECT vin, customer_id, model, invoice_date,
            CAST(REPLACE(price,'.','') AS UNSIGNED) AS price,
            CASE WHEN COUNT(*) OVER (PARTITION BY customer_id,model,invoice_date)>1 THEN TRUE ELSE FALSE
            END AS is_duplicate_suspect, created_at FROM sales_raw""",
    "after_sales_clean": """CREATE OR REPLACE VIEW after_sales_clean AS
        SELECT a.service_ticket,a.vin,a.customer_id,a.model,a.service_date,a.service_type,
            CASE WHEN s.vin IS NULL THEN TRUE ELSE FALSE END AS is_vin_not_in_sales,
            CASE WHEN a.service_date>CURDATE() THEN TRUE ELSE FALSE END AS is_future_date, a.created_at
        FROM after_sales_raw a LEFT JOIN (SELECT DISTINCT vin FROM sales_raw) s ON a.vin=s.vin""",
}

def run():
    with engine.begin() as conn:
        for name, sql in VIEWS.items():
            conn.execute(text(sql)); print(f"  ✅ {name}")
    print("\nCleaning views created.")

if __name__ == "__main__": run()