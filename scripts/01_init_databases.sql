-- Buat database untuk aplikasi
CREATE DATABASE IF NOT EXISTS maju_jaya;
CREATE USER IF NOT EXISTS 'maju_jaya'@'%' IDENTIFIED BY 'maju_jaya_pass';
GRANT ALL PRIVILEGES ON maju_jaya.* TO 'maju_jaya'@'%';

-- Buat database untuk Airflow metadata
CREATE DATABASE IF NOT EXISTS airflow_db;
CREATE USER IF NOT EXISTS 'airflow'@'%' IDENTIFIED BY 'airflow_pass';
GRANT ALL PRIVILEGES ON airflow_db.* TO 'airflow'@'%';

FLUSH PRIVILEGES;
