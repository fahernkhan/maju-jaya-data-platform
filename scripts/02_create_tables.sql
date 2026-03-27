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
