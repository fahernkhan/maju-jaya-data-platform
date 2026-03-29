# Data Cleaning & Standardization

Dokumen ini menjelaskan masalah data yang ditemukan di source layer
dan cara penanganannya di dbt staging layer.

**Prinsip:** Raw data tidak dimodifikasi (immutability).
Cleaning dilakukan di staging layer via FLAG dan CAST — bukan DELETE.

---

## customers_raw

| # | Kolom | Masalah | Contoh Data Kotor | Cara Handle | Output |
|---|-------|---------|-------------------|-------------|--------|
| 1 | `dob` | 3 format berbeda | `1980/11/15`, `14/01/1995`, `1998-08-04` | `REGEXP_CONTAINS` + `SAFE.PARSE_DATE` per format | DATE type, 1 format |
| 2 | `dob` | Placeholder date | `1900-01-01` | Flag `is_dob_suspect = TRUE` | Data tetap ada, di-flag |
| 3 | `dob` | NULL (corporate entity) | `NULL` | Flag `is_dob_suspect = TRUE`, classify `customer_type = 'corporate'` | Data tetap ada, di-flag |
| 4 | `name` | Corporate entity tanpa DOB | `PT Black Bird` | Heuristic: prefix PT/CV/UD → `customer_type = 'corporate'` | Classified |

---

## sales_raw

| # | Kolom | Masalah | Contoh Data Kotor | Cara Handle | Output |
|---|-------|---------|-------------------|-------------|--------|
| 1 | `price` | String dengan titik ribuan | `"350.000.000"` | `REPLACE('.', '') + SAFE_CAST AS INT64` | INTEGER 350000000 |
| 2 | `vin` | Duplicate suspect | VIN berbeda, same customer + model + date | `COUNT(*) OVER (PARTITION BY customer_id, model, invoice_date)` | Flag `is_duplicate_suspect = TRUE` |

### Deduplication Strategy (di intermediate layer)
```
Staging  : FLAG duplicate → is_duplicate_suspect = TRUE
Intermediate: ROW_NUMBER() OVER (PARTITION BY customer_id, model, invoice_date ORDER BY created_at ASC)
              → is_canonical = TRUE untuk record pertama
Mart     : WHERE is_canonical = TRUE → hanya 1 record per group
```

---

## after_sales_raw

| # | Kolom | Masalah | Contoh Data Kotor | Cara Handle | Output |
|---|-------|---------|-------------------|-------------|--------|
| 1 | `vin` | Orphan VIN (tidak ada di sales_raw) | `POI1059IIK` | LEFT JOIN ke sales_raw, flag `is_vin_not_in_sales = TRUE` | Data tetap, di-flag |
| 2 | `service_date` | Tanggal masa depan | `2026-09-10` | `CAST AS DATE > CURRENT_DATE()` → flag `is_future_date = TRUE` | Data tetap, di-flag |

### Filter di Serving Layer
```
fact_after_sales  : semua data termasuk yang di-flag (untuk audit)
mart_aftersales_priority : WHERE is_vin_not_in_sales = FALSE
                           AND is_future_date = FALSE
```

---

## customer_addresses

| # | Kolom | Masalah | Contoh Data Kotor | Cara Handle | Output |
|---|-------|---------|-------------------|-------------|--------|
| 1 | `city` | Whitespace | `" Bekasi "` | `TRIM(city)` | `"Bekasi"` |
| 2 | `province` | Whitespace | `" Jawa Barat "` | `TRIM(province)` | `"Jawa Barat"` |
| 3 | Multiple rows | Customer punya >1 address | 2 rows per customer | `ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY created_at DESC)` → ambil terbaru | 1 row per customer |

> **Kenapa tidak INITCAP untuk city/province?**
> Address adalah free-text field. INITCAP merusak acronym seperti
> `DKI` → `Dki`, `RT/RW` → `Rt/Rw`. Tanpa master data wilayah,
> TRIM adalah satu-satunya standardisasi yang aman dilakukan.

---

## Summary

| Source Table | Masalah Ditemukan | Ditangani Di |
|---|---|---|
| customers_raw | DOB 3 format, placeholder, corporate | `stg_customers.sql` |
| sales_raw | Price string, duplicate suspect | `stg_sales.sql` → `int_sales_enriched.sql` |
| after_sales_raw | Orphan VIN, future date | `stg_after_sales.sql` |
| customer_addresses | Whitespace, multiple rows | `stg_customer_addresses.sql` |

---

## Prinsip yang Dipakai

```
1. Immutability     → raw tables tidak pernah dimodifikasi
2. Flag, not delete → data kotor tetap ada, di-flag untuk audit
3. Downstream filter→ serving layer yang decide mau include/exclude
4. 1 responsibility → staging hanya clean & cast,
                       business logic di intermediate
```