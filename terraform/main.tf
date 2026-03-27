terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ── GCS BUCKET (Data Lake) ───────────────────────────────────
resource "google_storage_bucket" "raw" {
  name          = var.gcs_bucket_raw
  location      = var.region
  force_destroy = true # ⚠️ dev only

  versioning {
    enabled = true
  }

  lifecycle_rule {
    condition {
      age = 90
    }

    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }
}

# ── BIGQUERY DATASETS ────────────────────────────────────────
resource "google_bigquery_dataset" "raw" {
  dataset_id = "raw_maju"
  location   = var.region

  labels = {
    layer      = "raw"
    managed_by = "terraform"
  }
}

resource "google_bigquery_dataset" "stg" {
  dataset_id = "stg_maju"
  location   = var.region

  labels = {
    layer      = "staging"
    managed_by = "terraform"
  }
}

resource "google_bigquery_dataset" "mart" {
  dataset_id = "mart_maju"
  location   = var.region

  labels = {
    layer      = "mart"
    managed_by = "terraform"
  }
}

# ── SERVICE ACCOUNT (UNIVERSAL) ──────────────────────────────
resource "google_service_account" "pipeline" {
  account_id = "sa-pipeline-dev"

  display_name = "Universal Data Pipeline Service Account (Maju Jaya & Olist)"

  description = "Shared service account for data pipelines across Maju Jaya and Olist projects (Airflow, dbt, GCS, BigQuery)"
}

# ── IAM ROLES ────────────────────────────────────────────────
resource "google_project_iam_member" "pipeline_bq" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_bq_user" {
  project = var.project_id
  role    = "roles/bigquery.user"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}

resource "google_project_iam_member" "pipeline_gcs" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.pipeline.email}"
}