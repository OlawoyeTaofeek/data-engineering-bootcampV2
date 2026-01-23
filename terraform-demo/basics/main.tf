terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.16.0"
    }
  }
}

provider "google" {
  credentials = "./keys/my-creds.json"
  project = "terraform-project-485117"
  region  = "US-central1"
}


resource "google_storage_bucket" "auto_expiring_bucket" {
  name          = "terraform-demo"
  location      = "US"
  force_destroy = true
  // Optional, but recommended settings:
  storage_class = "STANDARD"
  uniform_bucket_level_access = true

  versioning {
    enabled     = true
  }
  lifecycle_rule {
    condition {
      age = 30 // days
    }
    action {
      type = "AbortIncompleteMultipartUpload"
    }
  }
}

resource "google_bigquery_dataset" "dataset" {
  dataset_id                  = "demo_dataset"
  friendly_name               = "demo dataset"
  description                 = "My datasets for analytics"
  location                    = "US"
  default_table_expiration_ms = 3600000

  labels = {
    env = "default"
  }

  access {
    role          = "roles/bigquery.dataOwner"
    user_by_email = google_service_account.bqowner.email
  }

  access {
    role   = "READER"
    domain = "hashicorp.com"
  }
}

resource "google_service_account" "bqowner" {
  account_id = "bqowner"
}