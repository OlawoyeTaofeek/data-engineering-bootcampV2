variable "credentials" {
  description = "My Credentials"
  default     = "../keys/my-creds.json"
  #ex: if you have a directory where this file is called keys with your service account json file
  #saved there as my-creds.json you could use default = "./keys/my-creds.json"
}

variable "project" {
  description = "Project"
  default     = "terraform-project-485117"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "us-central1"
}

variable "location" {
  description = "Project Location"
  #Update the below to your desired location
  default     = "US"
}

variable "bq_dataset_name" {
  description = "My BigQuery dataset name"
  default = "demo_dataset"
}

variable "gcs_bucket_name" {
  description = "My storage bucket Name"
  default = "terraform-demo"
}

variable "gcp_storage_class" {
    description = "Bucket Storage Class"
    default = "STANDARD"
}