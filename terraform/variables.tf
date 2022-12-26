locals {
  data_lake_bucket = "fin_sheet_data_bucket"
}

variable "project" {
  type = string
  default = "dtc-de-350616"
}

variable "region" {
  type = string
  description = "Region for GCP resources."
  default = "southamerica-east1"
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "fin_sheet_data"
}