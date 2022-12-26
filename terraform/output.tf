output "User" {
  value = var.project
}

output "GCS_bucket_name" {
    description = "Nome do bucket GCS"
    value = google_storage_bucket.bike-w-lake-bucket.name
}

output "GCS_bucket_region" {
    description = "Regĩao do Bucket GCS"
    value = var.region
}

output "BigQuery_name" {
  value = google_bigquery_dataset.dataset.dataset_id
}