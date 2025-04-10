output "bronze_bucket" {
  description = "Bronze layer S3 bucket"
  value       = aws_s3_bucket.bronze_bucket.bucket
}

output "silver_bucket" {
  description = "Silver layer S3 bucket"
  value       = aws_s3_bucket.silver_bucket.bucket
}

output "deployment_bucket" {
  description = "Deployment S3 bucket"
  value       = aws_s3_bucket.deployment_bucket.bucket
}

output "silver_database" {
  description = "Silver layer Glue database"
  value       = aws_glue_catalog_database.silver_database.name
}

output "glue_role_arn" {
  description = "ARN of the IAM role for Glue jobs"
  value       = aws_iam_role.glue_role.arn
}

output "ingest_to_bronze_job" {
  description = "Ingest to Bronze Glue job"
  value       = aws_glue_job.ingest_to_bronze.name
}

output "bronze_to_silver_job" {
  description = "Bronze to Silver Glue job"
  value       = aws_glue_job.bronze_to_silver.name
}

output "python_shell_example_job" {
  description = "Python Shell example Glue job"
  value       = aws_glue_job.python_shell_example.name
}
