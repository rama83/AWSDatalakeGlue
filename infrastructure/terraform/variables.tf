variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "us-east-1"
}

variable "aws_profile" {
  description = "AWS profile"
  type        = string
  default     = "default"
}

variable "environment" {
  description = "Deployment environment"
  type        = string
  default     = "dev"
  
  validation {
    condition     = contains(["dev", "test", "staging", "prod"], var.environment)
    error_message = "Environment must be one of: dev, test, staging, prod."
  }
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "datalake"
}

variable "bronze_bucket_name" {
  description = "Name of the Bronze layer S3 bucket"
  type        = string
  default     = null
}

variable "silver_bucket_name" {
  description = "Name of the Silver layer S3 bucket"
  type        = string
  default     = null
}

variable "deployment_bucket_name" {
  description = "Name of the deployment S3 bucket"
  type        = string
  default     = null
}

variable "silver_database_name" {
  description = "Name of the Silver layer Glue database"
  type        = string
  default     = null
}
