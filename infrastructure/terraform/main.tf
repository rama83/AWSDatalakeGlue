provider "aws" {
  region  = var.aws_region
  profile = var.aws_profile
}

# S3 Buckets
resource "aws_s3_bucket" "bronze_bucket" {
  bucket = "${var.environment}-${var.project_name}-bronze"
  
  tags = {
    Name        = "${var.environment}-${var.project_name}-bronze"
    Environment = var.environment
    Project     = var.project_name
    Layer       = "bronze"
  }
}

resource "aws_s3_bucket" "silver_bucket" {
  bucket = "${var.environment}-${var.project_name}-silver"
  
  tags = {
    Name        = "${var.environment}-${var.project_name}-silver"
    Environment = var.environment
    Project     = var.project_name
    Layer       = "silver"
  }
}

resource "aws_s3_bucket" "deployment_bucket" {
  bucket = "${var.environment}-${var.project_name}-deployment"
  
  tags = {
    Name        = "${var.environment}-${var.project_name}-deployment"
    Environment = var.environment
    Project     = var.project_name
    Layer       = "deployment"
  }
}

# Glue Database
resource "aws_glue_catalog_database" "silver_database" {
  name = "${var.environment}_${var.project_name}_silver"
  
  description = "Silver layer database for ${var.project_name} in ${var.environment} environment"
}

# IAM Role for Glue
resource "aws_iam_role" "glue_role" {
  name = "${var.environment}_${var.project_name}_glue_role"
  
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "glue.amazonaws.com"
        }
      }
    ]
  })
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

# Attach policies to the Glue role
resource "aws_iam_role_policy_attachment" "glue_service" {
  role       = aws_iam_role.glue_role.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
}

resource "aws_iam_role_policy" "s3_access" {
  name = "${var.environment}_${var.project_name}_s3_access"
  role = aws_iam_role.glue_role.id
  
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Effect = "Allow"
        Resource = [
          aws_s3_bucket.bronze_bucket.arn,
          "${aws_s3_bucket.bronze_bucket.arn}/*",
          aws_s3_bucket.silver_bucket.arn,
          "${aws_s3_bucket.silver_bucket.arn}/*",
          aws_s3_bucket.deployment_bucket.arn,
          "${aws_s3_bucket.deployment_bucket.arn}/*"
        ]
      }
    ]
  })
}

# Example Glue Job
resource "aws_glue_job" "ingest_to_bronze" {
  name     = "${var.environment}_ingest_to_bronze"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.deployment_bucket.bucket}/scripts/ingest_to_bronze.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"        = "python"
    "--class"               = "GlueApp"
    "--enable-metrics"      = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"     = "true"
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.deployment_bucket.bucket}/spark-logs/"
  }
  
  glue_version = "5.0"
  worker_type  = "G.1X"
  number_of_workers = 5
  timeout      = 2880
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_glue_job" "bronze_to_silver" {
  name     = "${var.environment}_bronze_to_silver"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "glueetl"
    script_location = "s3://${aws_s3_bucket.deployment_bucket.bucket}/scripts/bronze_to_silver.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--job-language"        = "python"
    "--class"               = "GlueApp"
    "--enable-metrics"      = "true"
    "--enable-continuous-cloudwatch-log" = "true"
    "--enable-spark-ui"     = "true"
    "--spark-event-logs-path" = "s3://${aws_s3_bucket.deployment_bucket.bucket}/spark-logs/"
  }
  
  glue_version = "5.0"
  worker_type  = "G.1X"
  number_of_workers = 5
  timeout      = 2880
  
  execution_property {
    max_concurrent_runs = 1
  }
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}

resource "aws_glue_job" "python_shell_example" {
  name     = "${var.environment}_python_shell_example"
  role_arn = aws_iam_role.glue_role.arn
  
  command {
    name            = "pythonshell"
    script_location = "s3://${aws_s3_bucket.deployment_bucket.bucket}/scripts/python_shell_example.py"
    python_version  = "3"
  }
  
  default_arguments = {
    "--enable-metrics"      = "true"
    "--enable-continuous-cloudwatch-log" = "true"
  }
  
  glue_version = "5.0"
  max_capacity = 1.0
  timeout      = 2880
  
  tags = {
    Environment = var.environment
    Project     = var.project_name
  }
}
