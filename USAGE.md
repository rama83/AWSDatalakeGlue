# AWS Data Lake Framework Usage Guide

This guide provides detailed instructions on how to use the AWS Data Lake Framework based on the medallion architecture.

## Table of Contents

1. [Project Overview](#project-overview)
2. [Local Development Setup](#local-development-setup)
3. [Running Jobs Locally](#running-jobs-locally)
4. [Testing](#testing)
5. [Deploying to AWS](#deploying-to-aws)
6. [Using the Framework](#using-the-framework)
7. [Extending the Framework](#extending-the-framework)

## Project Overview

This framework implements a data lake on AWS using the medallion architecture:

- **Bronze Layer**: Raw data stored in S3
- **Silver Layer**: Processed and transformed data using AWS S3Tables

The framework uses AWS Glue 5.0 for ETL processing, with support for both Spark and Python shell jobs.

## Local Development Setup

### Prerequisites

- Docker and Docker Compose
- AWS CLI configured with appropriate permissions
- Python 3.9+

### Setup Steps

1. Clone the repository:
   ```
   git clone <repository-url>
   cd AwsDataLakeGlue
   ```

2. Set up the local environment:
   ```
   chmod +x scripts/setup_local_env.sh
   ./scripts/setup_local_env.sh
   ```

   This script will:
   - Create data directories for input and output
   - Build and start the Docker container with AWS Glue 5.0

3. Verify the setup:
   ```
   docker ps
   ```

   You should see the `aws-glue-datalake` container running.

## Running Jobs Locally

The framework includes a script to run Glue jobs locally:

```
chmod +x scripts/run_local_job.sh
./scripts/run_local_job.sh <job_name> [job_args]
```

### Examples

1. Run the Python shell example job:
   ```
   ./scripts/run_local_job.sh python_shell_example --source_path=data/input/sample.csv --source_type=csv --target_bucket=test-bronze-bucket --target_prefix=data
   ```

2. Run the Spark-based ingest to Bronze job:
   ```
   ./scripts/run_local_job.sh ingest_to_bronze --JOB_NAME=ingest_to_bronze --source_path=data/input/sample.csv --source_type=csv --target_bucket=test-bronze-bucket --target_prefix=data --partition_keys=
   ```

3. Run the Bronze to Silver job:
   ```
   ./scripts/run_local_job.sh bronze_to_silver --JOB_NAME=bronze_to_silver --source_path=s3://test-bronze-bucket/data/2023/01/01/123456/sample.csv --table_name=test_table --transformations_json='[{"type":"rename_columns","params":{"column_map":{"name":"full_name"}}}]' --partition_keys=year=2023,month=01 --validate_schema=true
   ```

## Testing

The framework includes a comprehensive test suite:

```
# Run unit tests
pytest tests/unit/

# Run integration tests
pytest tests/integration/

# Run all tests with coverage
pytest tests --cov=src
```

### Adding Test Data

You can add test data to the `data/input/` directory for local testing. The framework includes a sample CSV file at `data/input/sample.csv`.

## Deploying to AWS

### Prerequisites

- AWS CLI configured with appropriate permissions
- Terraform (optional, for infrastructure deployment)

### Deployment Steps

1. Create environment-specific configuration:
   ```
   cp config/settings.yaml config/settings_dev.yaml
   # Edit config/settings_dev.yaml with environment-specific values
   ```

2. Deploy infrastructure (if using Terraform):
   ```
   cd infrastructure/terraform
   terraform init
   terraform apply -var="environment=dev"
   ```

3. Deploy Glue jobs:
   ```
   chmod +x scripts/deploy.sh
   ./scripts/deploy.sh dev
   ```

   This script will:
   - Create a deployment package
   - Upload the package to S3
   - Create or update Glue jobs

## Using the Framework

### Bronze Layer Operations

The Bronze layer is responsible for ingesting raw data:

```python
from src.bronze.ingest import BronzeIngestion

# Initialize Bronze ingestion
bronze_ingestion = BronzeIngestion(
    source_type="csv",
    target_bucket="my-bronze-bucket",
    target_prefix="data"
)

# Ingest a file
bronze_path = bronze_ingestion.ingest_file(
    source_path="path/to/file.csv",
    partition_keys=["year=2023", "month=01"],
    metadata={"source": "example"}
)

# Ingest a DataFrame
import pandas as pd
df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"]})

bronze_path = bronze_ingestion.ingest_dataframe(
    df=df,
    name="example_data",
    partition_keys=["year=2023", "month=01"],
    metadata={"source": "example"}
)
```

### Data Validation

The framework includes data validation capabilities:

```python
from src.bronze.validate import DataValidator

# Initialize validator
validator = DataValidator()

# Define expected schema
expected_schema = {
    "id": "int",
    "name": "string",
    "value": "float"
}

# Validate schema
validation_results = validator.validate_s3_data(
    s3_path="s3://my-bronze-bucket/data/file.csv",
    expected_schema=expected_schema
)

# Check data quality
quality_rules = [
    {
        "column": "id",
        "check_type": "not_null",
        "parameters": {}
    },
    {
        "column": "value",
        "check_type": "range",
        "parameters": {"min": 0, "max": 100}
    }
]

quality_results = validator.check_data_quality(df, quality_rules)
```

### Silver Layer Operations

The Silver layer transforms data and stores it in S3Tables:

```python
from src.silver.transform import DataTransformer

# Initialize transformer
transformer = DataTransformer()

# Define transformations
transformations = [
    {
        "type": "rename_columns",
        "params": {"column_map": {"old_name": "new_name"}}
    },
    {
        "type": "drop_columns",
        "params": {"columns": ["to_drop"]}
    },
    {
        "type": "add_column",
        "params": {"column": "doubled_value", "expression": "value * 2"}
    }
]

# Transform data
transformation_results = transformer.transform_s3_data(
    source_s3_path="s3://my-bronze-bucket/data/file.csv",
    transformations=transformations,
    target_s3_path="s3://my-silver-bucket/transformed/file.parquet"
)
```

### S3Tables Integration

The framework integrates with AWS S3Tables:

```python
from src.silver.s3tables import S3TablesManager

# Initialize S3Tables manager
s3tables_manager = S3TablesManager(
    database_name="my_silver_db"
)

# Create a table
columns = [
    {"Name": "id", "Type": "int"},
    {"Name": "name", "Type": "string"},
    {"Name": "value", "Type": "double"}
]

s3tables_manager.create_table(
    table_name="my_table",
    s3_location="s3://my-silver-bucket/my-table/",
    columns=columns,
    partition_keys=[{"Name": "year", "Type": "string"}, {"Name": "month", "Type": "string"}],
    description="My S3Table"
)

# Write data to the table
import pandas as pd
df = pd.DataFrame({"id": [1, 2, 3], "name": ["A", "B", "C"], "value": [10.0, 20.0, 30.0]})

s3tables_manager.write_to_table(
    df=df,
    table_name="my_table",
    partition_values={"year": "2023", "month": "01"},
    mode="append"
)

# Read data from the table
df = s3tables_manager.read_from_table(
    table_name="my_table",
    partition_values={"year": "2023", "month": "01"},
    columns=["id", "name", "value"]
)
```

## Extending the Framework

### Adding New Transformations

To add a new transformation type:

1. Modify `src/silver/transform.py` to add a new transformation type in the `apply_transformations` method.

2. Example:
   ```python
   elif transform_type == "my_new_transform":
       # Implement the new transformation
       column = params.get("column")
       factor = params.get("factor", 1)
       
       if column in result_df.columns:
           result_df[column] = result_df[column] * factor
   ```

### Adding New Validation Rules

To add a new validation rule:

1. Modify `src/bronze/validate.py` to add a new check type in the `check_data_quality` method.

2. Example:
   ```python
   elif check_type == "my_new_check":
       threshold = parameters.get("threshold", 0)
       if df[column].mean() < threshold:
           violation = {
               "rule": rule,
               "message": f"Column '{column}' has mean value below threshold {threshold}"
           }
   ```

### Creating New Glue Jobs

To create a new Glue job:

1. Create a new Python script in the `src/pipelines/` directory.

2. For a Spark job, use the following template:
   ```python
   import sys
   from awsglue.context import GlueContext
   from awsglue.job import Job
   from awsglue.utils import getResolvedOptions
   from pyspark.context import SparkContext
   
   from src.utils.logging_utils import get_glue_logger
   
   # Initialize logger
   logger = get_glue_logger("my_new_job")
   
   def process_args():
       """Process job arguments."""
       args = getResolvedOptions(
           sys.argv,
           [
               "JOB_NAME",
               # Add your job parameters here
           ],
       )
       
       return args
   
   def run_job():
       """Run the Glue job."""
       # Get job arguments
       args = process_args()
       logger.info(f"Starting job with arguments: {args}")
       
       # Initialize Spark context
       sc = SparkContext()
       glueContext = GlueContext(sc)
       spark = glueContext.spark_session
       job = Job(glueContext)
       job.init(args["JOB_NAME"])
       
       try:
           # Implement your job logic here
           
           # Commit the job
           job.commit()
           
           return {
               "status": "success",
               # Add your job results here
           }
       
       except Exception as e:
           logger.error(f"Error in job execution: {str(e)}")
           raise
   
   if __name__ == "__main__":
       run_job()
   ```

3. For a Python shell job, use the following template:
   ```python
   import argparse
   import sys
   import os
   
   # Add the project root to the Python path
   sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
   
   from src.utils.logging_utils import setup_logger
   
   # Initialize logger
   logger = setup_logger("my_new_job")
   
   def parse_args():
       """Parse command line arguments."""
       parser = argparse.ArgumentParser(description="My new job")
       
       # Add your job parameters here
       parser.add_argument(
           "--param1",
           type=str,
           required=True,
           help="Description of param1"
       )
       
       return parser.parse_args()
   
   def run_job():
       """Run the job."""
       # Parse arguments
       args = parse_args()
       logger.info(f"Starting job with arguments: {args}")
       
       try:
           # Implement your job logic here
           
           return {
               "status": "success",
               # Add your job results here
           }
       
       except Exception as e:
           logger.error(f"Error in job execution: {str(e)}")
           raise
   
   if __name__ == "__main__":
       run_job()
   ```

4. Update the deployment script to include your new job.
