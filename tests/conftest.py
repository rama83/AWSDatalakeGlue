"""
Pytest configuration for the AWS Data Lake Framework.
"""
import os
import tempfile
from pathlib import Path

import boto3
import pandas as pd
import pytest
from moto import mock_glue, mock_s3

# Set up test environment
os.environ["DATALAKE_ENVIRONMENT"] = "test"
os.environ["DATALAKE_S3_BRONZE_BUCKET"] = "test-bronze-bucket"
os.environ["DATALAKE_S3_SILVER_BUCKET"] = "test-silver-bucket"
os.environ["DATALAKE_S3TABLES_DATABASE_NAME"] = "test_silver_db"


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for boto3."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def s3(aws_credentials):
    """Mocked S3 service."""
    with mock_s3():
        # Create S3 client
        s3_client = boto3.client("s3", region_name="us-east-1")
        
        # Create test buckets
        for bucket in ["test-bronze-bucket", "test-silver-bucket"]:
            s3_client.create_bucket(Bucket=bucket)
        
        yield s3_client


@pytest.fixture(scope="function")
def glue(aws_credentials):
    """Mocked Glue service."""
    with mock_glue():
        # Create Glue client
        glue_client = boto3.client("glue", region_name="us-east-1")
        
        # Create test database
        glue_client.create_database(
            DatabaseInput={
                "Name": "test_silver_db",
                "Description": "Test database for S3Tables"
            }
        )
        
        yield glue_client


@pytest.fixture(scope="function")
def sample_csv_file():
    """Create a sample CSV file for testing."""
    # Create a temporary CSV file
    with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as temp_file:
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": range(1, 11),
            "name": [f"Name {i}" for i in range(1, 11)],
            "value": [i * 10 for i in range(1, 11)]
        })
        
        # Write to CSV
        df.to_csv(temp_file.name, index=False)
        
        yield temp_file.name
    
    # Clean up
    if os.path.exists(temp_file.name):
        os.remove(temp_file.name)


@pytest.fixture(scope="function")
def sample_parquet_file():
    """Create a sample Parquet file for testing."""
    # Create a temporary Parquet file
    with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": range(1, 11),
            "name": [f"Name {i}" for i in range(1, 11)],
            "value": [i * 10 for i in range(1, 11)]
        })
        
        # Write to Parquet
        df.to_parquet(temp_file.name, index=False)
        
        yield temp_file.name
    
    # Clean up
    if os.path.exists(temp_file.name):
        os.remove(temp_file.name)


@pytest.fixture(scope="function")
def sample_json_file():
    """Create a sample JSON file for testing."""
    # Create a temporary JSON file
    with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as temp_file:
        # Create a sample DataFrame
        df = pd.DataFrame({
            "id": range(1, 11),
            "name": [f"Name {i}" for i in range(1, 11)],
            "value": [i * 10 for i in range(1, 11)]
        })
        
        # Write to JSON
        df.to_json(temp_file.name, orient="records", lines=True)
        
        yield temp_file.name
    
    # Clean up
    if os.path.exists(temp_file.name):
        os.remove(temp_file.name)
