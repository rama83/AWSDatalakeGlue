"""
Unit tests for the Bronze layer.
"""
import os
from unittest.mock import patch

import pandas as pd
import pytest

from src.bronze.ingest import BronzeIngestion
from src.bronze.validate import DataValidator


class TestBronzeIngestion:
    """Tests for the BronzeIngestion class."""

    def test_init(self):
        """Test initialization."""
        ingestion = BronzeIngestion(
            source_type="csv",
            target_bucket="test-bucket",
            target_prefix="test-prefix"
        )
        
        assert ingestion.source_type == "csv"
        assert ingestion.target_bucket == "test-bucket"
        assert ingestion.target_prefix == "test-prefix"

    def test_ingest_file_local(self, s3, sample_csv_file):
        """Test ingesting a local file."""
        ingestion = BronzeIngestion(
            source_type="csv",
            target_bucket="test-bronze-bucket",
            target_prefix="test-data"
        )
        
        # Ingest the file
        target_path = ingestion.ingest_file(
            source_path=sample_csv_file,
            partition_keys=["test"],
            metadata={"test_key": "test_value"}
        )
        
        # Check the result
        assert target_path.startswith("s3://test-bronze-bucket/test-data/test/")
        assert target_path.endswith(".csv")

    def test_ingest_dataframe(self, s3):
        """Test ingesting a DataFrame."""
        ingestion = BronzeIngestion(
            source_type="parquet",
            target_bucket="test-bronze-bucket",
            target_prefix="test-data"
        )
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": range(1, 6),
            "name": [f"Test {i}" for i in range(1, 6)]
        })
        
        # Ingest the DataFrame
        target_path = ingestion.ingest_dataframe(
            df=df,
            name="test_data",
            partition_keys=["test"],
            metadata={"test_key": "test_value"}
        )
        
        # Check the result
        assert target_path.startswith("s3://test-bronze-bucket/test-data/test/")
        assert target_path.endswith(".parquet")


class TestDataValidator:
    """Tests for the DataValidator class."""

    def test_validate_schema_valid(self):
        """Test schema validation with valid data."""
        validator = DataValidator()
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.0, 20.0, 30.0]
        })
        
        # Define the expected schema
        expected_schema = {
            "id": "int",
            "name": "string",
            "value": "float"
        }
        
        # Validate the schema
        results = validator.validate_schema(df, expected_schema)
        
        # Check the results
        assert results["valid"] is True
        assert len(results["missing_columns"]) == 0
        assert len(results["type_mismatches"]) == 0

    def test_validate_schema_invalid(self):
        """Test schema validation with invalid data."""
        validator = DataValidator()
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10.0, 20.0, 30.0]
        })
        
        # Define the expected schema with a missing column and type mismatch
        expected_schema = {
            "id": "string",  # Type mismatch
            "name": "string",
            "value": "float",
            "missing_column": "int"  # Missing column
        }
        
        # Validate the schema
        results = validator.validate_schema(df, expected_schema)
        
        # Check the results
        assert results["valid"] is False
        assert "missing_column" in results["missing_columns"]
        assert any(m["column"] == "id" for m in results["type_mismatches"])

    def test_check_data_quality(self):
        """Test data quality checks."""
        validator = DataValidator()
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "name": ["A", "B", "C", "D", None],  # Contains a null value
            "value": [10, 20, 30, 40, 200]  # Contains a value outside range
        })
        
        # Define quality rules
        rules = [
            {
                "column": "name",
                "check_type": "not_null",
                "parameters": {}
            },
            {
                "column": "value",
                "check_type": "range",
                "parameters": {"min": 0, "max": 100}
            }
        ]
        
        # Check data quality
        results = validator.check_data_quality(df, rules)
        
        # Check the results
        assert results["valid"] is False
        assert len(results["rule_violations"]) == 2
