"""
Unit tests for the Silver layer.
"""
import os
from unittest.mock import patch, MagicMock

import pandas as pd
import pytest

from src.silver.transform import DataTransformer
from src.silver.s3tables import S3TablesManager


class TestDataTransformer:
    """Tests for the DataTransformer class."""

    def test_apply_transformations(self):
        """Test applying transformations to a DataFrame."""
        transformer = DataTransformer()
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3, 4, 5],
            "old_name": ["A", "B", "C", "D", "E"],
            "value": [10, 20, 30, 40, 50],
            "to_drop": [1, 2, 3, 4, 5]
        })
        
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
                "type": "filter_rows",
                "params": {"column": "value", "operator": "gt", "value": 20}
            },
            {
                "type": "add_column",
                "params": {"column": "doubled_value", "expression": "value * 2"}
            }
        ]
        
        # Apply transformations
        result_df = transformer.apply_transformations(df, transformations)
        
        # Check the results
        assert "new_name" in result_df.columns
        assert "old_name" not in result_df.columns
        assert "to_drop" not in result_df.columns
        assert len(result_df) == 3  # Filtered to values > 20
        assert "doubled_value" in result_df.columns
        assert result_df["doubled_value"].tolist() == [60, 80, 100]

    @patch("src.silver.transform.DataTransformer._load_dataframe")
    def test_transform_s3_data(self, mock_load_dataframe, s3):
        """Test transforming data from S3."""
        transformer = DataTransformer()
        
        # Create a test DataFrame
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"],
            "value": [10, 20, 30]
        })
        
        # Mock the _load_dataframe method
        mock_load_dataframe.return_value = df
        
        # Define transformations
        transformations = [
            {
                "type": "add_column",
                "params": {"column": "doubled_value", "expression": "value * 2"}
            }
        ]
        
        # Transform the data
        result = transformer.transform_s3_data(
            source_s3_path="s3://test-bronze-bucket/test.csv",
            transformations=transformations,
            target_s3_path="s3://test-silver-bucket/test.parquet"
        )
        
        # Check the results
        assert result["source_path"] == "s3://test-bronze-bucket/test.csv"
        assert result["target_path"] == "s3://test-silver-bucket/test.parquet"
        assert result["row_count"]["before"] == 3
        assert result["row_count"]["after"] == 3
        assert result["transformations_applied"] == 1


class TestS3TablesManager:
    """Tests for the S3TablesManager class."""

    @patch("boto3.Session")
    def test_init(self, mock_session):
        """Test initialization."""
        # Mock the session and clients
        mock_session.return_value.client.return_value = MagicMock()
        
        # Initialize the manager
        manager = S3TablesManager(
            database_name="test_db",
            catalog_id="123456789012"
        )
        
        # Check the initialization
        assert manager.database_name == "test_db"
        assert manager.catalog_id == "123456789012"

    @patch("boto3.Session")
    def test_create_table(self, mock_session, glue):
        """Test creating a table."""
        # Mock the session
        mock_session.return_value = MagicMock()
        mock_session.return_value.client.return_value = glue
        
        # Initialize the manager
        manager = S3TablesManager(database_name="test_silver_db")
        
        # Define columns
        columns = [
            {"Name": "id", "Type": "int"},
            {"Name": "name", "Type": "string"},
            {"Name": "value", "Type": "double"}
        ]
        
        # Create the table
        manager.create_table(
            table_name="test_table",
            s3_location="s3://test-silver-bucket/test-table/",
            columns=columns,
            table_type="EXTERNAL_TABLE",
            description="Test table"
        )
        
        # Check that the table was created
        response = glue.get_table(
            DatabaseName="test_silver_db",
            Name="test_table"
        )
        
        assert response["Table"]["Name"] == "test_table"
        assert response["Table"]["StorageDescriptor"]["Location"] == "s3://test-silver-bucket/test-table/"
        assert len(response["Table"]["StorageDescriptor"]["Columns"]) == 3
