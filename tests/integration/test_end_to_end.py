"""
Integration tests for end-to-end data flow.
"""
import os
import tempfile
from unittest.mock import patch

import pandas as pd
import pytest

from src.bronze.ingest import BronzeIngestion
from src.bronze.validate import DataValidator
from src.silver.transform import DataTransformer
from src.silver.s3tables import S3TablesManager


@pytest.mark.integration
class TestBronzeToSilverFlow:
    """Integration tests for Bronze to Silver data flow."""

    @pytest.mark.parametrize("file_type", ["csv", "parquet", "json"])
    def test_ingest_validate_transform(self, s3, glue, file_type):
        """Test the full data flow from ingestion to transformation."""
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=f".{file_type}", delete=False) as temp_file:
            # Create a sample DataFrame
            df = pd.DataFrame({
                "id": range(1, 11),
                "name": [f"Name {i}" for i in range(1, 11)],
                "value": [i * 10 for i in range(1, 11)]
            })
            
            # Write to file
            if file_type == "csv":
                df.to_csv(temp_file.name, index=False)
            elif file_type == "parquet":
                df.to_parquet(temp_file.name, index=False)
            elif file_type == "json":
                df.to_json(temp_file.name, orient="records", lines=True)
            
            temp_path = temp_file.name
        
        try:
            # Step 1: Ingest to Bronze
            bronze_ingestion = BronzeIngestion(
                source_type=file_type,
                target_bucket="test-bronze-bucket",
                target_prefix="test-data"
            )
            
            bronze_path = bronze_ingestion.ingest_file(
                source_path=temp_path,
                partition_keys=["test"],
                metadata={"test_key": "test_value"}
            )
            
            # Step 2: Validate the data
            validator = DataValidator()
            expected_schema = {
                "id": "int",
                "name": "string",
                "value": "int"
            }
            
            validation_results = validator.validate_s3_data(
                s3_path=bronze_path,
                expected_schema=expected_schema
            )
            
            assert validation_results["schema_validation"]["valid"] is True
            
            # Step 3: Transform the data
            transformer = DataTransformer()
            transformations = [
                {
                    "type": "add_column",
                    "params": {"column": "doubled_value", "expression": "value * 2"}
                },
                {
                    "type": "rename_columns",
                    "params": {"column_map": {"name": "full_name"}}
                }
            ]
            
            silver_path = "s3://test-silver-bucket/test-table/data.parquet"
            transformation_results = transformer.transform_s3_data(
                source_s3_path=bronze_path,
                transformations=transformations,
                target_s3_path=silver_path
            )
            
            assert transformation_results["row_count"]["before"] == 10
            assert transformation_results["row_count"]["after"] == 10
            
            # Step 4: Create S3Table and write data
            s3tables_manager = S3TablesManager(database_name="test_silver_db")
            
            # Create the table
            columns = [
                {"Name": "id", "Type": "int"},
                {"Name": "full_name", "Type": "string"},
                {"Name": "value", "Type": "int"},
                {"Name": "doubled_value", "Type": "int"}
            ]
            
            s3tables_manager.create_table(
                table_name="test_table",
                s3_location="s3://test-silver-bucket/test-table/",
                columns=columns,
                description="Test table"
            )
            
            # Read the transformed data
            transformed_df = pd.read_parquet(silver_path)
            
            # Write to S3Table
            write_results = s3tables_manager.write_to_table(
                df=transformed_df,
                table_name="test_table",
                mode="append"
            )
            
            assert write_results["row_count"] == 10
            assert write_results["table"] == "test_silver_db.test_table"
        
        finally:
            # Clean up
            if os.path.exists(temp_path):
                os.remove(temp_path)
