"""
Data validation module for the Bronze layer.
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from config.config import config
from src.utils.error_handling import ValidationError, handle_exceptions
from src.utils.logging_utils import setup_logger
from src.utils.s3 import S3Client

# Set up logger
logger = setup_logger(__name__)


class DataValidator:
    """Class for validating data in the Bronze layer."""

    def __init__(self):
        """Initialize the data validator."""
        self.s3_client = S3Client()

    @handle_exceptions(default_message="Error in data validation")
    def validate_schema(
        self, 
        df: pd.DataFrame, 
        expected_schema: Dict[str, str]
    ) -> Dict[str, Any]:
        """
        Validate the schema of a DataFrame.

        Args:
            df: Pandas DataFrame to validate
            expected_schema: Dictionary mapping column names to expected data types

        Returns:
            Dictionary with validation results
        """
        results = {
            "valid": True,
            "missing_columns": [],
            "extra_columns": [],
            "type_mismatches": []
        }
        
        # Check for missing columns
        for column in expected_schema:
            if column not in df.columns:
                results["missing_columns"].append(column)
                results["valid"] = False
        
        # Check for extra columns
        for column in df.columns:
            if column not in expected_schema:
                results["extra_columns"].append(column)
        
        # Check data types
        for column, expected_type in expected_schema.items():
            if column in df.columns:
                # Get the actual type
                actual_type = str(df[column].dtype)
                
                # Check if types match
                if not self._types_compatible(actual_type, expected_type):
                    results["type_mismatches"].append({
                        "column": column,
                        "expected_type": expected_type,
                        "actual_type": actual_type
                    })
                    results["valid"] = False
        
        return results

    @handle_exceptions(default_message="Error in data quality check")
    def check_data_quality(
        self, 
        df: pd.DataFrame, 
        rules: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Check data quality based on rules.

        Args:
            df: Pandas DataFrame to check
            rules: List of rule dictionaries with 'column', 'check_type', and 'parameters'

        Returns:
            Dictionary with quality check results
        """
        results = {
            "valid": True,
            "rule_violations": []
        }
        
        for rule in rules:
            column = rule.get("column")
            check_type = rule.get("check_type")
            parameters = rule.get("parameters", {})
            
            # Skip if column doesn't exist
            if column not in df.columns:
                continue
            
            violation = None
            
            # Apply the appropriate check
            if check_type == "not_null":
                null_count = df[column].isnull().sum()
                if null_count > 0:
                    violation = {
                        "rule": rule,
                        "message": f"Column '{column}' has {null_count} null values"
                    }
            
            elif check_type == "unique":
                duplicate_count = len(df) - df[column].nunique()
                if duplicate_count > 0:
                    violation = {
                        "rule": rule,
                        "message": f"Column '{column}' has {duplicate_count} duplicate values"
                    }
            
            elif check_type == "range":
                min_val = parameters.get("min")
                max_val = parameters.get("max")
                
                if min_val is not None and df[column].min() < min_val:
                    violation = {
                        "rule": rule,
                        "message": f"Column '{column}' has values below minimum {min_val}"
                    }
                elif max_val is not None and df[column].max() > max_val:
                    violation = {
                        "rule": rule,
                        "message": f"Column '{column}' has values above maximum {max_val}"
                    }
            
            elif check_type == "regex":
                pattern = parameters.get("pattern")
                if pattern:
                    import re
                    invalid_count = df[~df[column].astype(str).str.match(pattern)].shape[0]
                    if invalid_count > 0:
                        violation = {
                            "rule": rule,
                            "message": f"Column '{column}' has {invalid_count} values not matching pattern"
                        }
            
            # Add violation if found
            if violation:
                results["rule_violations"].append(violation)
                results["valid"] = False
        
        return results

    @handle_exceptions(default_message="Error loading data for validation")
    def validate_s3_data(
        self, 
        s3_path: str, 
        expected_schema: Optional[Dict[str, str]] = None,
        quality_rules: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """
        Validate data stored in S3.

        Args:
            s3_path: S3 path to the data
            expected_schema: Dictionary mapping column names to expected data types
            quality_rules: List of quality check rules

        Returns:
            Dictionary with validation results
        """
        import tempfile
        import os
        
        # Parse S3 path
        s3_parts = self.s3_client.parse_s3_path(s3_path)
        bucket = s3_parts["bucket"]
        key = s3_parts["key"]
        
        # Get file extension
        _, ext = os.path.splitext(key)
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Download the file
            self.s3_client.download_file(bucket, key, temp_path)
            
            # Load the data
            if ext.lower() == ".csv":
                df = pd.read_csv(temp_path)
            elif ext.lower() == ".parquet":
                df = pd.read_parquet(temp_path)
            elif ext.lower() in (".json", ".jsonl"):
                df = pd.read_json(temp_path, lines=True)
            else:
                raise ValidationError(f"Unsupported file format: {ext}")
            
            # Validate the data
            results = {"file_path": s3_path, "row_count": len(df)}
            
            if expected_schema:
                schema_results = self.validate_schema(df, expected_schema)
                results["schema_validation"] = schema_results
                
            if quality_rules:
                quality_results = self.check_data_quality(df, quality_rules)
                results["quality_validation"] = quality_results
            
            return results
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def _types_compatible(self, actual_type: str, expected_type: str) -> bool:
        """
        Check if data types are compatible.

        Args:
            actual_type: Actual data type
            expected_type: Expected data type

        Returns:
            True if types are compatible
        """
        # Normalize types for comparison
        actual_type = actual_type.lower()
        expected_type = expected_type.lower()
        
        # Direct match
        if actual_type == expected_type:
            return True
        
        # Check numeric types
        if expected_type in ("int", "integer"):
            return actual_type.startswith(("int", "uint"))
        
        if expected_type in ("float", "double", "numeric"):
            return (
                actual_type.startswith(("float", "double")) or 
                actual_type.startswith(("int", "uint"))
            )
        
        # Check string types
        if expected_type in ("str", "string", "text"):
            return actual_type in ("object", "string", "str")
        
        # Check date/time types
        if expected_type in ("date", "datetime", "timestamp"):
            return actual_type.startswith(("datetime", "timestamp"))
        
        # Check boolean types
        if expected_type in ("bool", "boolean"):
            return actual_type in ("bool", "boolean")
        
        return False
