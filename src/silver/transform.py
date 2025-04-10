"""
Data transformation module for the Silver layer.
"""
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from config.config import config
from src.utils.error_handling import TransformationError, handle_exceptions
from src.utils.logging_utils import setup_logger
from src.utils.s3 import S3Client

# Set up logger
logger = setup_logger(__name__)


class DataTransformer:
    """Class for transforming data for the Silver layer."""

    def __init__(self):
        """Initialize the data transformer."""
        self.s3_client = S3Client()

    @handle_exceptions(default_message="Error in data transformation")
    def apply_transformations(
        self, 
        df: pd.DataFrame, 
        transformations: List[Dict[str, Any]]
    ) -> pd.DataFrame:
        """
        Apply a series of transformations to a DataFrame.

        Args:
            df: Pandas DataFrame to transform
            transformations: List of transformation dictionaries

        Returns:
            Transformed DataFrame
        """
        result_df = df.copy()
        
        for transform in transformations:
            transform_type = transform.get("type")
            params = transform.get("params", {})
            
            if transform_type == "rename_columns":
                # Rename columns
                column_map = params.get("column_map", {})
                result_df = result_df.rename(columns=column_map)
            
            elif transform_type == "drop_columns":
                # Drop columns
                columns = params.get("columns", [])
                result_df = result_df.drop(columns=columns, errors="ignore")
            
            elif transform_type == "filter_rows":
                # Filter rows based on condition
                column = params.get("column")
                operator = params.get("operator")
                value = params.get("value")
                
                if column and operator and value is not None:
                    if operator == "eq":
                        result_df = result_df[result_df[column] == value]
                    elif operator == "ne":
                        result_df = result_df[result_df[column] != value]
                    elif operator == "gt":
                        result_df = result_df[result_df[column] > value]
                    elif operator == "lt":
                        result_df = result_df[result_df[column] < value]
                    elif operator == "gte":
                        result_df = result_df[result_df[column] >= value]
                    elif operator == "lte":
                        result_df = result_df[result_df[column] <= value]
                    elif operator == "in":
                        result_df = result_df[result_df[column].isin(value)]
                    elif operator == "not_in":
                        result_df = result_df[~result_df[column].isin(value)]
                    elif operator == "contains":
                        result_df = result_df[result_df[column].str.contains(value, na=False)]
                    elif operator == "not_contains":
                        result_df = result_df[~result_df[column].str.contains(value, na=False)]
            
            elif transform_type == "fill_nulls":
                # Fill null values
                columns = params.get("columns", result_df.columns.tolist())
                value = params.get("value")
                method = params.get("method")
                
                if method:
                    result_df[columns] = result_df[columns].fillna(method=method)
                elif value is not None:
                    result_df[columns] = result_df[columns].fillna(value)
            
            elif transform_type == "cast_types":
                # Cast column types
                type_map = params.get("type_map", {})
                for column, dtype in type_map.items():
                    if column in result_df.columns:
                        try:
                            result_df[column] = result_df[column].astype(dtype)
                        except Exception as e:
                            logger.warning(f"Could not cast column {column} to {dtype}: {str(e)}")
            
            elif transform_type == "apply_function":
                # Apply a function to columns
                columns = params.get("columns", [])
                function_name = params.get("function")
                args = params.get("args", [])
                kwargs = params.get("kwargs", {})
                
                if function_name and columns:
                    # Get the function from pandas
                    if hasattr(pd.Series, function_name):
                        for column in columns:
                            if column in result_df.columns:
                                func = getattr(result_df[column], function_name)
                                result_df[column] = func(*args, **kwargs)
            
            elif transform_type == "add_column":
                # Add a new column
                column = params.get("column")
                expression = params.get("expression")
                
                if column and expression:
                    # Use eval to apply the expression
                    result_df[column] = result_df.eval(expression)
            
            elif transform_type == "join":
                # Join with another DataFrame
                right_df_path = params.get("right_df_path")
                how = params.get("how", "inner")
                on = params.get("on")
                left_on = params.get("left_on")
                right_on = params.get("right_on")
                
                if right_df_path:
                    # Load the right DataFrame
                    right_df = self._load_dataframe(right_df_path)
                    
                    # Perform the join
                    if on:
                        result_df = result_df.merge(right_df, on=on, how=how)
                    elif left_on and right_on:
                        result_df = result_df.merge(
                            right_df, left_on=left_on, right_on=right_on, how=how
                        )
        
        return result_df

    @handle_exceptions(default_message="Error loading data for transformation")
    def transform_s3_data(
        self, 
        source_s3_path: str, 
        transformations: List[Dict[str, Any]],
        target_s3_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Transform data stored in S3.

        Args:
            source_s3_path: S3 path to the source data
            transformations: List of transformation dictionaries
            target_s3_path: Optional S3 path for the transformed data

        Returns:
            Dictionary with transformation results
        """
        import tempfile
        import os
        
        # Parse S3 paths
        source_parts = self.s3_client.parse_s3_path(source_s3_path)
        source_bucket = source_parts["bucket"]
        source_key = source_parts["key"]
        
        # Get file extension
        _, ext = os.path.splitext(source_key)
        
        # Create temporary files
        with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as source_temp_file:
            source_temp_path = source_temp_file.name
        
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as target_temp_file:
            target_temp_path = target_temp_file.name
        
        try:
            # Download the source file
            self.s3_client.download_file(source_bucket, source_key, source_temp_path)
            
            # Load the data
            df = self._load_dataframe(source_temp_path)
            
            # Apply transformations
            transformed_df = self.apply_transformations(df, transformations)
            
            # Save the transformed data
            transformed_df.to_parquet(target_temp_path, index=False)
            
            # Upload to S3 if target path is provided
            if target_s3_path:
                target_parts = self.s3_client.parse_s3_path(target_s3_path)
                target_bucket = target_parts["bucket"]
                target_key = target_parts["key"]
                
                self.s3_client.upload_file(
                    local_path=target_temp_path,
                    bucket=target_bucket,
                    s3_key=target_key,
                    metadata={
                        "source_path": source_s3_path,
                        "transformation_count": str(len(transformations))
                    }
                )
            
            return {
                "source_path": source_s3_path,
                "target_path": target_s3_path,
                "row_count": {
                    "before": len(df),
                    "after": len(transformed_df)
                },
                "column_count": {
                    "before": len(df.columns),
                    "after": len(transformed_df.columns)
                },
                "transformations_applied": len(transformations)
            }
        
        finally:
            # Clean up temporary files
            for path in [source_temp_path, target_temp_path]:
                if os.path.exists(path):
                    os.remove(path)

    def _load_dataframe(self, file_path: str) -> pd.DataFrame:
        """
        Load a DataFrame from a file.

        Args:
            file_path: Path to the file (local or S3)

        Returns:
            Pandas DataFrame
        """
        import os
        
        # Handle S3 paths
        if file_path.startswith("s3://"):
            s3_parts = self.s3_client.parse_s3_path(file_path)
            bucket = s3_parts["bucket"]
            key = s3_parts["key"]
            
            # Create a temporary file
            import tempfile
            _, ext = os.path.splitext(key)
            with tempfile.NamedTemporaryFile(suffix=ext, delete=False) as temp_file:
                temp_path = temp_file.name
            
            try:
                # Download the file
                self.s3_client.download_file(bucket, key, temp_path)
                return self._load_dataframe(temp_path)
            finally:
                # Clean up
                if os.path.exists(temp_path):
                    os.remove(temp_path)
        
        # Load based on file extension
        _, ext = os.path.splitext(file_path)
        ext = ext.lower()
        
        if ext == ".csv":
            return pd.read_csv(file_path)
        elif ext == ".parquet":
            return pd.read_parquet(file_path)
        elif ext in (".json", ".jsonl"):
            return pd.read_json(file_path, lines=True)
        else:
            raise TransformationError(f"Unsupported file format: {ext}")
