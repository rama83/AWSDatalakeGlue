"""
S3Tables integration for the Silver layer.
"""
from typing import Any, Dict, List, Optional, Union

import boto3
import pandas as pd

from config.config import config
from src.utils.error_handling import handle_exceptions
from src.utils.logging_utils import setup_logger
from src.utils.s3 import S3Client

# Set up logger
logger = setup_logger(__name__)


class S3TablesManager:
    """Class for managing S3Tables in the Silver layer."""

    def __init__(
        self,
        database_name: Optional[str] = None,
        catalog_id: Optional[str] = None,
    ):
        """
        Initialize the S3Tables manager.

        Args:
            database_name: Glue database name
            catalog_id: AWS Glue Catalog ID (account ID)
        """
        self.database_name = database_name or config.get("s3tables", "database_name")
        self.catalog_id = catalog_id or config.get("s3tables", "catalog_id")
        
        # Initialize clients
        self.s3_client = S3Client()
        
        # Create AWS session
        session = boto3.Session(
            region_name=config.get("aws", "region"),
            profile_name=config.get("aws", "profile")
        )
        
        # Initialize Glue client
        self.glue_client = session.client("glue")
        
        logger.info(f"Initialized S3Tables manager for database {self.database_name}")

    @handle_exceptions(default_message="Error creating S3Table")
    def create_table(
        self,
        table_name: str,
        s3_location: str,
        columns: List[Dict[str, str]],
        partition_keys: Optional[List[Dict[str, str]]] = None,
        table_type: str = "EXTERNAL_TABLE",
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new S3Table in the Glue Data Catalog.

        Args:
            table_name: Name of the table
            s3_location: S3 location for the table data
            columns: List of column definitions (name, type)
            partition_keys: List of partition key definitions
            table_type: Table type (EXTERNAL_TABLE or GOVERNED)
            description: Table description

        Returns:
            Response from the Glue CreateTable API
        """
        # Ensure the database exists
        self._ensure_database_exists()
        
        # Prepare storage descriptor
        storage_descriptor = {
            "Columns": columns,
            "Location": s3_location,
            "InputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
            "OutputFormat": "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
            "Compressed": True,
            "SerdeInfo": {
                "SerializationLibrary": "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
                "Parameters": {"serialization.format": "1"}
            },
            "StoredAsSubDirectories": False
        }
        
        # Prepare table input
        table_input = {
            "Name": table_name,
            "StorageDescriptor": storage_descriptor,
            "TableType": table_type,
            "Parameters": {
                "classification": "parquet",
                "typeOfData": "file",
                "EXTERNAL": "TRUE",
                "s3tables:governed": "true" if table_type == "GOVERNED" else "false"
            }
        }
        
        # Add partition keys if provided
        if partition_keys:
            table_input["PartitionKeys"] = partition_keys
        
        # Add description if provided
        if description:
            table_input["Description"] = description
        
        # Create the table
        response = self.glue_client.create_table(
            DatabaseName=self.database_name,
            TableInput=table_input,
            CatalogId=self.catalog_id if self.catalog_id else None
        )
        
        logger.info(f"Created S3Table {self.database_name}.{table_name} at {s3_location}")
        return response

    @handle_exceptions(default_message="Error updating S3Table")
    def update_table(
        self,
        table_name: str,
        columns: Optional[List[Dict[str, str]]] = None,
        partition_keys: Optional[List[Dict[str, str]]] = None,
        description: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an existing S3Table in the Glue Data Catalog.

        Args:
            table_name: Name of the table
            columns: List of column definitions (name, type)
            partition_keys: List of partition key definitions
            description: Table description

        Returns:
            Response from the Glue UpdateTable API
        """
        # Get the current table definition
        table = self.glue_client.get_table(
            DatabaseName=self.database_name,
            Name=table_name,
            CatalogId=self.catalog_id if self.catalog_id else None
        )["Table"]
        
        # Create a copy of the table input
        table_input = {
            "Name": table["Name"],
            "StorageDescriptor": table["StorageDescriptor"],
            "TableType": table["TableType"],
            "Parameters": table.get("Parameters", {})
        }
        
        # Update columns if provided
        if columns:
            table_input["StorageDescriptor"]["Columns"] = columns
        
        # Update partition keys if provided
        if partition_keys:
            table_input["PartitionKeys"] = partition_keys
        elif "PartitionKeys" in table:
            table_input["PartitionKeys"] = table["PartitionKeys"]
        
        # Update description if provided
        if description:
            table_input["Description"] = description
        elif "Description" in table:
            table_input["Description"] = table["Description"]
        
        # Update the table
        response = self.glue_client.update_table(
            DatabaseName=self.database_name,
            TableInput=table_input,
            CatalogId=self.catalog_id if self.catalog_id else None
        )
        
        logger.info(f"Updated S3Table {self.database_name}.{table_name}")
        return response

    @handle_exceptions(default_message="Error writing to S3Table")
    def write_to_table(
        self,
        df: pd.DataFrame,
        table_name: str,
        partition_values: Optional[Dict[str, str]] = None,
        mode: str = "append",
    ) -> Dict[str, Any]:
        """
        Write a DataFrame to an S3Table.

        Args:
            df: Pandas DataFrame to write
            table_name: Name of the target table
            partition_values: Dictionary of partition key-value pairs
            mode: Write mode ('append', 'overwrite')

        Returns:
            Dictionary with write operation results
        """
        import tempfile
        import os
        
        # Get the table definition
        table = self.glue_client.get_table(
            DatabaseName=self.database_name,
            Name=table_name,
            CatalogId=self.catalog_id if self.catalog_id else None
        )["Table"]
        
        # Get the table location
        base_location = table["StorageDescriptor"]["Location"]
        
        # Build the partition path if needed
        partition_path = ""
        if partition_values and "PartitionKeys" in table:
            partition_parts = []
            for partition_key in table["PartitionKeys"]:
                key_name = partition_key["Name"]
                if key_name in partition_values:
                    partition_parts.append(f"{key_name}={partition_values[key_name]}")
            
            if partition_parts:
                partition_path = "/".join(partition_parts)
        
        # Determine the target S3 location
        if partition_path:
            target_location = f"{base_location}/{partition_path}"
        else:
            target_location = base_location
        
        # Create a temporary file
        with tempfile.NamedTemporaryFile(suffix=".parquet", delete=False) as temp_file:
            temp_path = temp_file.name
        
        try:
            # Write DataFrame to the temporary file
            df.to_parquet(temp_path, index=False)
            
            # Parse the target location
            s3_parts = self.s3_client.parse_s3_path(target_location)
            target_bucket = s3_parts["bucket"]
            target_prefix = s3_parts["key"]
            
            # Generate a unique file name
            import uuid
            file_name = f"{uuid.uuid4()}.parquet"
            target_key = f"{target_prefix}/{file_name}" if target_prefix else file_name
            
            # Upload the file to S3
            self.s3_client.upload_file(
                local_path=temp_path,
                bucket=target_bucket,
                s3_key=target_key
            )
            
            # Create or update the partition if needed
            if partition_values and "PartitionKeys" in table:
                try:
                    # Check if partition exists
                    self.glue_client.get_partition(
                        DatabaseName=self.database_name,
                        TableName=table_name,
                        PartitionValues=list(partition_values.values()),
                        CatalogId=self.catalog_id if self.catalog_id else None
                    )
                    
                    # Partition exists, update it if needed
                    if mode == "overwrite":
                        self._update_partition(
                            table_name=table_name,
                            partition_values=partition_values,
                            location=target_location
                        )
                except Exception:
                    # Partition doesn't exist, create it
                    self._create_partition(
                        table_name=table_name,
                        partition_values=partition_values,
                        location=target_location
                    )
            
            return {
                "table": f"{self.database_name}.{table_name}",
                "location": f"{target_location}/{file_name}",
                "row_count": len(df),
                "partition": partition_path if partition_path else None
            }
        
        finally:
            # Clean up the temporary file
            if os.path.exists(temp_path):
                os.remove(temp_path)

    @handle_exceptions(default_message="Error reading from S3Table")
    def read_from_table(
        self,
        table_name: str,
        partition_values: Optional[Dict[str, str]] = None,
        columns: Optional[List[str]] = None,
        filter_expression: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Read data from an S3Table.

        Args:
            table_name: Name of the source table
            partition_values: Dictionary of partition key-value pairs
            columns: List of columns to select
            filter_expression: Filter expression

        Returns:
            Pandas DataFrame with the table data
        """
        # Import AWS Data Wrangler for S3Tables support
        try:
            import awswrangler as wr
        except ImportError:
            logger.error("AWS Data Wrangler is required for S3Tables support")
            raise ImportError("AWS Data Wrangler is required for S3Tables support")
        
        # Build the database.table name
        table_identifier = f"{self.database_name}.{table_name}"
        
        # Read the data using AWS Data Wrangler
        df = wr.s3.read_table(
            table=table_identifier,
            partition_filter=partition_values,
            columns=columns,
            filter_expression=filter_expression,
            catalog_id=self.catalog_id if self.catalog_id else None
        )
        
        logger.info(
            f"Read {len(df)} rows from S3Table {table_identifier}"
            f"{' with partition filter' if partition_values else ''}"
        )
        
        return df

    def _ensure_database_exists(self) -> None:
        """
        Ensure the Glue database exists, creating it if necessary.
        """
        try:
            # Check if the database exists
            self.glue_client.get_database(
                Name=self.database_name,
                CatalogId=self.catalog_id if self.catalog_id else None
            )
        except self.glue_client.exceptions.EntityNotFoundException:
            # Database doesn't exist, create it
            self.glue_client.create_database(
                DatabaseInput={
                    "Name": self.database_name,
                    "Description": f"Database for S3Tables in the Silver layer"
                },
                CatalogId=self.catalog_id if self.catalog_id else None
            )
            logger.info(f"Created Glue database {self.database_name}")

    def _create_partition(
        self,
        table_name: str,
        partition_values: Dict[str, str],
        location: str,
    ) -> None:
        """
        Create a partition in an S3Table.

        Args:
            table_name: Name of the table
            partition_values: Dictionary of partition key-value pairs
            location: S3 location for the partition
        """
        # Get the table definition
        table = self.glue_client.get_table(
            DatabaseName=self.database_name,
            Name=table_name,
            CatalogId=self.catalog_id if self.catalog_id else None
        )["Table"]
        
        # Create a storage descriptor for the partition
        storage_descriptor = dict(table["StorageDescriptor"])
        storage_descriptor["Location"] = location
        
        # Create the partition
        self.glue_client.create_partition(
            DatabaseName=self.database_name,
            TableName=table_name,
            PartitionInput={
                "Values": list(partition_values.values()),
                "StorageDescriptor": storage_descriptor
            },
            CatalogId=self.catalog_id if self.catalog_id else None
        )
        
        logger.info(
            f"Created partition {partition_values} in "
            f"S3Table {self.database_name}.{table_name}"
        )

    def _update_partition(
        self,
        table_name: str,
        partition_values: Dict[str, str],
        location: str,
    ) -> None:
        """
        Update a partition in an S3Table.

        Args:
            table_name: Name of the table
            partition_values: Dictionary of partition key-value pairs
            location: S3 location for the partition
        """
        # Get the table definition
        table = self.glue_client.get_table(
            DatabaseName=self.database_name,
            Name=table_name,
            CatalogId=self.catalog_id if self.catalog_id else None
        )["Table"]
        
        # Create a storage descriptor for the partition
        storage_descriptor = dict(table["StorageDescriptor"])
        storage_descriptor["Location"] = location
        
        # Update the partition
        self.glue_client.update_partition(
            DatabaseName=self.database_name,
            TableName=table_name,
            PartitionValueList=list(partition_values.values()),
            PartitionInput={
                "Values": list(partition_values.values()),
                "StorageDescriptor": storage_descriptor
            },
            CatalogId=self.catalog_id if self.catalog_id else None
        )
        
        logger.info(
            f"Updated partition {partition_values} in "
            f"S3Table {self.database_name}.{table_name}"
        )
