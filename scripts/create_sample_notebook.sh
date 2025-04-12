#!/bin/bash
# Create a sample Jupyter notebook for Glue development
set -e

# Check if the container is running
if ! docker ps | grep -q aws-glue-datalake; then
    echo "AWS Glue container is not running. Starting local environment..."
    ./scripts/setup_local_env.sh
fi

# Create notebooks directory if it doesn't exist
mkdir -p notebooks

# Create a sample notebook
cat > notebooks/glue_sample.ipynb << 'EOF'
{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "1",
   "metadata": {},
   "source": [
    "# AWS Glue 5.0 Sample Notebook\n",
    "\n",
    "This notebook demonstrates how to use AWS Glue 5.0 for data processing in the local development environment."
   ]
  },
  {
   "cell_type": "markdown",
   "id": "2",
   "metadata": {},
   "source": [
    "## Initialize Spark Session"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "3",
   "metadata": {},
   "outputs": [],
   "source": [
    "import sys\n",
    "from pyspark.context import SparkContext\n",
    "from awsglue.context import GlueContext\n",
    "from awsglue.job import Job\n",
    "from awsglue.transforms import *\n",
    "from pyspark.sql import SparkSession\n",
    "\n",
    "# Initialize Spark and Glue context\n",
    "sc = SparkContext()\n",
    "glueContext = GlueContext(sc)\n",
    "spark = glueContext.spark_session\n",
    "job = Job(glueContext)\n",
    "\n",
    "print(f\"Spark version: {spark.version}\")\n",
    "print(f\"Glue version: {sc._jvm.com.amazonaws.services.glue.util.GlueVersionInfo.getGlueVersion()}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "4",
   "metadata": {},
   "source": [
    "## Configure S3 Access for MinIO"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "5",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Configure Spark to use MinIO as S3\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.endpoint\", \"http://minio:9000\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.access.key\", \"minioadmin\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.secret.key\", \"minioadmin\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.path.style.access\", \"true\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.impl\", \"org.apache.hadoop.fs.s3a.S3AFileSystem\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.connection.ssl.enabled\", \"false\")\n",
    "spark._jsc.hadoopConfiguration().set(\"fs.s3a.aws.credentials.provider\", \"org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "6",
   "metadata": {},
   "source": [
    "## Create Sample Data"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "7",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a sample DataFrame\n",
    "data = [(1, \"John\", 30), (2, \"Alice\", 25), (3, \"Bob\", 35), (4, \"Jane\", 28), (5, \"Mike\", 40)]\n",
    "columns = [\"id\", \"name\", \"age\"]\n",
    "df = spark.createDataFrame(data, columns)\n",
    "df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "8",
   "metadata": {},
   "source": [
    "## Write to Bronze Layer"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "9",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Write to Bronze layer (S3)\n",
    "bronze_path = \"s3a://test-bronze-bucket/sample-data/\"\n",
    "df.write.mode(\"overwrite\").parquet(bronze_path)\n",
    "print(f\"Data written to Bronze layer: {bronze_path}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "10",
   "metadata": {},
   "source": [
    "## Read from Bronze Layer"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "11",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Read from Bronze layer\n",
    "bronze_df = spark.read.parquet(bronze_path)\n",
    "bronze_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "12",
   "metadata": {},
   "source": [
    "## Transform Data for Silver Layer"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "13",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Apply transformations\n",
    "from pyspark.sql.functions import col, upper, expr\n",
    "\n",
    "# Transform data for Silver layer\n",
    "silver_df = bronze_df.withColumn(\"name\", upper(col(\"name\")))\\\n",
    "                     .withColumn(\"age_group\", expr(\"CASE WHEN age < 30 THEN 'Young' WHEN age < 40 THEN 'Middle' ELSE 'Senior' END\"))\n",
    "silver_df.show()"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "14",
   "metadata": {},
   "source": [
    "## Write to Silver Layer"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "15",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Write to Silver layer\n",
    "silver_path = \"s3a://test-silver-bucket/sample-data/\"\n",
    "silver_df.write.mode(\"overwrite\").parquet(silver_path)\n",
    "print(f\"Data written to Silver layer: {silver_path}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "16",
   "metadata": {},
   "source": [
    "## Using AWS Glue Data Catalog"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "17",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Create a Glue catalog database and table\n",
    "try:\n",
    "    spark.sql(\"CREATE DATABASE IF NOT EXISTS sample_db\")\n",
    "    \n",
    "    # Create a table in the Glue catalog\n",
    "    spark.sql(f\"\"\"\n",
    "    CREATE TABLE IF NOT EXISTS sample_db.sample_table\n",
    "    USING parquet\n",
    "    LOCATION '{silver_path}'\n",
    "    AS SELECT * FROM silver_df\n",
    "    \"\"\")\n",
    "    \n",
    "    print(\"Created table in Glue catalog\")\n",
    "    \n",
    "    # Query the table\n",
    "    spark.sql(\"SELECT * FROM sample_db.sample_table\").show()\n",
    "except Exception as e:\n",
    "    print(f\"Error creating Glue catalog table: {str(e)}\")"
   ]
  },
  {
   "cell_type": "markdown",
   "id": "18",
   "metadata": {},
   "source": [
    "## Clean Up"
   ]
  },
  {
   "cell_type": "code",
   "execution_count": null,
   "id": "19",
   "metadata": {},
   "outputs": [],
   "source": [
    "# Stop the Spark session\n",
    "spark.stop()"
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.9.0"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
EOF

echo "Sample notebook created at notebooks/glue_sample.ipynb"
echo "To start Jupyter Lab and open this notebook, run: ./scripts/start_jupyter.sh"
