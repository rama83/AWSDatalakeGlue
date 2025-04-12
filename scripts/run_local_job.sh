#!/bin/bash
# Run a Glue job locally
set -e

# Check if job name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <job_name> [job_args]"
    echo "Example: $0 ingest_to_bronze --source_path=data/input/sample.csv --source_type=csv"
    echo "Example: $0 bronze_to_silver --source_path=s3://test-bronze-bucket/data/sample.csv --table_name=test_table"
    exit 1
fi

JOB_NAME=$1
shift  # Remove the first argument (job_name)

# Determine the job type based on the file extension
if [ -f "src/pipelines/${JOB_NAME}.py" ]; then
    # Check if the file contains SparkContext
    if grep -q "SparkContext\|GlueContext" "src/pipelines/${JOB_NAME}.py"; then
        JOB_TYPE="spark"
    else
        JOB_TYPE="python"
    fi
else
    echo "Error: Job file src/pipelines/${JOB_NAME}.py not found"
    exit 1
fi

# Set up environment for MinIO
S3_ENDPOINT="http://minio:9000"
S3_ACCESS_KEY="minioadmin"
S3_SECRET_KEY="minioadmin"

# Run the job in the Docker container
if [ "$JOB_TYPE" == "spark" ]; then
    echo "Running Spark Glue job: ${JOB_NAME}"

    # Set up Spark configuration for S3/MinIO access
    SPARK_CONF="\
        --master local[*] \
        --conf spark.driver.memory=5g \
        --conf spark.executor.memory=5g \
        --conf spark.hadoop.fs.s3a.endpoint=${S3_ENDPOINT} \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.access.key=${S3_ACCESS_KEY} \
        --conf spark.hadoop.fs.s3a.secret.key=${S3_SECRET_KEY} \
        --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
        --conf spark.hadoop.fs.s3.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider \
        --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
        --conf spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog \
        --conf spark.sql.catalog.glue_catalog.warehouse=s3://test-silver-bucket/ \
        --conf spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog \
        --conf spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO \
        --conf spark.sql.catalog.glue_catalog.s3.endpoint=${S3_ENDPOINT} \
        --conf spark.sql.catalog.glue_catalog.s3.access-key=${S3_ACCESS_KEY} \
        --conf spark.sql.catalog.glue_catalog.s3.secret-key=${S3_SECRET_KEY} \
        --conf spark.sql.catalog.glue_catalog.s3.path-style-access=true"

    # Run the Spark job
    docker exec -it aws-glue-datalake bash -c "cd /home/glue_user/workspace && \
        export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY} && \
        export AWS_SECRET_ACCESS_KEY=${S3_SECRET_KEY} && \
        export AWS_REGION=us-east-1 && \
        /home/glue_user/spark/bin/spark-submit ${SPARK_CONF} \
        src/pipelines/${JOB_NAME}.py \
        --JOB_NAME=${JOB_NAME} \
        $@"
else
    echo "Running Python Shell job: ${JOB_NAME}"

    # Run the Python shell job
    docker exec -it aws-glue-datalake bash -c "cd /home/glue_user/workspace && \
        export AWS_ACCESS_KEY_ID=${S3_ACCESS_KEY} && \
        export AWS_SECRET_ACCESS_KEY=${S3_SECRET_KEY} && \
        export AWS_REGION=us-east-1 && \
        export PYTHONPATH=/home/glue_user/workspace && \
        python src/pipelines/${JOB_NAME}.py $@"
fi

# Print job completion message
echo "Job ${JOB_NAME} execution completed."
