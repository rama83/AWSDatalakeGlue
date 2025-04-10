#!/bin/bash
# Run a Glue job locally

# Check if job name is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <job_name> [job_args]"
    echo "Example: $0 ingest_to_bronze --source_path=data/input/sample.csv --source_type=csv"
    exit 1
fi

JOB_NAME=$1
shift  # Remove the first argument (job_name)

# Determine the job type based on the file extension
if [ -f "src/pipelines/${JOB_NAME}.py" ]; then
    # Check if the file contains SparkContext
    if grep -q "SparkContext" "src/pipelines/${JOB_NAME}.py"; then
        JOB_TYPE="spark"
    else
        JOB_TYPE="python"
    fi
else
    echo "Error: Job file src/pipelines/${JOB_NAME}.py not found"
    exit 1
fi

# Run the job in the Docker container
if [ "$JOB_TYPE" == "spark" ]; then
    echo "Running Spark Glue job: ${JOB_NAME}"
    docker exec -it aws-glue-datalake spark-submit \
        --master local[*] \
        --conf spark.driver.memory=2g \
        --conf spark.executor.memory=2g \
        --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
        --conf spark.hadoop.fs.s3a.path.style.access=true \
        --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
        --conf spark.hadoop.fs.s3a.access.key=minioadmin \
        --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
        src/pipelines/${JOB_NAME}.py \
        --JOB_NAME=${JOB_NAME} \
        "$@"
else
    echo "Running Python Shell job: ${JOB_NAME}"
    docker exec -it aws-glue-datalake python \
        src/pipelines/${JOB_NAME}.py \
        "$@"
fi
