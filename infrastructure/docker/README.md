# AWS Glue 5.0 Local Development Environment

This directory contains the Docker configuration for setting up a local AWS Glue 5.0 development environment.

## Components

The local development environment consists of the following components:

1. **AWS Glue 5.0 Container**: Based on the official AWS Glue 5.0 Docker image, with additional tools for development.
2. **MinIO**: S3-compatible object storage for local development.
3. **Jupyter Lab**: Interactive development environment for Glue jobs.
4. **Spark History Server**: For viewing Spark job history.
5. **Livy Server**: For submitting Spark jobs via REST API.

## Setup

The environment is set up automatically by running:

```bash
./scripts/setup_local_env.sh
```

This script will:
1. Build and start the Docker containers
2. Configure AWS CLI to use MinIO for local S3 access
3. Create test buckets in MinIO

## Services and Ports

The following services are available:

| Service | URL | Description |
|---------|-----|-------------|
| Jupyter Lab | http://localhost:8888/?token=glue | Interactive development environment |
| Spark UI | http://localhost:4040 | Spark UI for running jobs |
| Spark History Server | http://localhost:18080 | History of completed Spark jobs |
| MinIO Console | http://localhost:9001 | S3-compatible storage UI (login: minioadmin/minioadmin) |
| Livy Server | http://localhost:8998 | REST API for Spark job submission |

## Using MinIO as S3

The environment is configured to use MinIO as a local S3 replacement. The following buckets are created automatically:

- `test-bronze-bucket`: For Bronze layer data
- `test-silver-bucket`: For Silver layer data
- `test-deployment-bucket`: For deployment artifacts

To use MinIO in your code:

### In Python/Boto3:

```python
import boto3

# Create S3 client for MinIO
s3 = boto3.client('s3',
                 endpoint_url='http://minio:9000',
                 aws_access_key_id='minioadmin',
                 aws_secret_access_key='minioadmin')

# List buckets
response = s3.list_buckets()
for bucket in response['Buckets']:
    print(bucket['Name'])
```

### In Spark:

```python
# Configure Spark to use MinIO
spark._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "http://minio:9000")
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "minioadmin")
spark._jsc.hadoopConfiguration().set("fs.s3a.path.style.access", "true")
spark._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
spark._jsc.hadoopConfiguration().set("fs.s3a.connection.ssl.enabled", "false")

# Read/write using S3 paths
df = spark.read.parquet("s3a://test-bronze-bucket/data/")
df.write.parquet("s3a://test-silver-bucket/data/")
```

## Customizing the Environment

You can customize the environment by modifying the following files:

- `Dockerfile`: Add additional dependencies or tools
- `docker-compose.yml`: Change port mappings, environment variables, or add services
- `entrypoint.sh`: Modify startup behavior
- `jupyter_notebook_config.py`: Configure Jupyter settings

## Troubleshooting

### Container fails to start

Check Docker logs:
```bash
docker logs aws-glue-datalake
```

### Cannot connect to MinIO

Verify MinIO is running:
```bash
docker ps | grep minio
```

### Spark jobs fail with S3 access errors

Ensure you're using the correct S3 endpoint and credentials in your code. For Spark jobs, make sure the Hadoop configuration is properly set for S3A access.
