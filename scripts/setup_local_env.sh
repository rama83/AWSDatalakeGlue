#!/bin/bash
# Setup local development environment
set -e

echo "Setting up local AWS Glue development environment..."

# Create data directories if they don't exist
mkdir -p data/input
mkdir -p data/output

# Make scripts executable
chmod +x scripts/*.sh

# Build and start the Docker containers
cd infrastructure/docker
docker-compose build
docker-compose up -d

# Wait for services to be ready
echo "Waiting for services to start..."
sleep 5

# Configure AWS CLI to use MinIO for local development
echo "Configuring AWS CLI for local development..."
cat > ~/.aws/minio_config << EOF
[profile minio]
region = us-east-2
output = json
s3 =
    endpoint_url = http://localhost:9000
    signature_version = s3v4
    addressing_style = path
EOF

echo "Local environment is ready!"
echo "======================================================="
echo "Services available:"
echo "Spark UI:           http://localhost:4040"
echo "Spark History:      http://localhost:18080"
echo "Jupyter Lab:        http://localhost:8888/?token=glue"
echo "MinIO Console:      http://localhost:9001"
echo "======================================================="
echo "To run a Glue job locally:  ./scripts/run_local_job.sh <job_name>"
echo "To access the container shell: docker exec -it aws-glue-datalake bash"
echo "To use MinIO as local S3:    export AWS_PROFILE=minio"
echo "======================================================="
