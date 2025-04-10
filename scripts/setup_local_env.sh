#!/bin/bash
# Setup local development environment

# Create data directories if they don't exist
mkdir -p data/input
mkdir -p data/output

# Build and start the Docker container
cd infrastructure/docker
docker-compose up -d

echo "Local environment is ready!"
echo "To run a Glue job locally, use: ./scripts/run_local_job.sh <job_name>"
echo "To access the container shell, use: docker exec -it aws-glue-datalake bash"
