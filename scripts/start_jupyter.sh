#!/bin/bash
# Start Jupyter Lab for interactive development
set -e

# Check if the container is running
if ! docker ps | grep -q aws-glue-datalake; then
    echo "AWS Glue container is not running. Starting local environment..."
    ./scripts/setup_local_env.sh
fi

# Print Jupyter Lab access information
echo "======================================================="
echo "Jupyter Lab is available at: http://localhost:8888/?token=glue"
echo "======================================================="
echo "You can use this notebook to interactively develop and test your Glue jobs."
echo "To access AWS services locally, use the following code in your notebook:"
echo ""
echo "import boto3"
echo "s3 = boto3.client('s3', endpoint_url='http://minio:9000',"
echo "                 aws_access_key_id='minioadmin',"
echo "                 aws_secret_access_key='minioadmin')"
echo ""
echo "# List buckets"
echo "response = s3.list_buckets()"
echo "for bucket in response['Buckets']:"
echo "    print(bucket['Name'])"
echo "======================================================="

# Open browser to Jupyter Lab
if command -v xdg-open &> /dev/null; then
    xdg-open http://localhost:8888/?token=glue
elif command -v open &> /dev/null; then
    open http://localhost:8888/?token=glue
elif command -v start &> /dev/null; then
    start http://localhost:8888/?token=glue
else
    echo "Could not automatically open browser. Please open the URL manually."
fi
