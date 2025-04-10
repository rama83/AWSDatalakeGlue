#!/bin/bash
# Deploy AWS Glue jobs and resources

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo "Error: AWS CLI is not installed"
    exit 1
fi

# Check if environment is provided
if [ -z "$1" ]; then
    echo "Usage: $0 <environment>"
    echo "Example: $0 dev"
    exit 1
fi

ENVIRONMENT=$1
CONFIG_FILE="config/settings_${ENVIRONMENT}.yaml"

# Check if config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Error: Config file $CONFIG_FILE not found"
    exit 1
fi

echo "Deploying to environment: $ENVIRONMENT"

# Create a deployment package
echo "Creating deployment package..."
mkdir -p dist
zip -r dist/datalake_package.zip src config -x "*.pyc" "__pycache__/*"

# Upload the package to S3
echo "Uploading package to S3..."
S3_BUCKET=$(grep -A1 "deployment:" "$CONFIG_FILE" | grep "bucket:" | awk '{print $2}')
S3_PREFIX=$(grep -A2 "deployment:" "$CONFIG_FILE" | grep "prefix:" | awk '{print $2}')

if [ -z "$S3_BUCKET" ] || [ -z "$S3_PREFIX" ]; then
    echo "Error: S3 bucket or prefix not found in config file"
    exit 1
fi

aws s3 cp dist/datalake_package.zip "s3://${S3_BUCKET}/${S3_PREFIX}/datalake_package.zip"

# Deploy Glue jobs
echo "Deploying Glue jobs..."

# Get job configurations from the config file
JOBS_CONFIG=$(grep -A100 "glue_jobs:" "$CONFIG_FILE" | grep -B100 -m1 "^[a-z]" | grep -v "^[a-z]" | grep -v "glue_jobs:")

# Parse and deploy each job
echo "$JOBS_CONFIG" | while IFS= read -r line; do
    if [[ $line == *":"* && $line != *"#"* ]]; then
        JOB_NAME=$(echo "$line" | cut -d':' -f1 | tr -d ' ')
        
        echo "Deploying job: $JOB_NAME"
        
        # Extract job parameters
        JOB_TYPE=$(echo "$JOBS_CONFIG" | grep -A20 "$JOB_NAME:" | grep "type:" | head -1 | awk '{print $2}')
        SCRIPT_LOCATION=$(echo "$JOBS_CONFIG" | grep -A20 "$JOB_NAME:" | grep "script_location:" | head -1 | awk '{print $2}')
        WORKER_TYPE=$(echo "$JOBS_CONFIG" | grep -A20 "$JOB_NAME:" | grep "worker_type:" | head -1 | awk '{print $2}')
        NUM_WORKERS=$(echo "$JOBS_CONFIG" | grep -A20 "$JOB_NAME:" | grep "number_of_workers:" | head -1 | awk '{print $2}')
        
        # Create or update the Glue job
        if [ "$JOB_TYPE" == "spark" ]; then
            aws glue create-job \
                --name "${ENVIRONMENT}_${JOB_NAME}" \
                --role "AWSGlueServiceRole" \
                --command "Name=glueetl,ScriptLocation=s3://${S3_BUCKET}/${S3_PREFIX}/${SCRIPT_LOCATION}" \
                --default-arguments "{\"--job-language\":\"python\",\"--class\":\"GlueApp\"}" \
                --glue-version "5.0" \
                --worker-type "$WORKER_TYPE" \
                --number-of-workers "$NUM_WORKERS" \
                --max-retries 0 \
                --timeout 2880 || \
            aws glue update-job \
                --job-name "${ENVIRONMENT}_${JOB_NAME}" \
                --job-update "Role=AWSGlueServiceRole,Command={Name=glueetl,ScriptLocation=s3://${S3_BUCKET}/${S3_PREFIX}/${SCRIPT_LOCATION}},DefaultArguments={\"--job-language\":\"python\",\"--class\":\"GlueApp\"},GlueVersion=5.0,WorkerType=$WORKER_TYPE,NumberOfWorkers=$NUM_WORKERS,Timeout=2880,MaxRetries=0"
        else
            aws glue create-job \
                --name "${ENVIRONMENT}_${JOB_NAME}" \
                --role "AWSGlueServiceRole" \
                --command "Name=pythonshell,ScriptLocation=s3://${S3_BUCKET}/${S3_PREFIX}/${SCRIPT_LOCATION}" \
                --glue-version "5.0" \
                --max-capacity 1.0 \
                --max-retries 0 \
                --timeout 2880 || \
            aws glue update-job \
                --job-name "${ENVIRONMENT}_${JOB_NAME}" \
                --job-update "Role=AWSGlueServiceRole,Command={Name=pythonshell,ScriptLocation=s3://${S3_BUCKET}/${S3_PREFIX}/${SCRIPT_LOCATION}},GlueVersion=5.0,MaxCapacity=1.0,Timeout=2880,MaxRetries=0"
        fi
    fi
done

echo "Deployment completed successfully!"
