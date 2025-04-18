# AWS Data Lake Framework with Medallion Architecture

A production-grade data lake framework built on AWS using the medallion architecture pattern.

## Architecture Overview

This framework implements a data lake based on the medallion architecture with:

- **Bronze Layer**: Raw data stored in S3
- **Silver Layer**: Processed and transformed data using AWS S3Tables

## Components

- AWS Glue 5.0 for ETL processing
- S3 for storage
- S3Tables for the Silver layer
- Terraform for infrastructure as code
- Docker for local development and testing

## Getting Started

### Prerequisites

- AWS CLI configured with appropriate permissions
- Docker and Docker Compose
- Python 3.9+
- Terraform (optional, for infrastructure deployment)

### Local Development Setup

1. Clone the repository:
   ```
   git clone <repository-url>
   cd AwsDataLakeGlue
   ```

2. Set up the local environment:
   ```
   ./scripts/setup_local_env.sh
   ```

3. Run a local Glue job:
   ```
   ./scripts/run_local_job.sh <job-name>
   ```

### Running Tests

```
pytest tests/
```

### Deploying to AWS

1. Update configuration in `config/settings.yaml`

2. Deploy infrastructure (if using Terraform):
   ```
   cd infrastructure/terraform
   terraform init
   terraform apply
   ```

3. Deploy Glue jobs:
   ```
   ./scripts/deploy.sh
   ```

## Project Structure

- `src/bronze/`: Bronze layer processing modules
- `src/silver/`: Silver layer processing modules
- `src/pipelines/`: AWS Glue job definitions
- `src/utils/`: Utility functions
- `tests/`: Test suite
- `config/`: Configuration files
- `infrastructure/`: Infrastructure as code
- `data/`: Local data for development and testing

## Local Development with Docker

This project includes Docker configuration to simulate the AWS Glue environment locally:

```
cd infrastructure/docker
docker-compose up -d
```

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the MIT License - see the LICENSE file for details.
#
