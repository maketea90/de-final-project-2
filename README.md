# The Data Engineering Project

## Pipeline

This is an ETL pipeline designed to automate ingestion, transformation, and loading of data from a source database to a data warehouse, in the cloud. Each lambda function in the src directory is a specific step in the pipeline: 

- **Ingestion**: Automatically pulls new or updated data from the source database, uploading these new records as csv files to an AWS S3 bucket (ingestion bucket). Files are labelled by the current date and time. 

- **Processing**: Retrieves new data from the ingestion bucket and transforms it to comply with the warehouse star schema (dimension and fact tables). Uploads the transformed data to another AWS S3 bucket (processed bucket) in parquet format. 

- **Warehousing**: Retrieves the transformed data from the processed S3 bucket and loads it to the warehouse (an AWS RDS PostgreSQL database instance). 

Each lambda continuously logs progress in AWS CloudWatch. Database credentials and bucket names are stored in AWS Secrets Manager. 

## Cloud 

All cloud services used are provisioned with terraform (infrastructure as code). These include: 

- **S3 buckets** for the ingested data and processed data respectively, and an **S3 bucket** to store both the lambda function dependencies (lambda layer) and a file to track the most recent update (latest_update.json) 
- The three **lambda functions** themselves (ingestion, processing and warehousing) 
- An **AWS RDS Postgres database instance** (the warehouse) 
- A **scheduler** that invokes the ingestion lambda every 20 minutes
- Adding warehouse credentials to AWS Secrets Manager
- Appropriate **iam roles** and **iam policies/attachments** where necessary

## Testing

Each lambda function has been fully unit tested with pytest.

## Extensions

Possible extensions to the project in the future:

1. Serving the processed data over an EC2 hosted FastAPI
2. Creating a machine learning model which classifies the processed data

## Makefile

From the main directory...

To create a venv virtual environment and install the necessary dependencies run the terminal command: make make-build

To setup terraform run the command: make setup-terraform