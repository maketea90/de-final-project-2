from src.lambda_ingestion import lambda_ingestion
from src.lambda_processing import lambda_processing
from src.lambda_warehousing import lambda_warehousing
from moto import mock_aws
import boto3
import pytest
import os

@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "us-east-1"


@pytest.fixture(scope="function")
def mocked_aws(aws_credentials):
    """
    Mock all AWS interactions
    Requires you to create your own boto3 clients
    """
    with mock_aws():
        yield

@pytest.mark.skip(reason="no reason")
def test_ingestion_lambda_uploads_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket="nc-lambda-bucket-joe-final-project-2025")
    lambda_ingestion({}, {})
    s3_client = boto3.client('s3')
    # prefix = 'sales_order/'  
    result = s3_client.list_objects(Bucket='nc-joe-ingestion-bucket-2025')
    result2 = s3_client.list_objects(Bucket='nc-lambda-bucket-joe-final-project-2025')
    print(result, '\n'*2, result2)
    assert False

def test_processing_lambda_uploads_processed_data_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket="nc-lambda-bucket-joe-final-project-2025")
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    lambda_ingestion({}, {})
    latest_update = lambda_processing({}, {})
    s3_client = boto3.client('s3')
    for table in ['sales_order', 'staff', 'department']:
        result = s3_client.get_object(Bucket='nc-joe-ingestion-bucket-2025', Key=f'{table}/{latest_update[table]}.csv')
        print(result)
    assert False

@pytest.mark.skip(reason="no reason")
def test_rds_behaviour(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket='nc-lambda-bucket-joe-final-project-2025')
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    
    rds_client = boto3.client("rds")
    rds_client.create_db_instance(DBName='warehouse', Port=8080, MasterUsername='username',
    MasterUserPassword='password', DBInstanceIdentifier='warehouse', Engine='postgres', DBInstanceClass='db.m5.small')
    description = rds_client.describe_db_instances(DBInstanceIdentifier='warehouse')
    hostname = description['DBInstances'][0]['Endpoint']['Address']
    log_files = rds_client.describe_db_log_files(DBInstanceIdentifier='warehouse')
    print(description)
    print(log_files)
    lambda_ingestion({}, {})
    lambda_processing({}, {})
    lambda_warehousing({}, {}, hostname)
    assert False