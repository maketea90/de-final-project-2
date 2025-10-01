from src.lambda_ingestion import lambda_ingestion
from src.lambda_processing import lambda_processing
from src.lambda_warehousing import lambda_warehousing
from moto import mock_aws
import boto3
import pytest
import os
from moto.core import patch_client
import pg8000.native
from dotenv import dotenv_values

config = dotenv_values('.env')

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

# @pytest.mark.skip(reason='nothing')
def test_processing_lambda_uploads_processed_data_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket="nc-lambda-bucket-joe-final-project-2025")
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    latest_update = lambda_ingestion({}, {})
    lambda_processing({}, {})   
    s3_client = boto3.client('s3') 
    ingestion_bucket = s3_client.list_objects(Bucket='nc-joe-ingestion-bucket-2025')
    processed_bucket = s3_client.list_objects(Bucket='nc-joe-processed-bucket-2025')
    ingestion_bucket_files = [item['Key'] for item in ingestion_bucket['Contents']]
    # print(ingestion_bucket_files, '\n\n')
    processed_bucket_files = [item['Key'] for item in processed_bucket['Contents']]
    # print(processed_bucket_files)
    ingestion_files = []
    for table in ['sales_order', 'staff', 'department']:
        ingestion_files.append(f'{table}/{latest_update[table]}.csv')
    assert set(ingestion_bucket_files) == set(ingestion_files)
    assert set(processed_bucket_files) == set(['dim_staff.parquet', 'fact_sales_order.parquet'])

# @pytest.mark.skip(reason="no reason")
def test_rds_behaviour(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket='nc-lambda-bucket-joe-final-project-2025')
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    # print(description)
    # print(log_files)
    latest_update = lambda_ingestion({}, {})
    lambda_processing({}, {}) 
    lambda_warehousing({}, {})
    con_rds = pg8000.native.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    dim_sales = con_rds.run('SELECT * FROM dim_staff LIMIT 5')
    fact_sales_order = con_rds.run('SELECT * FROM fact_sales_order LIMIT 5')
    print(dim_sales, '\n', fact_sales_order)
    assert False