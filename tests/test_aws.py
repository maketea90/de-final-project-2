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
import pandas as pd

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

# @pytest.mark.skip(reason="no reason")
def test_ingestion_lambda_uploads_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket="nc-lambda-bucket-joe-final-project-2025")
    latest_update = lambda_ingestion({}, {})
    s3_client = boto3.client('s3')
    result = s3_client.list_objects(Bucket='nc-joe-ingestion-bucket-2025')
    result2 = s3_client.list_objects(Bucket='nc-lambda-bucket-joe-final-project-2025')
    ingestion_bucket_files = [item['Key'] for item in result['Contents']]
    latest_update_file = [item['Key'] for item in result2['Contents']]
    print(result2)
    ingestion_files = []
    for table in ['sales_order', 'staff', 'department']:
        ingestion_files.append(f'{table}/{latest_update[table]}.csv')
    assert set(ingestion_bucket_files) == set(ingestion_files)
    assert set(latest_update_file) == set(['latest_update.json'])

# @pytest.mark.skip(reason='nothing')
def test_processing_lambda_uploads_processed_data_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket="nc-lambda-bucket-joe-final-project-2025")
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    latest_update = lambda_ingestion({}, {}, True)
    lambda_processing({}, {})   
    s3_client = boto3.client('s3')
    processed_bucket = s3_client.list_objects(Bucket='nc-joe-processed-bucket-2025')
    processed_bucket_files = [item['Key'] for item in processed_bucket['Contents']]
    assert set(processed_bucket_files) == set(['dim_staff.parquet', 'fact_sales_order.parquet'])

# @pytest.mark.skip(reason="no reason")
def test_rds_behaviour(mocked_aws):
    test_data1 = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df1 = pd.DataFrame(data=test_data1, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    body1 = test_df1.to_parquet(index=False)
    test_data2 = [[2, 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [3, 4, 10, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [4, 4, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [5, 7, 18, 4, 49659, 2.41, 3, '2022-11-05', '2022-11-08', 25, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [6, 3, 13, 18, 83908, 3.99, 3, '2022-11-04', '2022-11-07', 17, '2022-11-04', '11:37:10.341', '2022-11-04', '11:37:10.341']]
    test_df2 = pd.DataFrame(data=test_data2, columns=['sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price','currency_id','agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id', 'last_updated_date', 'last_updated_time', 'created_at_date', 'created_at_time'])
    body2 = test_df2.to_parquet(index=False)
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket='nc-joe-processed-bucket-2025')
    s3_client=boto3.client('s3')
    s3_client.put_object(Bucket='nc-joe-processed-bucket-2025', Key='dim_staff.parquet', Body=body1)
    s3_client.put_object(Bucket='nc-joe-processed-bucket-2025', Key='fact_sales_order.parquet', Body=body2)
    lambda_warehousing({}, {})
    con_rds = pg8000.native.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    dim_sales = con_rds.run('SELECT * FROM dim_staff')
    fact_sales_order = con_rds.run('SELECT * FROM fact_sales_order')
    assert dim_sales == test_data1
    assert fact_sales_order == test_data2