from src.lambda_warehousing import lambda_warehousing, fetch_all_processed_data, fetch_processed_data_by_table, upload_dim_table_data, upload_fact_table_data, connect_warehouse
from src.lambda_ingestion import lambda_ingestion
from src.lambda_processing import lambda_processing
from moto import mock_aws
import boto3
import pytest
import os
from moto.core import patch_client
import pg8000.native
from dotenv import dotenv_values
import pandas as pd
import json

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

def test_fetch_processed_data_by_table_works(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    test_data1 = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df1 = pd.DataFrame(data=test_data1, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    body1 = test_df1.to_parquet(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='dim_staff.parquet', Body=body1)
    df = fetch_processed_data_by_table('dim_staff')
    assert list(df.columns) == ['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location']
    assert list(df.values[0]) == [1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester']
    assert list(df.values[1]) == [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester']
    assert list(df.values[2]) == [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester']
    assert list(df.values[3]) == [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds']
    assert list(df.values[4]) == [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']

def test_upload_dim_table_data_works():
    test_data_staff = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df_staff = pd.DataFrame(data=test_data_staff, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    data = {'dim_staff': test_df_staff}
    con_rds = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    upload_dim_table_data(data, 'dim_staff', con_rds)
    uploaded = con_rds.run('SELECT * FROM dim_staff WHERE staff_id BETWEEN 1 AND 5')
    assert list(uploaded) == test_data_staff

def test_warehousing(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    test_data1 = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df1 = pd.DataFrame(data=test_data1, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    body1 = test_df1.to_parquet(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='dim_staff.parquet', Body=body1)
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='new_parquets.json', Body=json.dumps({'new_parquets': ['dim_staff']}))
    lambda_warehousing({}, {})
    con_rds = pg8000.native.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    dim_staff = con_rds.run('SELECT * FROM dim_staff WHERE staff_id BETWEEN 1 AND 5')
    # with pytest.raises(Exception) as e:
    assert dim_staff == test_data1