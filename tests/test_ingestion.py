from src.lambda_ingestion import lambda_ingestion, get_latest_update, upload_data, connect_db
from moto import mock_aws
import boto3
import pytest
import os
from moto.core import patch_client
import pg8000.native
from dotenv import dotenv_values
import pandas as pd
import json

TABLE_LIST={'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'design': ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location', 'file_name']}

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

def test_ingestion_lambda_uploads_to_s3(mocked_aws):
    # print(config['TABLE_LIST'])
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    latest_update = lambda_ingestion({}, {})
    # print(latest_update)
    s3_client = boto3.client('s3')
    result = s3_client.list_objects(Bucket=config['INGESTION_BUCKET'])
    result2 = s3_client.list_objects(Bucket=config['LAMBDA_BUCKET'])
    ingestion_bucket_files = [item['Key'] for item in result['Contents']]
    latest_update_file = [item['Key'] for item in result2['Contents']]
    # print(result2)
    ingestion_files = []
    for table in ['sales_order', 'staff', 'department', 'counterparty', 'address', 'currency', 'design']:
        ingestion_files.append(f'{table}/{latest_update[table]}.csv')
    assert set(ingestion_bucket_files) == set(ingestion_files)
    assert set(latest_update_file) == set(['latest_update.json'])


def test_get_latest_update_fetches_most_recent_update_times_for_each_table(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    s3_client = boto3.client('s3')
    LATEST_UPDATE = {table: "1994-00-00 00:00:00.0" for table in TABLE_LIST.keys()}
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='latest_update.json', Body=json.dumps(LATEST_UPDATE))
    latest_updates = get_latest_update(s3_client)
    for table in TABLE_LIST.keys():
        assert latest_updates[table] == "1994-00-00 00:00:00.0"

def test_get_latest_update_when_no_latest_update_exists_returns_0_timedate(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    s3_client = boto3.client('s3')
    latest_updates = get_latest_update(s3_client)
    for table in TABLE_LIST.keys():
        assert latest_updates[table] == "0000-00-00 00:00:00.0"

def test_upload_data_uploads_latest_data(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    s3_client = boto3.client('s3')
    db = connect_db()
    latest_update = get_latest_update(s3_client)
    uploaded = []
    for table in TABLE_LIST.keys():
        uploaded.append(table)
        upload_data(db, s3_client, table, latest_update)
        objects = s3_client.list_objects(Bucket=config['INGESTION_BUCKET'])
        ingestion_bucket_files = [item['Key'] for item in objects['Contents']]
        assert set(ingestion_bucket_files) == set([f'{table}/{latest_update[table]}.csv' for table in uploaded])

