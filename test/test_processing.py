from src.lambda_processing import lambda_processing, fetch_data, process_staff_data, process_sales_order_data, process_counterparty_data, process_currency_data, process_dates, process_design, process_location, fetch_file_from_ingest
from src.lambda_ingestion import lambda_ingestion
from moto import mock_aws
import boto3
import pytest
import os
from moto.core import patch_client
import pg8000.native
from dotenv import dotenv_values
import pandas as pd
import json
import datetime
from decimal import Decimal

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

def test_processing_lambda_uploads_files_to_s3(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    latest_update = lambda_ingestion({}, {})
    lambda_processing({}, {})   
    s3_client = boto3.client('s3')
    processed_bucket = s3_client.list_objects(Bucket=config['PROCESSED_BUCKET'])
    processed_bucket_files = [item['Key'] for item in processed_bucket['Contents']]
    lambda_bucket = s3_client.list_objects(Bucket=config['LAMBDA_BUCKET'])
    lambda_bucket_files = [item['Key'] for item in lambda_bucket['Contents']]
    assert set(processed_bucket_files) == set(['dim_location.parquet','dim_design.parquet','dim_staff.parquet', 'fact_sales_order.parquet', 'dim_counterparty.parquet', 'dim_date.parquet', 'dim_currency.parquet'])
    assert set(lambda_bucket_files) == set(['latest_update.json', 'updated_tables.json', 'new_parquets.json'])

def test_fetch_data_works(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    dummy_latest_update = {table: 'latest' for table in TABLE_LIST.keys()}
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='latest_update.json', Body=json.dumps(dummy_latest_update))

    dummy_data = pd.DataFrame(data=[32,432,4,3232,3214,4234], columns=['numbers'])
    for table in TABLE_LIST.keys():
        s3_client.put_object(Bucket=config['INGESTION_BUCKET'], Key=f'{table}/latest.csv', Body=dummy_data.to_csv(index=False))

    data, latest_update = fetch_data()
    assert data.keys() == dummy_latest_update.keys()
    for value in data.values():
        assert list(value.columns) == list(dummy_data.columns)
        assert list(value) == list(dummy_data)
    assert latest_update == dummy_latest_update

def test_fetch_file_from_ingest(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    df = pd.DataFrame(data=[0,1,2,23,4,5,43], columns=['numbers'])
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['INGESTION_BUCKET'], Key='path', Body=df.to_csv(index=False))
    file = fetch_file_from_ingest(s3_client, 'path')
    assert list(file['numbers']) == [0,1,2,23,4,5,43]

def test_process_location():
    dummy_data = {'address': pd.DataFrame(data=[[8, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]], columns=TABLE_LIST['address']), 'sales_order': pd.DataFrame(data=[[2, datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), datetime.datetime(2022, 11, 3, 14, 20, 52, 186000), 3, 19, 8, 42972, Decimal('3.94'), 2, '2022-11-07', '2022-11-08', 8]], columns=TABLE_LIST['sales_order'])}
    df_location = process_location(dummy_data)
    assert list(df_location.columns) == ['location_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone']
    assert list(df_location.values[0]) == [8, '6826 Herzog Via', None, 'Avon', 'New Patienceburgh', '28441', 'Turkey', '1803 637401']

def test_process_design():
    dummy_data = {'design': pd.DataFrame(data=[[8, datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), 'Wooden', '/usr', 'wooden-20220717-npgz.json'], [51, datetime.datetime(2023, 1, 12, 18, 50, 9, 935000), datetime.datetime(2023, 1, 12, 18, 50, 9, 935000), 'Bronze', '/private', 'bronze-20221024-4dds.json']], columns=TABLE_LIST['design'])}
    df_design = process_design(dummy_data)
    assert list(df_design.columns) == ['design_id', 'design_name', 'file_location', 'file_name']
    assert list(df_design.values[0]) == [8, 'Wooden', '/usr', 'wooden-20220717-npgz.json']
    assert list(df_design.values[1]) == [51, 'Bronze', '/private', 'bronze-20221024-4dds.json']

# @pytest.mark.skip(reason='doesn\'t currently work')
def test_process_dates():
    dummy_data = {'sales_order': pd.DataFrame(data=[[2, "2022-11-3 14:20:52.186000", "2022-11-3 14:20:52.186000", 3, 19, 8, 42972, Decimal('3.94'), 2, '2022-11-07', '2022-11-08', 8]], columns=TABLE_LIST['sales_order'])}
    df_sales_order = process_sales_order_data(dummy_data)
    df_dates = process_dates(dummy_data)
    print(df_dates.values)
    assert list(df_dates.columns) == ['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter']
    assert list(df_dates.values[0]) == [ '2022-11-3', 2022, 11, 3, 3, 'Thursday', 'November', 4]

def test_process_currency_data():
    dummy_data = {'currency': pd.DataFrame(data=[[1, 'GBP', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], [2, 'USD', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)], [3, 'EUR', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]], columns=TABLE_LIST['currency'])}
    # con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])
    # values = con.run(f'SELECT {', '.join(TABLE_LIST['currency'])} FROM currency LIMIT 3')
    # print(values)
    # assert False
    df_currency = process_currency_data(dummy_data)
    assert list(df_currency.columns) == ['currency_id', 'currency_code', 'currency_name']
    assert list(df_currency.values[0]) == [1, 'GBP', 'Great British Pound']
    assert list(df_currency.values[1]) == [2, 'USD', 'United States Dollar']
    assert list(df_currency.values[2]) == [3, 'EUR', "Euro"]

def test_process_counterparty_data():
    dummy_data = {'counterparty': pd.DataFrame(data=[[1, 'Fahey and Sons', 15, 'Micheal Toy', 'Mrs. Lucy Runolfsdottir', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]], columns=TABLE_LIST['counterparty']), 'address': pd.DataFrame(data=[[15, '605 Haskell Trafficway', 'Axel Freeway', None, 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]], columns=TABLE_LIST['address'])}
    df_counterparty = process_counterparty_data(dummy_data)
    print(df_counterparty.columns)
    assert list(df_counterparty.columns) == ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postcode', 'counterparty_legal_country', 'counterparty_legal_phone_number']
    assert list(df_counterparty.values[0]) == [1, 'Fahey and Sons', '605 Haskell Trafficway', 'Axel Freeway', None, 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447']

def test_process_sales_order():
    dummy_data = {'sales_order': pd.DataFrame(data=[[2, "2022-11-3 14:20:52.186000", "2022-11-3 14:20:52.186000", 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8]], columns=TABLE_LIST['sales_order'])}
    # con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])
    # values = con.run(f'SELECT {', '.join(TABLE_LIST['sales_order'])} FROM sales_order LIMIT 2')
    # print(values)
    # assert False
    df_sales_order = process_sales_order_data(dummy_data)
    assert list(df_sales_order.columns) == ['sales_order_id',
    'design_id' ,
    'sales_staff_id',
    'counterparty_id' ,
    'units_sold' ,
    'unit_price' ,
    'currency_id' ,
    'agreed_delivery_date',
    'agreed_payment_date' ,
    'agreed_delivery_location_id' ,
    'last_updated_date' ,
    'last_updated_time' ,
    'created_date' ,
    'created_time' ]
    


    assert list(df_sales_order.values[0]) == [2, 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8, '2022-11-3', '14:20:52.186000', '2022-11-3', '14:20:52.186000']

def test_process_staff_data():
    dummy_data = {'staff': pd.DataFrame(data=[[1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]], columns=TABLE_LIST['staff']), 'department': pd.DataFrame(data=[[2, 'Purchasing', 'Manchester', 'Naomi Lapaglia', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]], columns=TABLE_LIST['department'])}
    df_staff = process_staff_data(dummy_data)
    assert list(df_staff.columns) == ['staff_id',
    'first_name',
    'last_name',
    'email_address',
    'department_name',
    'location']
    assert list(df_staff.values[0]) == [1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester']

def test_process_lambda(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
    conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    dummy_data = {'staff': pd.DataFrame(data=[[1, 'Jeremie', 'Franey', 2, 'jeremie.franey@terrifictotes.com', datetime.datetime(2022, 11, 3, 14, 20, 51, 563000), datetime.datetime(2022, 11, 3, 14, 20, 51, 563000)]], columns=TABLE_LIST['staff']), 'department': pd.DataFrame(data=[[2, 'Purchasing', 'Manchester', 'Naomi Lapaglia', datetime.datetime(2022, 11, 3, 14, 20, 49, 962000), datetime.datetime(2022, 11, 3, 14, 20, 49, 962000)]], columns=TABLE_LIST['department'])}
    dummy_latest_update = {table: 'latest' for table in TABLE_LIST.keys()}
    processed_dummy_data = process_staff_data(dummy_data)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='updated_tables.json', Body=json.dumps({'updates': ['staff', 'department']}))
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='latest_update.json', Body=json.dumps(dummy_latest_update))
    for table in dummy_data.keys():
        s3_client.put_object(Bucket=config['INGESTION_BUCKET'], Key=f'{table}/latest.csv', Body=dummy_data[table].to_csv(index=False))
    lambda_processing({},{})
    response = s3_client.get_object(Bucket=config['LAMBDA_BUCKET'], Key='new_parquets.json')
    result = response['Body'].read().decode('utf-8')
    new_files = json.loads(result)
    s3_path = f"s3://{config['PROCESSED_BUCKET']}/dim_staff.parquet"
    # processed_data = wr.s3.read_parquet(s3_path)
    s3_client.get_object
    assert new_files == {'new_parquets': ['dim_staff']}
    # assert list(processed_data) == list(processed_dummy_data)