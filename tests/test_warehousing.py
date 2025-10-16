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

# @pytest.mark.skip(reason='no')
# def test_warehouse_uploads_dim_staff(mocked_aws):
#     test_data1 = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
#     test_df1 = pd.DataFrame(data=test_data1, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
#     body1 = test_df1.to_parquet(index=False)
#     data = {
#         'dim_staff': body1
#     }
#     con_rds = pg8000.native.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    
#     upload_dim_table_data(data, 'dim_staff', con_rds, )
#     data = con_rds.run('SELECT * FROM dim_staff')
#     assert data == test_data1

def test_fetch_processed_data_by_table_works(mocked_aws):
    conn = boto3.resource('s3', region_name='us-east-1')
    conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
    test_data1 = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df1 = pd.DataFrame(data=test_data1, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    body1 = test_df1.to_parquet(index=False)
    s3_client = boto3.client('s3')
    s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='dim_staff.parquet', Body=body1)
    df = fetch_processed_data_by_table('dim_staff')
    # print(df.iloc[0])
    # assert False
    assert list(df.columns) == ['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location']
    assert list(df.values[0]) == [1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester']
    assert list(df.values[1]) == [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester']
    assert list(df.values[2]) == [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester']
    assert list(df.values[3]) == [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds']
    assert list(df.values[4]) == [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']

# @pytest.mark.skip(reason='too much')
# def test_fetch_all_processed_data(mocked_aws):
#     conn = boto3.resource('s3', region_name='us-east-1')
#     conn.create_bucket(Bucket=config['PROCESSED_BUCKET'])
#     # conn.create_bucket(Bucket=config['INGESTION_BUCKET'])
#     # conn.create_bucket(Bucket=config['LAMBDA_BUCKET'])
#     # lambda_ingestion({}, {})
#     # lambda_processing({}, {})
#     test_data_staff = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
#     test_df_staff = pd.DataFrame(data=test_data_staff, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
#     body_staff = test_df_staff.to_parquet(index=False)
#     test_data_sales_order = [[2, 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [3, 4, 10, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [4, 4, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [5, 7, 18, 4, 49659, 2.41, 3, '2022-11-05', '2022-11-08', 25, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [6, 3, 13, 18, 83908, 3.99, 3, '2022-11-04', '2022-11-07', 17, '2022-11-04', '11:37:10.341', '2022-11-04', '11:37:10.341']]
#     test_df_sales_order = pd.DataFrame(data=test_data_sales_order, columns=['sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price','currency_id','agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id', 'last_updated_date', 'last_updated_time', 'created_at_date', 'created_at_time'])
#     body_sales_order = test_df_sales_order.to_parquet(index=False)
#     test_data_counterparty = [[1, 'Fahey and Sons', '605 Haskell Trafficway', 'Axel Freeway', 'None', 'East Bobbie', '88253-4257', 'Heard Island and McDonald Islands', '9687 937447'], [2, 'Leannon, Predovic and Morar', '079 Horacio Landing', 'None', 'None', 'Utica', '93045', 'Austria', '7772 084705'], [3, 'Armstrong Inc', '179 Alexie Cliffs', 'None', 'None', 'Aliso Viejo', '99305-7380', 'San Marino', '9621 880720'], [4, 'Kohler Inc', '37736 Heathcote Lock', 'Noemy Pines', 'None', 'Bartellview', '42400-5199', 'Congo', '1684 702261'], [5, 'Frami, Yundt and Macejkovic', '364 Goodwin Streets', 'None', 'None', 'Sayreville', '85544-4254', 'Svalbard & Jan Mayen Islands', '0847 468066']]
#     test_df_counterparty = pd.DataFrame(data=test_data_counterparty, columns=['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postcode', 'counterparty_legal_country', 'counterparty_legal_phone_number'])
#     body_counterparty = test_df_counterparty.to_parquet(index=False)

#     s3_client=boto3.client('s3')
#     s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='dim_staff.parquet', Body=body_staff)
#     s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='fact_sales_order.parquet', Body=body_sales_order)
#     s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key='dim_counterparty.parquet', Body=body_counterparty)

#     data = fetch_all_processed_data()
#     assert list(data.keys()) == ['dim_staff', 'dim_counterparty', 'fact_sales_order']
#     assert list(data['dim_staff'].columns) == ['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location']
#     assert list(data['dim_counterparty'].columns) == ['counterparty_id', 'counterparty_legal_name', 'counterparty_legal_address_line_1', 'counterparty_legal_address_line_2', 'counterparty_legal_district', 'counterparty_legal_city', 'counterparty_legal_postcode', 'counterparty_legal_country', 'counterparty_legal_phone_number']
#     assert list(data['fact_sales_order'].columns) == ['sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price','currency_id','agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id', 'last_updated_date', 'last_updated_time', 'created_at_date', 'created_at_time']

def test_upload_dim_table_data_works():
    test_data_staff = [[1, 'Jeremie', 'Franey', 'jeremie.franey@terrifictotes.com', 'Purchasing', 'Manchester'], [2, 'Deron', 'Beier', 'deron.beier@terrifictotes.com', 'Facilities', 'Manchester'], [3, 'Jeanette', 'Erdman', 'jeanette.erdman@terrifictotes.com', 'Facilities', 'Manchester'], [4, 'Ana', 'Glover', 'ana.glover@terrifictotes.com', 'Production', 'Leeds'], [5, 'Magdalena', 'Zieme', 'magdalena.zieme@terrifictotes.com', 'HR', 'Leeds']]
    test_df_staff = pd.DataFrame(data=test_data_staff, columns=['staff_id', 'first_name', 'last_name', 'email_address', 'department_name', 'location'])
    data = {'dim_staff': test_df_staff}
    con_rds = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    upload_dim_table_data(data, 'dim_staff', con_rds)
    uploaded = con_rds.run('SELECT * FROM dim_staff WHERE staff_id BETWEEN 1 AND 5')
    assert list(uploaded) == test_data_staff
    # assert test_data_staff in uploaded

# @pytest.mark.skip(reason='too big')
# def test_upload_fact_table_data():
#     test_data_sales_order = [[2, 3, 19, 8, 42972, 3.94, 2, '2022-11-07', '2022-11-08', 8, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [3, 4, 10, 4, 65839, 2.91, 3, '2022-11-06', '2022-11-07', 19, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [4, 4, 10, 16, 32069, 3.89, 2, '2022-11-05', '2022-11-07', 15, '2022-11-03', '14:20:52.188', '2022-11-03', '14:20:52.188'], [5, 7, 18, 4, 49659, 2.41, 3, '2022-11-05', '2022-11-08', 25, '2022-11-03', '14:20:52.186', '2022-11-03', '14:20:52.186'], [6, 3, 13, 18, 83908, 3.99, 3, '2022-11-04', '2022-11-07', 17, '2022-11-04', '11:37:10.341', '2022-11-04', '11:37:10.341']]
#     test_df_sales_order = pd.DataFrame(data=test_data_sales_order, columns=['sales_order_id', 'design_id', 'sales_staff_id', 'counterparty_id', 'units_sold', 'unit_price','currency_id','agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id', 'last_updated_date', 'last_updated_time', 'created_at_date', 'created_at_time'])
#     data = {'fact_sales_order': test_df_sales_order}
#     con_rds = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
#     upload_fact_table_data(data, 'fact_sales_order', con_rds)
#     uploaded = con_rds.run('SELECT * FROM fact_sales_order')
#     for item in test_data_sales_order:
#         assert item in uploaded

# @pytest.mark.skip(reason="no reason")
def test_rds_behaviour_2(mocked_aws):
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
    # con_rds.run('DROP DATABASE IF EXISTS final_project')
    # con_rds.run('CREATE DATABASE final_project')
    # dim_counterparty = con_rds.run('SELECT * FROM dim_counterparty LIMIT 5')
    dim_staff = con_rds.run('SELECT * FROM dim_staff WHERE staff_id BETWEEN 1 AND 5')
    # with pytest.raises(Exception) as e:
    assert dim_staff == test_data1
    # fact_sales_order = con_rds.run('SELECT * FROM fact_sales_order LIMIT 5')
    # dim_date = con_rds.run('SELECT * FROM dim_date LIMIT 5')
    # dim_currency = con_rds.run('SELECT * FROM dim_currency LIMIT 5')
    # dim_design = con_rds.run('SELECT * FROM dim_design LIMIT 5')
    # dim_location = con_rds.run('SELECT * FROM dim_location LIMIT 5')
    # print(fact_sales_order)
    # print(dim_staff)
    # print(dim_counterparty)
    # print(dim_date)
    # print(dim_currency)
    # assert False