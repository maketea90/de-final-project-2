import boto3
import json
import pandas as pd
from io import StringIO
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated']}

def process_staff_data(data):
    department = data['department'][['department_id', 'department_name', 'location']]
    merged = pd.merge(data['staff'], department, on='department_id',  how='left').drop('department_id', axis=1)
    merged = merged.drop('created_at', axis=1)
    merged = merged.drop('last_updated', axis=1)
    return merged

def process_sales_order_data(data):
    df_sales_order = data['sales_order']
    df_sales_order[['last_updated_date', 'last_updated_time']] = df_sales_order['last_updated'].str.split(' ', n=1, expand=True)
    df_sales_order[['created_at_date', 'created_at_time']] = df_sales_order['created_at'].str.split(' ', n=1, expand=True)
    df_sales_order.index.name = 'sales_record_id'
    # print(df_sales_order.head())
    df_sales_order = df_sales_order.drop('created_at', axis=1)
    df_sales_order= df_sales_order.drop('last_updated', axis=1)
    df_sales_order = df_sales_order.rename(columns={'staff_id': 'sales_staff_id'})
    return df_sales_order

def fetch_data():
    s3_client = boto3.client('s3')
    result = s3_client.get_object(Bucket='nc-lambda-bucket-joe-final-project-2025', Key='latest_update.json')
    result = result['Body'].read().decode('utf-8')
    latest_update = json.loads(result)
    print(latest_update)
    data = {}
    for table in TABLE_LIST.keys():
        try:
            logging.info(f'fetching latest data from "{table}" table')
            response = s3_client.get_object(Bucket='nc-joe-ingestion-bucket-2025', Key=f'{table}/{latest_update[table]}.csv')
            object_content = StringIO(response['Body'].read().decode('utf-8'))
            data[table] = pd.read_csv(object_content)
            first_row = data[table].iloc[0]
        except ClientError as e:
            logging.info(f'fetching data from table "{table}" failed due to {e}')
        else:
            logging.info(f'data from table "{table}" successfully retrieved')
        # print(first_row)
    logging.info('process complete')
    return data, latest_update

def lambda_processing(event, target):
    s3_client = boto3.client('s3')
    logging.info('fetching data from s3 ingestion bucket')
    data, latest_update = fetch_data()
    logging.info('processing data')
    df_dim_staff = process_staff_data(data)
    df_fact_sales_order = process_sales_order_data(data)
    logging.info('loading processed data to s3')
    try:
        parqueted_dim_staff = df_dim_staff.to_parquet(index=False)
        # print(parqueted_dim_staff)
        s3_client.put_object(Bucket='nc-joe-processed-bucket-2025', Key='dim_staff.parquet', Body=parqueted_dim_staff)
    except Exception as e:
        logging.info(f'upload of processed data "dim_staff" failed due to {e}')
    else:
        logging.info(f'upload of processed data "dim_staff" was successful')
    try:
        parqueted_fact_sales_order = df_fact_sales_order.to_parquet(index=False)
        # print(parqueted_fact_sales_order)
        s3_client.put_object(Bucket='nc-joe-processed-bucket-2025', Key='fact_sales_order.parquet', Body=parqueted_fact_sales_order)
    except Exception as e:
        logging.info(f'upload of processed data "fact_sales_order" failed due to {e}')
    else:
        logging.info('upload of processed data "fact_sales_order" was successful')
    return latest_update