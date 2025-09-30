import boto3
import json
import pandas as pd
from io import StringIO

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated']}

def process_staff_data(data):
    print(data['staff'].head())
    pass

def process_sales_order_data(data):
    print(data['sales_order'].head())
    pass

def fetch_data():
    s3_client = boto3.client('s3')
    result = s3_client.get_object(Bucket='nc-lambda-bucket-joe-final-project-2025', Key='latest_update.json')
    result = result['Body'].read().decode('utf-8')
    latest_update = json.loads(result)
    print(latest_update)
    data = {}
    for table in TABLE_LIST.keys():
        response = s3_client.get_object(Bucket='nc-joe-ingestion-bucket-2025', Key=f'{table}/{latest_update[table]}.csv')
        object_content = StringIO(response['Body'].read().decode('utf-8'))
        data[table] = pd.read_csv(object_content)
        first_row = data[table].iloc[0]
        print(first_row)
    return data

def lambda_processing(event, target):
    data = fetch_data()
    process_staff_data(data)
    process_sales_order_data(data)
    pass