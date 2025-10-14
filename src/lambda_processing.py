import boto3
import json
import pandas as pd
from io import StringIO
import logging
from botocore.exceptions import ClientError
import numpy as np
from datetime import datetime
from dotenv import dotenv_values

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'design': ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location', 'file_name']}

config = dotenv_values('.env')

def process_staff_data(data):
    department = data['department'][['department_id', 'department_name', 'location']]
    merged = pd.merge(data['staff'], department, on='department_id',  how='left').drop('department_id', axis=1)
    merged = merged.drop('created_at', axis=1)
    merged = merged.drop('last_updated', axis=1)
    return merged

def process_sales_order_data(data):
    df_sales_order = data['sales_order']
    df_sales_order[['last_updated_date', 'last_updated_time']] = df_sales_order['last_updated'].str.split(' ', n=1, expand=True)
    df_sales_order[['created_date', 'created_time']] = df_sales_order['created_at'].str.split(' ', n=1, expand=True)
    # df_sales_order.index.name = 'sales_record_id'
    # print(df_sales_order.iloc[0])
    df_sales_order = df_sales_order.drop('created_at', axis=1)
    df_sales_order= df_sales_order.drop('last_updated', axis=1)
    df_sales_order = df_sales_order.rename(columns={'staff_id': 'sales_staff_id'})
    # df_sales_order['sales_staff_id'] = df_sales_order['sales_staff_id'].map(int)
    # df_sales_order = df_sales_order.rename(columns={'sales_order_id': 'sales_record_id'})
    # print(df_sales_order.iloc[0])
    print(df_sales_order.dtypes)
    return df_sales_order

def process_counterparty_data(data):
    df_counterparty = data['counterparty']
    df_address = data['address']
    
    # df_counterparty.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
    # df_address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)

    merged = pd.merge(df_counterparty, df_address, left_on='legal_address_id', right_on='address_id', how='left').drop('address_id', axis=1)
    for column in ['legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at_x', 'last_updated_x','created_at_y', 'last_updated_y']:
        merged = merged.drop(f'{column}', axis=1)
    merged = merged.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 'postal_code': 'counterparty_legal_postcode', 'country': 'counterparty_legal_country', 'phone': 'counterparty_legal_phone_number'})
    # print(merged.iloc[0])
    # merged = merged.fillna(value=np.nan)
    return merged

def find_currency_name(code, conversions):
    filtered = filter(lambda x: x['abbreviation'] == code, conversions)
    return list(filtered)[0]['currency']

def process_currency_data(data):
    df_currency = data['currency'][['currency_id', 'currency_code']]
    with open('./currency_code_conversions.json', 'r') as f:
        conversions = json.load(f)
        currency_numpy_array = df_currency[['currency_id', 'currency_code']].to_numpy()
        data = [np.concatenate((arr, [find_currency_name(arr[1], conversions)])) for arr in currency_numpy_array]
        df = pd.DataFrame(columns=['currency_id', 'currency_code', 'currency_name'])
        df['currency_id'] = [arr[0] for arr in data]
        df['currency_code'] = [arr[1] for arr in data] 
        df['currency_name'] = [arr[2] for arr in data]
        return df

def process_dates(data):
    # df_sales_order = data['sales_order'].copy()
    # df_sales_order[['last_updated_date', 'last_updated_time']] = df_sales_order['last_updated'].str.split(' ', n=1, expand=True)
    # df_sales_order[['created_date', 'created_time']] = df_sales_order['created_at'].str.split(' ', n=1, expand=True)
    df_sales_order_dates = data['sales_order'][['last_updated_date', 'created_date', 'agreed_delivery_date', 'agreed_payment_date']]
    frames = [df_sales_order_dates['last_updated_date'], df_sales_order_dates['created_date'], df_sales_order_dates['agreed_delivery_date'], df_sales_order_dates['agreed_payment_date']]
    df_sales_order_dates = pd.concat(frames).drop_duplicates()
    df_dates = pd.DataFrame(columns=['date_id', 'year', 'month', 'day', 'day_of_week', 'day_name', 'month_name', 'quarter'])
    df_dates['date_id'] = df_sales_order_dates
    df_dates['year'] = (df_sales_order_dates.str[:4]).map(int)
    df_dates['month'] = (df_sales_order_dates.str[5:7]).map(int)
    df_dates['day'] = (df_sales_order_dates.str[8:10]).map(int)
    dt = df_sales_order_dates.apply(lambda x: datetime.strptime(x, "%Y-%m-%d"))
    df_dates['day_of_week'] = dt.map(datetime.weekday)
    df_dates['day_name'] = dt.apply(lambda x: x.strftime('%A'))
    df_dates['month_name'] = dt.apply(lambda x: x.strftime('%B'))
    df_dates['quarter'] = dt.apply(lambda x: ((x.month-1)//3) + 1)
    return df_dates

def process_design(data):
    df_design = data['design']
    df_design = df_design.drop('created_at', axis=1)
    df_design = df_design.drop('last_updated', axis=1)
    return df_design

def process_location(data):
    df_address = data['address']
    df_sales_order = data['sales_order']
    # print(df_address.iloc[0])
    # print(df_sales_order.iloc[0])
    df_location = pd.merge(df_sales_order, df_address, left_on='agreed_delivery_location_id', right_on='address_id', how='left')
    # print(df_location.iloc[0])
    removed = list(df_sales_order.columns)
    removed.append('created_at_x') 
    removed.append('created_at_y')
    removed.append('last_updated_x')
    removed.append('last_updated_y')
    removed.remove('created_at')
    removed.remove('last_updated')
    # print(removed)
    for column in removed:
        df_location = df_location.drop(f'{column}', axis=1)
    df_location = df_location.rename(columns={'address_id': 'location_id'})
    # print(df_location.iloc[0])
    df_location.drop_duplicates(inplace=True)
    return df_location

def fetch_file_from_ingest(client, path):
    try:
        response = client.get_object(Bucket=config['INGESTION_BUCKET'], Key=path)
        object_content = StringIO(response['Body'].read().decode('utf-8'))
        return pd.read_csv(object_content)
    except ClientError as e:
        # logging.info(f'data fetch failed: {e}')
        raise e

def fetch_data():
    s3_client = boto3.client('s3')
    result = s3_client.get_object(Bucket=config['LAMBDA_BUCKET'], Key='latest_update.json')
    result = result['Body'].read().decode('utf-8')
    latest_update = json.loads(result)
    print(latest_update)
    data = {}
    for table in TABLE_LIST.keys():
        try:
            logging.info(f'fetching latest data from "{table}" table')
            # response = s3_client.get_object(Bucket=config['INGESTION_BUCKET'], Key=f'{table}/{latest_update[table]}.csv')
            # object_content = StringIO(response['Body'].read().decode('utf-8'))
            # data[table] = pd.read_csv(object_content)
            data[table] = fetch_file_from_ingest(s3_client, f'{table}/{latest_update[table]}.csv')
        except ClientError as e:
            logging.info(f'fetching data from table "{table}" failed due to {e}')
        else:
            logging.info(f'data from table "{table}" successfully retrieved')
    return data, latest_update

def lambda_processing(event, target):
    s3_client = boto3.client('s3')
    logging.info('fetching data from s3 ingestion bucket')
    data, latest_update = fetch_data()
    response = s3_client.get_object(Bucket=config['LAMBDA_BUCKET'], Key='updated_tables.json')
    result = response['Body'].read().decode('utf-8')
    updated_tables = json.loads(result)['updates']
    logging.info('processing data')
    dataframes = {}
    if 'staff' in updated_tables:
        dataframes['dim_staff'] = process_staff_data(data)
    if 'counterparty' in updated_tables:
        dataframes['dim_counterparty'] = process_counterparty_data(data)
    if 'currency' in updated_tables:
        dataframes['dim_currency'] = process_currency_data(data)
    if 'design' in updated_tables:
        dataframes['dim_design'] = process_design(data)
    if 'address' in updated_tables:
        dataframes['dim_location'] = process_location(data)
    if 'sales_order' in updated_tables:
        dataframes['fact_sales_order'] = process_sales_order_data(data)
        dataframes['dim_date'] = process_dates(data)
    new_files = list(dataframes.keys())
    s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='new_parquets.json', Body=json.dumps({'new_parquets': new_files}))
    logging.info('loading processed data to s3')
    for name, df in dataframes.items():
        try:
            parqueted = df.to_parquet(index=False)
            s3_client.put_object(Bucket=config['PROCESSED_BUCKET'], Key=f'{name}.parquet', Body=parqueted)
        except Exception as e:
            logging.info(f'upload of processed data "{name}" failed: {e}')
        else:
            logging.info(f'upload of processed data "{name}" was successful')
    logging.info('processing lambda complete')
    return latest_update