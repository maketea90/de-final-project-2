import boto3
import json
import pandas as pd
from io import StringIO
import logging
from botocore.exceptions import ClientError
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'design': ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location', 'file_name']}

# config = dotenv_values('.env')

def get_secret(name):

    secret_name = name
    region_name = "eu-west-2"

    # Create a Secrets Manager client
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )

    try:
        get_secret_value_response = client.get_secret_value(
            SecretId=secret_name
        )
        secret = json.loads(get_secret_value_response["SecretString"])
        return secret
    except ClientError as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e
    
config = get_secret('bucket_names')

def process_staff_data(data):
    logging.info('processing staff data')
    department = data['department'][['department_id', 'department_name', 'location']]
    merged = pd.merge(data['staff'], department, on='department_id',  how='left').drop('department_id', axis=1)
    merged = merged.drop('created_at', axis=1)
    merged = merged.drop('last_updated', axis=1)
    return merged

def process_sales_order_data(data):
    logging.info('processing sales_order data')
    df_sales_order = data['sales_order']
    df_sales_order[['last_updated_date', 'last_updated_time']] = df_sales_order['last_updated'].str.split(' ', n=1, expand=True)
    df_sales_order[['created_date', 'created_time']] = df_sales_order['created_at'].str.split(' ', n=1, expand=True)
    df_sales_order = df_sales_order.drop('created_at', axis=1)
    df_sales_order= df_sales_order.drop('last_updated', axis=1)
    df_sales_order = df_sales_order.rename(columns={'staff_id': 'sales_staff_id'})
    return df_sales_order

def process_counterparty_data(data):
    logging.info('processing counterparty data')
    df_counterparty = data['counterparty']
    df_address = data['address']
    
    # df_counterparty.drop_duplicates(subset=['address_id'], keep='last', inplace=True)
    # df_address.drop_duplicates(subset=['address_id'], keep='last', inplace=True)

    merged = pd.merge(df_counterparty, df_address, left_on='legal_address_id', right_on='address_id', how='left').drop('address_id', axis=1)
    for column in ['legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at_x', 'last_updated_x','created_at_y', 'last_updated_y']:
        merged = merged.drop(f'{column}', axis=1)
    merged = merged.rename(columns={'address_line_1': 'counterparty_legal_address_line_1', 'address_line_2': 'counterparty_legal_address_line_2', 'district': 'counterparty_legal_district', 'city': 'counterparty_legal_city', 'postal_code': 'counterparty_legal_postcode', 'country': 'counterparty_legal_country', 'phone': 'counterparty_legal_phone_number'})
    return merged

def find_currency_name(code):
    if code == 'GBP':
        return 'Great British Pound'
    elif code == 'USD':
        return 'United States Dollar'
    elif code == 'EUR':
        return 'Euro'
    # return list(filtered)[0]['currency']

def process_currency_data(data):
    logging.info('processing currency data')
    df_currency = data['currency'][['currency_id', 'currency_code']]
    df_currency['currency_name'] = df_currency['currency_code'].map(find_currency_name)
    # with open('./currency_code_conversions.json', 'r') as f:
    #     conversions = json.load(f)
    #     currency_numpy_array = df_currency[['currency_id', 'currency_code']].to_numpy()
    #     data = [np.concatenate((arr, [find_currency_name(arr[1], conversions)])) for arr in currency_numpy_array]
    #     df = pd.DataFrame(columns=['currency_id', 'currency_code', 'currency_name'])
    #     df['currency_id'] = [arr[0] for arr in data]
    #     df['currency_code'] = [arr[1] for arr in data] 
    #     df['currency_name'] = [arr[2] for arr in data]
    #     return df
    return df_currency

def process_dates(data):
    logging.info('processing dates data')
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
    logging.info('processing design data')
    df_design = data['design']
    df_design = df_design.drop('created_at', axis=1)
    df_design = df_design.drop('last_updated', axis=1)
    return df_design

def process_location(data):
    logging.info('processing location data')
    df_address = data['address']
    df_sales_order = data['sales_order']
    df_location = pd.merge(df_sales_order, df_address, left_on='agreed_delivery_location_id', right_on='address_id', how='left')
    removed = list(df_sales_order.columns)
    removed.append('created_at_x') 
    removed.append('created_at_y')
    removed.append('last_updated_x')
    removed.append('last_updated_y')
    removed.remove('created_at')
    removed.remove('last_updated')
    for column in removed:
        df_location = df_location.drop(f'{column}', axis=1)
    df_location = df_location.rename(columns={'address_id': 'location_id'})
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

def fetch_data(updated_tables):
    s3_client = boto3.client('s3')
    result = s3_client.get_object(Bucket=config['LAMBDA_BUCKET'], Key='latest_update.json')
    result = result['Body'].read().decode('utf-8')
    latest_update = json.loads(result)
    print(latest_update)
    data = {}
    for table in updated_tables:
        try:
            logging.info(f'fetching latest data from "{table}" table')
            data[table] = fetch_file_from_ingest(s3_client, f'{table}/{latest_update[table]}.csv')
        except Exception as e:
            logging.info(f'fetching data from table "{table}" failed: {e}')
        else:
            logging.info(f'data from table "{table}" successfully retrieved')
    return data, latest_update

def lambda_processing(event, target):
    logging.info('begin processing')
    s3_client = boto3.client('s3')
    logging.info('fetching data from s3 ingestion bucket')
    updated_tables = event['updates']
    data, latest_update = fetch_data(updated_tables)
    logging.info('processing new data')
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
    # s3_client.put_object(Bucket=config['LAMBDA_BUCKET'], Key='new_parquets.json', Body=json.dumps({'new_parquets': new_files}))
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
    if len(new_files) > 0:
        logging.info('calling warehouse lambda')
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='warehouse-lambda',
            InvocationType='Event',
            Payload=json.dumps({'new_parquets': new_files})
        )
    return latest_update