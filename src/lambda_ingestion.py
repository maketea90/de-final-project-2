import pg8000.native
from dotenv import dotenv_values
import pandas as pd
import boto3
import datetime
# import Math
import os
import logging
from botocore.exceptions import ClientError
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

config = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'design': ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location', 'file_name']}

LATEST_UPDATE = {table: "0000-00-00 00:00:00.0" for table in TABLE_LIST.keys()}

def lambda_ingestion(event, target, force_update=False):
    s3_client=boto3.client('s3')
    if force_update:
        global LATEST_UPDATE
        LATEST_UPDATE = {table: "0000-00-00 00:00:00.0" for table in TABLE_LIST.keys()}
        s3_client.put_object(Bucket='nc-lambda-bucket-joe-final-project-2025', Key='latest_update.json', Body=json.dumps(LATEST_UPDATE))
    
    s3_latest_update = s3_client.get_object(Bucket='nc-lambda-bucket-joe-final-project-2025', Key='latest_update.json')
    result = s3_latest_update['Body'].read().decode('utf-8')
    LATEST_UPDATE = json.loads(result)
    
    con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])
    for table in TABLE_LIST.keys():
        last_updated = con.run(f'SELECT last_updated::text as last_updated FROM {table} ORDER BY last_updated DESC LIMIT 1')[0][0]
        if last_updated > LATEST_UPDATE[table]:
            logging.info(f'new update for table "{table}"; processing')
            values = con.run(f'SELECT {', '.join(TABLE_LIST[table])} FROM {table} WHERE last_updated::text > \'{LATEST_UPDATE[table]}\'')
            df = pd.DataFrame(data=values, columns=TABLE_LIST[table])
            logging.info('connecting to s3')
            try:
                s3_client.put_object(Key=f'{table}/{last_updated}.csv', Body=df.to_csv(index=False), Bucket='nc-joe-ingestion-bucket-2025')
                LATEST_UPDATE[table] = last_updated
            except ClientError as e:
                logging.info(f'upload for table "{table}" failed with error {e}')
            else:
                logging.info(f'upload for table "{table}" successful')
        else:
            logging.info(f'no new updates for table "{table}"')
    s3_client.put_object(Key='latest_update.json', Body=json.dumps(LATEST_UPDATE), Bucket='nc-lambda-bucket-joe-final-project-2025')
    logging.info('ingestion lambda complete')
    return LATEST_UPDATE