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

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated']}

# DATETIME_NOW = {'sales_order': datetime.datetime(1,1,1,1,1,1,1), 'staff' : datetime.datetime(1,1,1,1,1,1,1)}

DATETIME_NOW = {table: datetime.datetime(1,1,1,1,1,1,1) for table in TABLE_LIST.keys()}

def lambda_ingestion(event, target):
    s3_client=boto3.client('s3')
    con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])
    for table in ['sales_order', 'staff', 'department']:
        last_updated = con.run(f'SELECT last_updated FROM {table} ORDER BY last_updated DESC LIMIT 1')[0][0]
        if last_updated > DATETIME_NOW[table]:
            logging.info(f'new update for table {table}, processing')
            values = con.run(f'SELECT * FROM {table}')
            df = pd.DataFrame(data=values, columns=TABLE_LIST[table])
            logging.info('connecting to s3')
            try:
                s3_client.put_object(Key=f'{table}/{last_updated.strftime('%a %d %b %Y, %I:%M%p (%f)')}.csv', Body=df.to_csv(index=False), Bucket='nc-joe-ingestion-bucket-2025')
                DATETIME_NOW[table] = last_updated
            except ClientError as e:
                logging.info(f'upload for table {table} failed with error {e}')
                # return 'failure'
            logging.info(f'upload for table {table} successful')
            # return 'success'
        else:
            logging.info(f'no new updates for table {table}')
    stringified_datetimes = {}
    for key, value in DATETIME_NOW.items():
        stringified_datetimes[key] = value.strftime('%a %d %b %Y, %I:%M%p (%f)')
    s3_client.put_object(Key='latest_update.json', Body=json.dumps(stringified_datetimes), Bucket='nc-lambda-bucket-joe-final-project-2025')