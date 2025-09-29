import pg8000.native
from dotenv import dotenv_values
import pandas as pd
import boto3
import datetime
# import Math
import os
import logging
from botocore.exceptions import ClientError

config = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}


def lambda_ingestion(event, target):

    con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])
    time_now = datetime.datetime.now()    
    last_updated_sales_order = con.run('SELECT last_updated FROM sales_order ORDER BY last_updated DESC LIMIT 1')
    # print(last_updated_sales_order)
    last_updated_staff = con.run("SELECT last_updated FROM staff ORDER BY last_updated DESC LIMIT 1")
    print(last_updated_staff)
    if last_updated_sales_order[0][0] > time_now:
        print('updating')
        table = 'sales_order'
        sales_order_columns = ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
        staff_columns = ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated']
        sales_order_values = con.run("SELECT * FROM sales_order")
        staff_values = con.run("SELECT * FROM staff")
        df_sales_order = pd.DataFrame(columns=sales_order_columns, data=sales_order_values)
        df_staff = pd.DataFrame(columns=staff_columns, data=staff_values)
        # upload_file(f'{table}/{last_updated_sales_order}', 'nc-joe-ingestion-bucket-2025', df_sales_order.to_csv(index=False))
        # return 'successfully updated'
        s3_client = boto3.client('s3')
        try:
            s3_client.put_object(Key=f'{table}/{last_updated_sales_order}.csv', Body=df_sales_order.to_csv(index=False), Bucket='nc-joe-ingestion-bucket-2025')
        except ClientError as e:
            logging.error(e)
            return 'failure'
        return 'success'
    elif last_updated_staff[0][0] > time_now:
        print('updating')
        table = 'staff'
        sales_order_columns = ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id']
        staff_columns = ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated']
        sales_order_values = con.run("SELECT * FROM sales_order")
        staff_values = con.run("SELECT * FROM staff")
        df_sales_order = pd.DataFrame(columns=sales_order_columns, data=sales_order_values)
        df_staff = pd.DataFrame(columns=staff_columns, data=staff_values)
        # upload_file(f'{table}/{last_updated_staff}', 'nc-joe-ingestion-bucket-2025', df_staff.to_csv(index=False))
        s3_client = boto3.client('s3')
        try:
            s3_client.put_object(Key=f'{table}/{last_updated_staff}.csv', Body=df_staff.to_csv(index=False), Bucket='nc-joe-ingestion-bucket-2025')
        except ClientError as e:
            logging.error(e)
            return 'failure'
        return 'success'
    else:
        print('no new updates')
        return 'no new updates'

lambda_ingestion({}, {})