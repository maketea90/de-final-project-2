import boto3
import pandas as pd
import logging
import pg8000.native
import psycopg2
import io
import awswrangler as wr
from dotenv import dotenv_values

config = dotenv_values(".env")
conflict_columns = {'dim_staff': 'staff_id', 'dim_counterparty': 'counterparty_id', 'dim_date': 'date_id', 'dim_currency': 'currency_id', 'dim_design': 'design_id', 'dim_location': 'location_id'}

def lambda_warehousing(event, target):
    s3_client = boto3.client('s3')
    data = {}
    for table in ['dim_staff', 'dim_counterparty', 'fact_sales_order', 'dim_date', 'dim_currency', 'dim_design',  'dim_location']:
        try:
            logging.info(f'fetching {table} parquet data from processed bucket')
            s3_path = f"s3://nc-joe-processed-bucket-2025/{table}.parquet"
            processed_data = wr.s3.read_parquet(s3_path)
            data[table] = processed_data
        except Exception as e:
            logging.info(f'{table} parquet file fetch failed: {e}')
        else:
            logging.info(f'{table} parquet data successfully retrieved')
    logging.info('attempting connection to rds database')
    con = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
    for dim_table in ['dim_staff', 'dim_counterparty', 'dim_date', 'dim_currency', 'dim_design', 'dim_location']:
        logging.info(f'loading {dim_table} data to rds')
        try:
            print(data[dim_table].iloc[0])
            wr.postgresql.to_sql(data[dim_table], con, schema='public', table=f'{dim_table}', mode='upsert', chunksize=1000, upsert_conflict_columns=[conflict_columns[dim_table]])
            logging.info(f'successfully loaded batch from "{dim_table}" into rds')
        except Exception as e:
            logging.info(f'failed to load data from table "{dim_table}" into rds: {e}')
            raise e
    for fact_table in ['fact_sales_order']:
        logging.info(f'loading {fact_table} data into rds')
        print(data[fact_table].iloc[0])
        try:
            wr.postgresql.to_sql(data[fact_table], con, schema='public', table=f'{fact_table}', mode='append', chunksize=1000)
            logging.info(f'successfully loaded batch from "{fact_table}" into rds')
        except Exception as e:
            logging.info(f'failed to load data from table "{fact_table}" into rds: {e}')
            raise e
    logging.info('warehousing lambda complete')