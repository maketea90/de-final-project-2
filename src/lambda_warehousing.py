import boto3
import pandas as pd
import logging
import pg8000.native
import psycopg2
import io
import awswrangler as wr
from dotenv import dotenv_values
import json

config = dotenv_values(".env")
conflict_columns = {'dim_staff': 'staff_id', 'dim_counterparty': 'counterparty_id', 'dim_date': 'date_id', 'dim_currency': 'currency_id', 'dim_design': 'design_id', 'dim_location': 'location_id'}

def fetch_processed_data_by_table(table):
    try:
        s3_path = f"s3://nc-joe-processed-bucket-2025/{table}.parquet"
        processed_data = wr.s3.read_parquet(s3_path)
        return processed_data
    except Exception as e:
        raise e
    
def fetch_all_processed_data(new_files):
    # s3_client = boto3.client('s3')
    data = {}
    for table in new_files:
        try:
            logging.info(f'fetching {table} parquet data from processed bucket')
            data[table] = fetch_processed_data_by_table(table)
        except Exception as e:
            logging.info(f'{table} parquet file fetch failed: {e}')
        else:
            logging.info(f'{table} parquet data successfully retrieved')
    return data

def upload_dim_table_data(data, dim_table, con):
    try:   
        wr.postgresql.to_sql(data[dim_table], con, schema='public', table=f'{dim_table}', mode='upsert', chunksize=1000, upsert_conflict_columns=[conflict_columns[dim_table]])
        logging.info(f'successfully loaded batch from "{dim_table}" into rds')
    except Exception as e:
        logging.info(f'failed to load data from table "{dim_table}" into rds: {e}')
        raise e

def upload_fact_table_data(data, fact_table, con):
    try:
        wr.postgresql.to_sql(data[fact_table], con, schema='public', table=f'{fact_table}', mode='append', chunksize=1000)
        logging.info(f'successfully loaded batch from "{fact_table}" into rds')
    except Exception as e:
        logging.info(f'failed to load data from table "{fact_table}" into rds: {e}')
        raise e
    
def connect_warehouse():
    try:
        con = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
        return con
    except Exception as e:
        raise e

def lambda_warehousing(event, target):
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(Bucket=config['LAMBDA_BUCKET'], Key='new_parquets.json')
        result = response['Body'].read().decode('utf-8')
        new_files = json.loads(result)['new_parquets']
        data = fetch_all_processed_data(new_files)
    except Exception as e:
        raise e
    logging.info('connecting to RDS')
    con = connect_warehouse()
    for table in new_files:
        logging.info(f'loading {table} data to rds')
        if table.startswith('dim'):
            upload_dim_table_data(data, table, con)
        elif table.startswith('fact'):
            upload_fact_table_data(data, table, con)
    logging.info('warehousing lambda complete')