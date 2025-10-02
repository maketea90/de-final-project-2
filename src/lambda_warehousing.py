import boto3
import pandas as pd
import logging
import pg8000.native
import psycopg2
import io
import awswrangler as wr
from dotenv import dotenv_values

config = dotenv_values(".env")

def fetch_dim_staff():
    pass

def lambda_warehousing(event, target):
    s3_client = boto3.client('s3')
    data = {}
    try:
        logging.info('fetching dim_staff parquet data from processed bucket')
        dim_staff_parquet = s3_client.get_object(Bucket='nc-joe-processed-bucket-2025', Key='dim_staff.parquet')
        df_dim_staff = pd.read_parquet(io.BytesIO(dim_staff_parquet['Body'].read()))
        data['dim_staff'] = df_dim_staff
        # print(data['dim_staff'].iloc[0])
    except Exception as e:
        logging.info(f'dim_staff parquet file fetch failed due to {e}')
    else:
        logging.info('dim_staff parquet data successfully retrieved')
    try:
        logging.info('fetching fact_sales_order parquet data from processed bucket')
        fact_sales_order_parquet = s3_client.get_object(Bucket='nc-joe-processed-bucket-2025', Key='fact_sales_order.parquet')
        df_fact_sales_order = pd.read_parquet(io.BytesIO(fact_sales_order_parquet['Body'].read()))
        data['fact_sales_order'] = df_fact_sales_order
        # print(data['fact_sales_order'].iloc[0])
    except Exception as e:
        logging.info(f'dim_staff parquet file fetch failed due to {e}')
    else:
        logging.info('dim_staff parquet data successfully retrieved')
    logging.info('attempting connection to rds database')

    con = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])

    logging.info('loading dim_staff data to rds')
    try:
        wr.postgresql.to_sql(data['dim_staff'], con, schema="public", table="dim_staff", mode="overwrite", chunksize=1000)
        logging.info('successfully loaded batch from dim_staff into rds')
    except Exception as e:
        logging.info('failed to load data from table "dim_staff" into rds')
        raise e
    logging.info('loading data from fact_sales_order into rds')
    try:
        wr.postgresql.to_sql(data['fact_sales_order'], con, schema='public', table='fact_sales_order', mode='overwrite', chunksize=1000)
        logging.info('successfully loaded batch from fact_sales_order into rds')
    except Exception as e:
        logging.info('failed to load data from table "fact_sales_order" into rds')
        raise e

    logging.info('process complete')