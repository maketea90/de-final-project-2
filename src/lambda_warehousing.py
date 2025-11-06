import boto3
import pandas as pd
import logging
import pg8000.native
import io
from dotenv import dotenv_values
import json
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
import psycopg2

config = dotenv_values(".env")
conflict_columns = {'dim_staff': 'staff_id', 'dim_counterparty': 'counterparty_id', 'dim_date': 'date_id', 'dim_currency': 'currency_id', 'dim_design': 'design_id', 'dim_location': 'location_id'}
engine = create_engine(f'postgresql://postgres:{config['WAREHOUSE_PASSWORD']}@localhost:5432/{config['WAREHOUSE_DATABASE']}')

def postgres_upsert(table, conn, keys, data_iter):

    data = [dict(zip(keys, row)) for row in data_iter]

    insert_statement = insert(table.table).values(data)
    upsert_statement = insert_statement.on_conflict_do_update(
        constraint=f"{table.table.name}_pkey",
        set_={c.key: c for c in insert_statement.excluded},
    )
    conn.execute(upsert_statement)

def fetch_processed_data_by_table(table, client):
    try:
        obj = client.get_object(Bucket=config['PROCESSED_BUCKET'], Key=f'{table}.parquet')
        processed_data = pd.read_parquet(io.BytesIO(obj['Body'].read()))
        return processed_data
    except Exception as e:
        raise e
    
def fetch_all_processed_data(new_files):
    s3_client = boto3.client('s3')
    data = {}
    for table in new_files:
        try:
            logging.info(f'fetching {table} parquet data from processed bucket')
            data[table] = fetch_processed_data_by_table(table, s3_client)
        except Exception as e:
            logging.info(f'{table} parquet file fetch failed: {e}')
        else:
            logging.info(f'{table} parquet data successfully retrieved')
    return data

def upload_dim_table_data(data, dim_table):
    try:   
        # wr.postgresql.to_sql(data[dim_table], con, schema='public', table=f'{dim_table}', mode='upsert', chunksize=1000, upsert_conflict_columns=[conflict_columns[dim_table]])
        data[dim_table].to_sql(f'{dim_table}', 
              con=engine, schema='public',
              if_exists='append',
              index=False,
              method=postgres_upsert) 
        logging.info(f'successfully loaded batch from "{dim_table}" into rds')
    except Exception as e:
        logging.info(f'failed to load data from table "{dim_table}" into rds: {e}')
        raise e

def upload_fact_table_data(data, fact_table):
    try:
        # wr.postgresql.to_sql(data[fact_table], con, schema='public', table=f'{fact_table}', mode='append', chunksize=1000)
        data[fact_table].to_sql(
            name=f"{fact_table}", # table name
            con=engine,  # engine
            if_exists="append", #  If the table already exists, append
            index=False # no index
        )
        logging.info(f'successfully loaded batch from "{fact_table}" into rds')
    except Exception as e:
        logging.info(f'failed to load data from table "{fact_table}" into rds: {e}')
        raise e
    
# def connect_warehouse():
#     try:
#         con = pg8000.Connection('postgres', database=config['WAREHOUSE_DATABASE'], port=config['WAREHOUSE_PORT'], password=config['WAREHOUSE_PASSWORD'])
#         return con
#     except Exception as e:
#         raise e

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
    # con = connect_warehouse()
    for table in new_files:
        logging.info(f'loading {table} data to rds')
        if table.startswith('dim'):
            upload_dim_table_data(data, table)
        elif table.startswith('fact'):
            upload_fact_table_data(data, table)
    logging.info('warehousing lambda complete')