import boto3
import pandas as pd
import logging
import pg8000.native
import io
# from dotenv import dotenv_values
import json
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
# import psycopg2
from botocore.exceptions import ClientError

# config = dotenv_values(".env")
# conflict_columns = {'dim_staff': 'staff_id', 'dim_counterparty': 'counterparty_id', 'dim_date': 'date_id', 'dim_currency': 'currency_id', 'dim_design': 'design_id', 'dim_location': 'location_id'}
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

config = get_secret("postgres-db-credentials")
bucket_names = get_secret('bucket_names')

# ('postgresql+psycopg2://user:password@hostname/database_name')

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
        obj = client.get_object(Bucket=bucket_names['PROCESSED_BUCKET'], Key=f'{table}.parquet')
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

def upload_dim_table_data(data, dim_table, engine):

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

def upload_fact_table_data(data, fact_table, engine):
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
    new_files = event['new_parquets']
    try:
        # response = s3_client.get_object(Bucket=bucket_names['LAMBDA_BUCKET'], Key='new_parquets.json')
        # result = response['Body'].read().decode('utf-8')
        # new_files = json.loads(result)['new_parquets']
        data = fetch_all_processed_data(new_files)
    except Exception as e:
        raise e
    # logging.info('connecting to RDS')
    # con = connect_warehouse()
    try:
        engine = create_engine(f'postgresql://{config['username']}:{config['password']}@{config['host']}/{config['dbname']}')
    except Exception as e:
        raise e
    
    for table in new_files:
        logging.info(f'loading {table} data to rds')
        if table.startswith('dim'):
            upload_dim_table_data(data, table, engine)
        elif table.startswith('fact'):
            upload_fact_table_data(data, table, engine)
    logging.info('warehousing lambda complete')