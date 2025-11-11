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
from sqlalchemy import Integer, String, Float, Date, Time, ForeignKey
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)
# config = dotenv_values(".env")
# conflict_columns = {'dim_staff': 'staff_id', 'dim_counterparty': 'counterparty_id', 'dim_date': 'date_id', 'dim_currency': 'currency_id', 'dim_design': 'design_id', 'dim_location': 'location_id'}
class Base(DeclarativeBase):
    pass


# --- Dimension Tables ---

class Counterparty(Base):
    __tablename__ = "dim_counterparty"

    counterparty_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    counterparty_legal_name: Mapped[str] = mapped_column(String, nullable=False)
    counterparty_legal_address_line_1: Mapped[str] = mapped_column(String, nullable=False)
    counterparty_legal_address_line_2: Mapped[str] = mapped_column(String)
    counterparty_legal_district: Mapped[str] = mapped_column(String)
    counterparty_legal_city: Mapped[str] = mapped_column(String, nullable=False)
    counterparty_legal_postcode: Mapped[str] = mapped_column(String, nullable=False)
    counterparty_legal_country: Mapped[str] = mapped_column(String, nullable=False)
    counterparty_legal_phone_number: Mapped[str] = mapped_column(String, nullable=False)


class Staff(Base):
    __tablename__ = "dim_staff"

    staff_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    first_name: Mapped[str] = mapped_column(String, nullable=False)
    last_name: Mapped[str] = mapped_column(String, nullable=False)
    email_address: Mapped[str] = mapped_column(String, nullable=False)
    department_name: Mapped[str] = mapped_column(String, nullable=False)
    location: Mapped[str] = mapped_column(String, nullable=False)


class Currency(Base):
    __tablename__ = "dim_currency"

    currency_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    currency_code: Mapped[str] = mapped_column(String, nullable=False)
    currency_name: Mapped[str] = mapped_column(String, nullable=False)


class Dates(Base):
    __tablename__ = "dim_date"

    date_id: Mapped[datetime.date] = mapped_column(Date, primary_key=True)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    month: Mapped[int] = mapped_column(Integer, nullable=False)
    day: Mapped[int] = mapped_column(Integer, nullable=False)
    day_of_week: Mapped[int] = mapped_column(Integer, nullable=False)
    day_name: Mapped[str] = mapped_column(String, nullable=False)
    month_name: Mapped[str] = mapped_column(String, nullable=False)
    quarter: Mapped[int] = mapped_column(Integer, nullable=False)


class Design(Base):
    __tablename__ = "dim_design"

    design_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    design_name: Mapped[str] = mapped_column(String, nullable=False)
    file_location: Mapped[str] = mapped_column(String, nullable=False)
    file_name: Mapped[str] = mapped_column(String, nullable=False)


class Location(Base):
    __tablename__ = "dim_location"

    location_id: Mapped[int] = mapped_column(Integer, primary_key=True, nullable=False)
    address_line_1: Mapped[str] = mapped_column(String, nullable=False)
    address_line_2: Mapped[str] = mapped_column(String)
    district: Mapped[str] = mapped_column(String)
    city: Mapped[str] = mapped_column(String, nullable=False)
    postal_code: Mapped[str] = mapped_column(String, nullable=False)
    country: Mapped[str] = mapped_column(String, nullable=False)
    phone: Mapped[str] = mapped_column(String, nullable=False)


# --- Fact Table ---

class SalesOrder(Base):
    __tablename__ = "fact_sales_order"

    sales_order_id: Mapped[int] = mapped_column(Integer, nullable=False)
    design_id: Mapped[int] = mapped_column(Integer, ForeignKey("dim_design.design_id"), nullable=False)
    sales_staff_id: Mapped[int] = mapped_column(Integer, ForeignKey("dim_staff.staff_id"), nullable=False)
    counterparty_id: Mapped[int] = mapped_column(Integer, ForeignKey("dim_counterparty.counterparty_id"), nullable=False)
    units_sold: Mapped[int] = mapped_column(Integer, nullable=False)
    unit_price: Mapped[float] = mapped_column(Float, nullable=False)
    currency_id: Mapped[int] = mapped_column(Integer, ForeignKey("dim_currency.currency_id"), nullable=False)
    agreed_delivery_date: Mapped[datetime.date] = mapped_column(Date, ForeignKey("dim_date.date_id"), nullable=False)
    agreed_payment_date: Mapped[datetime.date] = mapped_column(Date, ForeignKey("dim_date.date_id"), nullable=False)
    agreed_delivery_location_id: Mapped[int] = mapped_column(Integer, ForeignKey("dim_location.location_id"), nullable=False)
    last_updated_date: Mapped[datetime.date] = mapped_column(Date, ForeignKey("dim_date.date_id"), nullable=False)
    last_updated_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)
    created_date: Mapped[datetime.date] = mapped_column(Date, ForeignKey("dim_date.date_id"), nullable=False)
    created_time: Mapped[datetime.time] = mapped_column(Time, nullable=False)

    sales_record_id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)


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
logging.info('fetching rds credentials')
config = get_secret("postgres-db-credentials-2")

DATABASE_URL = f'postgresql://{config['username']}:{config['password']}@{config['host']}/{config['dbname']}'
        
logging.info('creating tables')
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)

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
    dim_tables = [table for table in new_files if table.startswith('dim')]
    fact_tables = [table for table in new_files if table.startswith('fact')]
    
    for table in dim_tables:
        logging.info(f'loading {table} data to rds')
        upload_dim_table_data(data, table, engine)
    
    for table in fact_tables:
        logging.info(f'loading {table} data to rds')
        upload_fact_table_data(data, table, engine)
    
    # for table in new_files:
    #     logging.info(f'loading {table} data to rds')
    #     if table.startswith('dim'):
    #         upload_dim_table_data(data, table, engine)
    #     elif table.startswith('fact'):
    #         upload_fact_table_data(data, table, engine)
    logging.info('warehousing lambda complete')

# lambda_warehousing()