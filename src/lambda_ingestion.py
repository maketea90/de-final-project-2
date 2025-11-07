import pg8000.native
import pandas as pd
import boto3
import logging
from botocore.exceptions import ClientError
import json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# config = dotenv_values(".env")  # config = {"USER": "foo", "EMAIL": "foo@example.org"}

TABLE_LIST = {'sales_order': ['sales_order_id', 'created_at', 'last_updated', 'design_id', 'staff_id', 'counterparty_id', 'units_sold', 'unit_price', 'currency_id', 'agreed_delivery_date', 'agreed_payment_date', 'agreed_delivery_location_id'], 'staff': ['staff_id', 'first_name', 'last_name', 'department_id', 'email_address', 'created_at', 'last_updated'], 'department': ['department_id', 'department_name', 'location', 'manager', 'created_at', 'last_updated'], 'counterparty': ['counterparty_id', 'counterparty_legal_name', 'legal_address_id', 'commercial_contact', 'delivery_contact', 'created_at', 'last_updated'], 'address': ['address_id', 'address_line_1', 'address_line_2', 'district', 'city', 'postal_code', 'country', 'phone', 'created_at', 'last_updated'], 'currency': ['currency_id', 'currency_code', 'created_at', 'last_updated'], 'design': ['design_id', 'created_at', 'last_updated', 'design_name', 'file_location', 'file_name']}

# LATEST_UPDATE = {table: "0000-00-00 00:00:00.0" for table in TABLE_LIST.keys()}

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
    except Exception as e:
        # For a list of exceptions thrown, see
        # https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        raise e

config_buckets = get_secret('bucket_names')
config_host = get_secret('totesys_database_credentials')

def get_latest_update(client):
    # config = get_secret('bucket_names')
    try:
        s3_latest_update = client.get_object(Bucket=config_buckets['LAMBDA_BUCKET'], Key='latest_update.json')
        result = s3_latest_update['Body'].read().decode('utf-8')
        return json.loads(result)
    except Exception as e:
        logging.info(f'latest_update fetch failed: {e}')
        return {table: "0000-00-00 00:00:00.0" for table in TABLE_LIST.keys()}

def connect_db():
    # config = get_secret('totesys_database_credentials')
    try:
        con = pg8000.native.Connection(config_host['USER'], host=config_host['HOST'], database=config_host['DATABASE'], port=config_host['PORT'], password=config_host['PASSWORD'])
        return con
    except Exception as e:
        logging.info(f'connection to database failed')
        raise e

def upload_data(con, client, table, latest_update):
    # config = get_secret('bucket_names')
    last_updated = con.run(f'SELECT last_updated::text as last_updated FROM {table} ORDER BY last_updated DESC LIMIT 1')[0][0]
    if last_updated > latest_update[table]:
        logging.info(f'new update for table "{table}"; processing')
        values = con.run(f'SELECT {', '.join(TABLE_LIST[table])} FROM {table} WHERE last_updated::text > \'{latest_update[table]}\'')
        df = pd.DataFrame(data=values, columns=TABLE_LIST[table])
        logging.info('connecting to s3')
        try:
            client.put_object(Key=f'{table}/{last_updated}.csv', Body=df.to_csv(index=False), Bucket=config_buckets['INGESTION_BUCKET'])
            latest_update[table] = last_updated
            logging.info(f'upload for table "{table}" successful')
            return True
        except ClientError as e:
            logging.info(f'upload for table "{table}" failed with error {e}')
    else:
        logging.info(f'no new updates for table "{table}"')
        return False

def lambda_ingestion(event, target):
    # config = get_secret('bucket_names')
    s3_client=boto3.client('s3')
    db = connect_db()
    LATEST_UPDATE = get_latest_update(s3_client)
    updated_tables = []
    for table in TABLE_LIST.keys():
        new_update = upload_data(db, s3_client, table, LATEST_UPDATE)
        if(new_update):
            updated_tables.append(table)
    # s3_client.put_object(Key='updated_tables.json', Body=json.dumps({'updates': updated_tables}), Bucket=config['LAMBDA_BUCKET'])
    if len(updated_tables) > 0:
        logging.info('updating records')
        s3_client.put_object(Key='latest_update.json', Body=json.dumps(LATEST_UPDATE), Bucket=config_buckets['LAMBDA_BUCKET'])
        logger.info('Calling process_lambda')
        lambda_client = boto3.client('lambda')
        lambda_client.invoke(
            FunctionName='processing-lambda',
            InvocationType='Event',
            Payload=json.dumps({'updates': updated_tables})
        )
        logging.info('ingestion lambda complete')
    else:
        logger.info('No new updates, ending here')
    return LATEST_UPDATE