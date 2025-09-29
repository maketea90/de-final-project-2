from src.lambda_ingestion import lambda_ingestion
from moto import mock_aws
import boto3

@mock_aws
def test_ingestion_lambda():    
    conn = boto3.resource("s3", region_name="us-east-1")
    conn.create_bucket(Bucket="nc-joe-ingestion-bucket-2025")
    conn.create_bucket(Bucket='nc-lambda-bucket-joe-final-project-2025')
    result = 'no new updates'
    while result == 'no new updates':

        result = lambda_ingestion({}, {})
        
    # body = conn.Object("nc-joe-ingestion-bucket-2025", "sales_order/").get()["Body"].read().decode("utf-8")
    
    bucket = conn.Bucket('nc-joe-ingestion-bucket-2025')
    for files in bucket.objects.filter(Prefix='sales_order'):
        print('here', files)
    assert False