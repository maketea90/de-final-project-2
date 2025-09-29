import boto3

def lambda_processing(event, target):
    s3_client = boto3.client('s3')
    