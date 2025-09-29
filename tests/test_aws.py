from unittest.mock import Mock, patch
from src.lambda_ingestion import lambda_ingestion
import pg8000.native
from dotenv import dotenv_values
import datetime

config = dotenv_values(".env")

con = pg8000.native.Connection(config['USER'], host=config['HOST'], database=config['DATABASE'], port=config['PORT'], password=config['PASSWORD'])

# my_mock_last_updated = Mock(return_value=datetime.datetime.now() + datetime.timedelta(minutes=2))

my_mock_put_object = Mock(return_value = 5)

class MockPutObject:
    def __init__(self,):
        self.put_object = my_mock_put_object

# class MockLastUpdated():
#     def __init__(self):
#         self.run = my_mock_last_updated

def test_aws_s3_client(): 
    patcher_put_object = patch('src.lambda_ingestion.boto3.client', return_value = MockPutObject())
    mock_put_object = patcher_put_object.start()
    assert lambda_ingestion({}, {}) == 'success' or 'no new updates'
    patcher_put_object.stop()


