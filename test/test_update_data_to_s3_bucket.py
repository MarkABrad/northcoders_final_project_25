from src.update_data_to_s3_bucket import update_data_to_s3_bucket
from src.upload_to_s3_bucket import write_to_s3_bucket
from utils.utils_for_ingestion import list_of_tables, get_file_contents_of_last_uploaded,\
reformat_data_to_json
import uuid
from pprint import pprint
import pytest
import os
import boto3
from moto import mock_aws
from unittest.mock import patch, Mock
from pprint import pprint
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

@pytest.fixture(scope="module", autouse=True)
def aws_credentials():
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"
    os.environ["AWS_DEFAULT_REGION"] = "eu-west-2"

class TestUploadsDataWithTimeStamp:
    def test_contents_of_file_test_data_in_s3(self):
        with mock_aws():
            bucket_name = f"test-bucket-{uuid.uuid4().hex}"
            s3_client = boto3.client('s3')
            s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
            
            def mock_list_of_tables():
                return["counterparty"]
            
            mock_reformated_data_from_db = Mock()

            with open('test/test_data/test_data.json','r') as f:
                data = json.load(f)
            mock_reformated_data_from_db = Mock(return_value=data["counterparty"])

            write_to_s3_bucket(s3_client, bucket_name, mock_list_of_tables, mock_reformated_data_from_db)
            
            no_of_files_before_update = s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount']

            
            def mock_additional_data(x,y):
                return [{
                "counterparty_id": 21,
                "counterparty_legal_name": "Fahey and Sons",
                "legal_address_id": 15,
                "commercial_contact": "Micheal Toy",
                "delivery_contact": "Mrs. Lucy Runolfsdottir",
                "created_at": "2026-02-03T14:20:51.563000",
                "last_updated": "2026-02-03T14:20:51.563000"
                }]

            mock_data_last_uploaded = Mock(return_value=[{
            "counterparty_id": 20,
            "counterparty_legal_name": "Yost, Watsica and Mann",
            "legal_address_id": 2,
            "commercial_contact": "Sophie Konopelski",
            "delivery_contact": "Janie Doyle",
            "created_at": "2025-11-03T14:20:51.563000",
            "last_updated": "2025-11-03T14:20:51.563000"
            }])
          
            update_data_to_s3_bucket(s3_client, bucket_name, mock_list_of_tables,  
                                mock_additional_data, mock_data_last_uploaded)
            print(s3_client.list_objects_v2(Bucket=bucket_name))
            assert s3_client.list_objects_v2(Bucket=bucket_name)['KeyCount'] == (no_of_files_before_update +1)

            
            json_file_on_s3 = s3_client.list_objects_v2(Bucket=bucket_name)
            timestamped_files = []

            for file_key in json_file_on_s3['Contents']:
                if file_key["Key"].endswith('.json'):
                    file_key_without_extension = file_key["Key"].replace('.json', '')
                    timestamp_str = file_key_without_extension.split('/')[1:5]  
                    timestamp = ' '.join(timestamp_str)
                    try:
                        timestamp_obj = datetime.strptime(timestamp, '%Y %m %d %H:%M:%S.%f')
                        timestamped_files.append((file_key["Key"], timestamp_obj))
                    except ValueError as e:
                        pprint(f"Error parsing timestamp for file {file_key['Key']}: {e}")

            timestamped_files.sort(key=lambda x: x[1], reverse=True)

            if timestamped_files:
                json_file_last_updated = timestamped_files[0][0]
                print(f"Most recent file is: {json_file_last_updated}")
            else:
                print("No JSON files found in the bucket.")

            assert json_file_last_updated == 'counterparty/2026/02/03/14:20:51.563000.json'


            file_data = s3_client.get_object(
                Bucket=bucket_name, 
                Key=json_file_last_updated,
            )
            data = file_data['Body'].read().decode('utf-8')

            # Fetch the file content from S3 and check if file is empty
            file_data = s3_client.get_object(
                Bucket=bucket_name, 
                Key=file_key['Key']
            )
            data = file_data['Body'].read().decode('utf-8')

            
            if not data.strip():  
                    pprint(f"File {json_file_last_updated} is empty")
            else:
                try:
                    file_content = json.loads(data)
                    pprint(f"File content for {json_file_last_updated}: {file_content}")
                    
                except json.JSONDecodeError as e:
                    pprint(f"Error decoding JSON in {fil_key}: {e}")
                except Exception as e:
                    pprint(f"An unexpected error occurred for {json_file_last_updated}: {e}")

            data_from_s3_bucket = []

            if 'file_content' in locals():
                for dict_item in file_content:
                    data_from_s3_bucket.append(dict_item)
                    
                with open('test/test_data/test_data.json','r') as f:
                    db_data = json.load(f)["counterparty"]

                db_data.append(mock_additional_data("x","y")[0])
                expected = db_data

                assert sorted(data_from_s3_bucket, key=lambda d: d["counterparty_id"]) == \
                    sorted(expected, key=lambda d: d["counterparty_id"])

class TestErrorRaising:
    def test_returns_error_if_txt_file_empty_or_currupt(self, caplog):
            with mock_aws():
                bucket_name = f"test-bucket-{uuid.uuid4().hex}"
                s3_client = boto3.client('s3')
                s3_client.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint':'eu-west-2'}
                )
                with caplog.at_level(logging.INFO):
                    result = update_data_to_s3_bucket(
                    s3_client,
                    bucket_name,
                    list_of_tables,
                    reformat_data_to_json,
                    get_file_contents_of_last_uploaded,
                    )
                    assert not result
                    assert "NoneType" in caplog.text

    def test_logs_error_when_no_bucket_found(self,caplog):
        with mock_aws():
            bucket_name = f"test-bucket-{uuid.uuid4().hex}"
            s3_client = boto3.client("s3")
            with caplog.at_level(logging.INFO):
                result = update_data_to_s3_bucket(
                    s3_client,
                    bucket_name,
                    list_of_tables,
                    reformat_data_to_json,
                    get_file_contents_of_last_uploaded,
                    )
                assert not result
                assert "NoneType" in caplog.text
        
