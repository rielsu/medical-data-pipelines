import pytest
from unittest.mock import Mock, patch
from lambda_function import lambda_handler


# Mock S3 client
class MockS3Client:
    def __init__(self):
        self.storage = {}

    def get_object(self, Bucket, Key):
        return {"Body": Mock(read=lambda: self.storage[Bucket][Key].encode())}

    def put_object(self, Bucket, Key, Body):
        if Bucket not in self.storage:
            self.storage[Bucket] = {}
        self.storage[Bucket][Key] = Body

    def create_bucket(self, Bucket):
        self.storage[Bucket] = {}


@pytest.fixture(scope="function")
def mock_s3():
    return MockS3Client()


@pytest.fixture(scope="function")
def mock_env_vars(monkeypatch):
    """Mock environment variables"""
    monkeypatch.setenv("DATALAKE_BUCKET", " medical-dev-data-lake")


@patch("lambda_function.s3", new_callable=lambda: MockS3Client())
def test_lambda_handler(mock_s3, mock_env_vars):
    # Set up the mock S3 environment
    bucket_name = " medical-dev-data-lake"
    destination_bucket_name = " medical-dev-data-lake"
    mock_s3.create_bucket(Bucket=bucket_name)
    mock_s3.create_bucket(Bucket=destination_bucket_name)

    # Upload a test file to the source S3 bucket
    object_key = "test.ndjson"
    mock_s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body='{"key1": "value1"}\n{"key2": "value2"}',
    )

    # Define the event to pass to the lambda handler
    event = {"uploadedFiles": [object_key], "participantId": "12345"}

    # Call the lambda handler
    response = lambda_handler(event, None)

    # Check if the response is as expected
    assert response["statusCode"] == 200
    assert response["participantId"] == "12345"
    assert "test.ndjson_flattened.ndjson.gz" in response["uploadedFiles"]

    # Check if the file was created in the destination bucket
    flattened_files = mock_s3.storage[destination_bucket_name]
    assert len(flattened_files) == 2
    assert "test.ndjson_flattened.ndjson.gz" in flattened_files
