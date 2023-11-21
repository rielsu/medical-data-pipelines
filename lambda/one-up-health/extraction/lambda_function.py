import boto3
import asyncio
import logging
from one_up_health_client import OneUpHealthClient
import json
import logging
import os

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_secret(secret_name, region_name):
    # Create a Secrets Manager client

    session = boto3.session.Session()
    client = session.client(service_name="secretsmanager", region_name=region_name)

    # Get the secret value
    get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    return json.loads(get_secret_value_response["SecretString"])


def lambda_handler(event, context):
    """
    AWS Lambda handler to interact with OneUpHealth API and store data in S3.

    Parameters:
    - event (dict): AWS Lambda input event. Must contain 'app_user_id'.
    - context (obj): AWS Lambda context object.

    Returns:
    - dict: Response with status and message.
    """

    # Get app_user_id from the event
    app_user_id = event["app_user_id"]
    logger.info(f"Received app_user_id: {app_user_id}")

    # Define secret details
    secret_name = os.environ.get("SECRET_NAME", "")
    secret_region_name = os.environ.get("SECRET_REGION_NAME", "us-east-2")

    client_id_key = os.environ.get("CLIENT_ID_KEY", "1up_client_id")
    client_secret_key = os.environ.get("CLIENT_SECRET_KEY", "1up_secret_id")

    # Fetch client_id and client_secret from Secrets Manager
    secrets = get_secret(secret_name, secret_region_name)
    client_id = secrets.get(client_id_key, "")
    client_secret = secrets.get(client_secret_key, "")
    logger.info("Retrieved secrets from Secrets Manager.")

    # Create an instance of OneUpHealthClient
    client = OneUpHealthClient(client_id, client_secret, app_user_id)
    logger.info("Initialized OneUpHealthClient.")

    # Generate user authentication code
    auth_code = client.generate_user_auth_code()
    logger.info(f"Generated user authentication code: {auth_code}")

    # Generate access token
    access_token = client.generate_access_token(auth_code)
    logger.info(f"Generated access token: {access_token}")

    # Populate all versions of data
    client.populate_all_versions()
    logger.info("Populated all versions of data.")

    # Fetch the data lake (S3 bucket name) from the environment variable
    data_lake_bucket = os.environ.get("DATALAKE_BUCKET", " medical-datalake")

    uploaded_files = asyncio.run(client.store_all_files_to_s3(data_lake_bucket))
    logger.info(f"Stored all files to S3 bucket: {data_lake_bucket}")

    return {
        "statusCode": 200,
        "body": "Files loaded to S3 successfully!",
        "participantId": app_user_id,
        "uploadedFiles": uploaded_files,
    }
