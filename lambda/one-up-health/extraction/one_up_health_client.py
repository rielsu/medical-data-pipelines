import requests
import os
import boto3
from botocore.config import Config
import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import datetime
import logging


# Define a class to interact with the OneUpHealth API
class OneUpHealthClient:
    # Initialize the client with necessary credentials and setup
    def __init__(self, client_id, client_secret, app_user_id):
        self.client_id = client_id
        self.client_secret = client_secret
        self.app_user_id = app_user_id
        self.access_token = None
        self.refresh_token = None
        # Store exported data for different FHIR versions
        self.exported_data = {"dstu2": {}, "stu3": {}, "r4": {}}
        # Configure boto3 for higher concurrency
        self.config = Config(max_pool_connections=50)
        self.s3_client = boto3.client("s3", config=self.config)

    # Generate an authorization code for the user
    def generate_user_auth_code(self):
        # Endpoint to generate the auth code
        url = "https://api.1up.health/user-management/v1/user/auth-code"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "app_user_id": self.app_user_id,
        }
        response = requests.post(url, headers=headers, data=data)
        # Print the response for debugging purposes
        print(response.text)
        if response.status_code == 200:
            return response.json().get("code")
        else:
            raise Exception(
                f"Failed to generate auth code. Status code: {response.status_code}, Response: {response.text}"
            )

    # Use the auth code to generate an access token
    def generate_access_token(self, auth_code):
        # Endpoint to get the access token
        url = "https://auth.1up.health/oauth2/token"
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        data = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": auth_code,
            "grant_type": "authorization_code",
        }
        response = requests.post(url, headers=headers, data=data)
        if response.status_code == 200:
            self.access_token = response.json().get("access_token")
            self.refresh_token = response.json().get("refresh_token")
            return self.access_token
        else:
            raise Exception(
                f"Failed to generate access token. Status code: {response.status_code}, Response: {response.text}"
            )

    # Query all users from the OneUpHealth API
    def query_all_users(self):
        url = "https://api.1up.health/r4/Patient"
        headers = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "x-oneup-user-id": "client",
        }
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                f"Failed to query all users. Status code: {response.status_code}, Response: {response.text}"
            )

    # Export bulk data for a specific FHIR version
    def bulk_export_for_version(self, fhir_version):
        url = f"https://analytics.1up.health/bulk-data/{fhir_version}/$export"
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            data = response.json()
            for output in data["output"]:
                self.exported_data[fhir_version][output["type"]] = output["url"]
        else:
            raise Exception(
                f"Failed to export bulk data for {fhir_version}. Status code: {response.status_code}, Response: {response.text}"
            )

    # Export data for all FHIR versions
    def populate_all_versions(self):
        for version in ["dstu2", "stu3", "r4"]:
            self.bulk_export_for_version(version)

    # Download the exported data in ndjson format
    def download_ndjson(self, url):
        headers = {"Authorization": f"Bearer {self.access_token}"}
        response = requests.get(url, headers=headers)
        if response.status_code == 200:
            return response.text
        else:
            raise Exception(
                f"Failed to download ndjson. Status code: {response.status_code}, Response: {response.text}"
            )

    # Download all exported files and save them locally
    def download_all_files(self, storage_directory="json"):
        user_directory = os.path.join(storage_directory, self.app_user_id)
        if not os.path.exists(user_directory):
            os.makedirs(user_directory)

        # Create directories for each FHIR version
        [
            os.makedirs(os.path.join(user_directory, version))
            for version in self.exported_data
            if not os.path.exists(os.path.join(user_directory, version))
        ]

        # Download and save the data for each FHIR version and data type
        [
            open(
                os.path.join(user_directory, version, f"{data_type}.ndjson"), "w"
            ).write(self.download_ndjson(url))
            for version, data_types in self.exported_data.items()
            for data_type, url in data_types.items()
        ]

    # Upload a file to S3 asynchronously
    async def upload_to_s3(
        self, bucket_name, file_content, source, version, year, month, day, data_type
    ):
        loop = asyncio.get_event_loop()
        s3_path = f"format=raw/source={source}/user_id={self.app_user_id}/version={version}/year={year}/month={month}/day={day}/{year}{month}{day}_{data_type}.ndjson"
        func = partial(
            self.s3_client.put_object,
            Bucket=bucket_name,
            Key=s3_path,
            Body=file_content,
        )
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, func)
        logging.info(f"Successfully uploaded file to S3: {s3_path}")
        return {
            "participantId": self.app_user_id,
            "source": source,
            "version": version,
            "model": data_type,
            "path": s3_path,
        }

    # Store all the downloaded files to S3
    async def store_all_files_to_s3(self, bucket_name):
        tasks = []
        # Get the current date for the S3 path
        now = datetime.datetime.now()
        year, month, day = f"{now.year}", f"{now.month:02}", f"{now.day:02}"

        # Create tasks to upload each file to S3
        for version, data_types in self.exported_data.items():
            for data_type, url in data_types.items():
                content = self.download_ndjson(url)
                source = "oneuphealth"
                tasks.append(
                    self.upload_to_s3(
                        bucket_name,
                        content,
                        source,
                        version,
                        year,
                        month,
                        day,
                        data_type,
                    )
                )

        # Execute all the upload tasks asynchronously
        uploaded_files = await asyncio.gather(*tasks)
        logging.info(f"Total files uploaded: {len(uploaded_files)}")
        return uploaded_files


# Sample usage of the OneUpHealthClient class
if __name__ == "__main__":
    client = OneUpHealthClient(
        client_id="eb55054dc2c5eb4xxxxxxx9133b05b5ba",
        client_secret="7a345558c1xxxxxxxc1eab6c3aaa8258",
        app_user_id="raghav_salma_test_patient_1",
    )

    auth_code = client.generate_user_auth_code()
    access_token = client.generate_access_token(auth_code)
    client.populate_all_versions()
    asyncio.run(client.store_all_files_to_s3("fhir-datalake"))
