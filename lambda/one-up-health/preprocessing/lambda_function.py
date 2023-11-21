import json
import boto3
import gzip
from io import BytesIO
import os
import logging
from flattener import flatten_json

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Create an S3 client object
s3 = boto3.client("s3")


def lambda_handler(event, context):
    # Get the name of the S3 bucket from the environment variable DATALAKE_BUCKET
    bucket_name = os.environ.get("DATALAKE_BUCKET")
    if not bucket_name:
        raise ValueError("DATALAKE_BUCKET environment variable not set")
    logger.info(f"Using bucket: {bucket_name}")

    # Extract the relevant information from the event
    uploaded_files = event.get("uploadedFiles")
    participant_id = event.get("participantId")

    # Check if there are any uploaded files in the event
    if not uploaded_files:
        logger.error("No uploaded files in the event.")
        return {"statusCode": 400, "body": "No uploaded files provided."}

    logger.info(f"Processing files for participant ID: {participant_id}")
    flattened_files = []

    # Loop through each file in the uploaded_files list
    for file in uploaded_files:
        participant_id = file.get("participantId")
        source = file.get("source")
        version = file.get("version")
        model = file.get("model")
        path = file.get("path")

        logger.info(f"Processing file: {path}")
        object_key = path

        try:
            # Download the ndjson file from S3
            file_obj = s3.get_object(Bucket=bucket_name, Key=object_key)
            file_content = file_obj["Body"].read().decode("utf-8")

            # Process each line in the NDJSON file and write to a new NDJSON file
            flattened_lines = []
            for line in file_content.strip().split("\n"):
                json_obj = json.loads(line)
                flattened_obj = flatten_json(json_obj)
                flattened_lines.append(json.dumps(flattened_obj))

            # Compress the flattened_lines using gzip
            gzip_buffer = BytesIO()
            with gzip.GzipFile(fileobj=gzip_buffer, mode="wb") as gz_file:
                for line in flattened_lines:
                    gz_file.write((line + "\n").encode("utf-8"))
            gzip_buffer.seek(0)

            # Upload the compressed file to S3
            dest_key = (
                path.replace("format=raw", "format=ndjson") + "_flattened.ndjson.gz"
            )
            s3.put_object(Bucket=bucket_name, Key=dest_key, Body=gzip_buffer.getvalue())
            flattened_files.append(
                {
                    "path": dest_key,
                    "participantId": participant_id,
                    "source": source,
                    "version": version,
                    "model": model,
                }
            )
            logger.info(f"Successfully processed and uploaded file: {dest_key}")

        except s3.exceptions.NoSuchKey as e:
            logger.error(f"File not found in S3: {object_key}")
        except json.JSONDecodeError as e:
            logger.error(f"Error decoding JSON in file {path}: {str(e)}")
        except Exception as e:
            logger.error(f"Error processing file {path}: {str(e)}")
            raise e

    logger.info(f"Finished processing files for participant ID: {participant_id}")
    # Return a dictionary with the statusCode and body
    return {
        "statusCode": 200,
        "body": "Files loaded to S3 successfully!",
        "participantId": participant_id,
        "uploadedFiles": flattened_files,
    }
