import boto3
import sys
import os
import subprocess
import logging
import json
import re

# Set up logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)


def snake_case(s):
    return "".join(["_" + c.lower() if c.isupper() else c for c in s]).lstrip("_")


def get_stage_prefix(stage):
    if stage == "staging":
        return "stg"
    elif stage == "intermediate":
        return "int"
    else:
        return ""


def create_output_path(event, datalake_bucket):
    participant_id = event.get("participantId")
    source = event.get("source")
    stage = event.get("stage")
    version = event.get("version")
    model = event.get("model")
    stage_prefix = get_stage_prefix(stage)
    model_snake_case = snake_case(model)
    return f"s3a://{datalake_bucket}/format=parquet/source={source}/stage={stage}/version={version}/resource_type={model}/user_id={participant_id}/{stage_prefix}_{model_snake_case}.parquet"


def process_event_list(event_list, datalake_bucket):
    """
    Check if the event is a list of events or a single event.
    Returns True if it's a list of events, False if it's a single event.
    """
    filtered_events = [event for event in event_list if event.get("status") != "skip"]
    participant_id = filtered_events[0].get("participantId")
    source = filtered_events[0].get("source")
    model = " medical_medical_records"
    stage = "intermediate"
    version = filtered_events[0].get("version")

    stage_prefix = "int"
    model_snake_case = snake_case(model)
    input_paths = [event.get("path")[0] for event in filtered_events]
    output_paths = [
        create_output_path(event, datalake_bucket) for event in filtered_events
    ]

    spark_script_path = f"spark_transformations/source={source}/stage={stage}/version={version}/{stage_prefix}_{source}_{version}_{model_snake_case}.py"

    return (
        input_paths,
        output_paths,
        spark_script_path,
        {
            "version": version,
            "model": model,
            "participantId": participant_id,
            "source": source,
            "spark_script_path": spark_script_path,
        },
    )


def process_event(event, datalake_bucket):
    """
    Process a single event.
    """
    if isinstance(event, list):
        return process_event_list(event, datalake_bucket)

    participant_id = event.get("participantId")
    source = event.get("source")
    stage = event.get("stage")
    version = event.get("version")
    model = event.get("model")
    input_path = event.get("path")

    stage_prefix = get_stage_prefix(stage)
    model_snake_case = snake_case(model)

    output_path = create_output_path(event, datalake_bucket)

    spark_script_path = f"spark_transformations/source={source}/stage={stage}/version={version}/{stage_prefix}_{source}_{version}_{model_snake_case}.py"

    return (
        [input_path],
        [output_path],
        spark_script_path,
        {
            "version": version,
            "model": model,
            "participantId": participant_id,
            "source": source,
            "spark_script_path": spark_script_path,
        },
    )


def check_file_exists(s3_bucket: str, file_path: str) -> bool:
    """
    Check if a file exists in an S3 bucket.
    """
    s3_client = boto3.client("s3")
    try:
        s3_client.head_object(Bucket=s3_bucket, Key=file_path)
        return True
    except Exception as e:
        logger.error(f"File {file_path} not found in {s3_bucket}: {e}")
        return False


def s3_script_download(s3_bucket_script: str, input_script: str) -> None:
    """
    Downloads a script from an S3 bucket to the /tmp directory.
    """
    s3_client = boto3.resource("s3")

    try:
        logger.info(
            f"Downloading script {input_script} from {s3_bucket_script} to /tmp"
        )
        s3_client.Bucket(s3_bucket_script).download_file(
            input_script, "/tmp/spark_script.py"
        )
    except Exception as e:
        logger.error(
            f"Error downloading script {input_script} from {s3_bucket_script}: {e}"
        )
    else:
        logger.info(f"Script {input_script} successfully downloaded to /tmp")


def spark_submit(s3_bucket_script: str, input_script: str, event: dict) -> None:
    """
    Submits a local Spark script using spark-submit.
    """
    # Run the spark-submit command on the local copy of the script
    try:
        logger.info(f"Submitting Spark script {input_script} from {s3_bucket_script}")
        subprocess.run(
            ["spark-submit", "/tmp/spark_script.py", "--event", json.dumps(event)],
            check=True,
        )
    except Exception as e:
        logger.error(f"Error submitting Spark script with exception: {e}")
        raise e
    else:
        logger.info(f"Script {input_script} successfully submitted")


def lambda_handler(event, context):
    """
    Lambda_handler is called when the AWS Lambda is triggered. The function is downloading file
    from Amazon S3 location and spark submitting the script in AWS Lambda.
    """
    logger.info("******************Start AWS Lambda Handler************")

    s3_bucket_script = os.environ.get("SCRIPT_BUCKET")
    datalake_bucket = os.environ.get("DATALAKE_BUCKET")
    # input_script = os.environ["SPARK_SCRIPT"]
    input_paths, output_paths, spark_script_path, event_details = process_event(
        event, datalake_bucket
    )

    os.environ["participant_id"] = event_details.get("participantId")

    os.environ["input_paths"] = ",".join(input_paths)
    os.environ["output_paths"] = ",".join(output_paths)

    # Check if the Spark script exists in the S3 bucket
    if not check_file_exists(s3_bucket_script, spark_script_path):
        return {
            "statusCode": 200,
            "body": f"Spark script skipped as it does not exist in the bucket. in {spark_script_path}",
            "status": "skip",
        }

    s3_script_download(s3_bucket_script, spark_script_path)
    spark_submit(s3_bucket_script, spark_script_path, event)

    return {
        "statusCode": 200,
        "status": "proceed",
        "path": output_paths,
        **event_details,
    }
