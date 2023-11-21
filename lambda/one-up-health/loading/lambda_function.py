import boto3
import sys
import os
import logging
from pyspark.sql import SparkSession
import re


def setup_logging():
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


def setup_spark_session():
    aws_region = os.environ["AWS_REGION"]
    aws_access_key_id = os.environ["AWS_ACCESS_KEY_ID"]
    aws_secret_access_key = os.environ["AWS_SECRET_ACCESS_KEY"]
    session_token = os.environ["AWS_SESSION_TOKEN"]

    spark = (
        SparkSession.builder.appName("Spark-on-AWS-Lambda")
        .master("local[*]")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        .config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        .config("spark.hadoop.fs.s3a.session.token", session_token)
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
        )
        .getOrCreate()
    )
    return spark


def read_input_output_paths():
    input_paths = os.environ["input_paths"].split(",")
    output_paths = os.environ["output_paths"].split(",")
    if len(input_paths) != len(output_paths):
        raise ValueError("The number of input paths and output paths must be the same.")
    return input_paths, output_paths


def process_input_output_paths(input_paths, output_paths, spark, logger):
    for input_path, target_path in zip(input_paths, output_paths):
        logger.info(f" ******* Processing Input path {input_path}")
        logger.info(f" ******* Target path {target_path}")
        logger.info(f"Started Reading the ndjson file from S3 location {input_path}")
        df = spark.read.json(input_path)
        logger.info(
            f"Started Writing the parquet file to Target S3 location {target_path}"
        )
        df.write.mode("overwrite").parquet(target_path)


def extract_paths(event, datalake_bucket, participant_id):
    uploaded_files = event.get("uploadedFiles", [])
    input_paths = []
    output_paths = []
    schema_paths = []
    uploaded_files_info = []

    for file in uploaded_files:
        participant_id = file.get("participantId")
        source = file.get("source")
        version = file.get("version")
        model = file.get("model")
        file_path = file.get("path")
        input_path = f"s3a://{datalake_bucket}/{file_path}"
        input_paths.append(input_path)
        output_path = f"s3a://{datalake_bucket}/format=parquet/source={source}/version={version}/resource_type={model}/user_id={participant_id}/{model}.parquet"
        schema_path = f"format=parquet/source={source}/schemas/level=source/version={version}/{model}_schema.json"
        output_paths.append(output_path)
        schema_paths.append(schema_path)
        uploaded_files_info.append(
            {
                "path": output_path,
                "model": model,
                "version": version,
                "source": source,
                "participantId": participant_id,
            }
        )

    return input_paths, output_paths, schema_paths, uploaded_files_info


def update_schemas(
    update_schema, schema_paths, input_paths, datalake_bucket, s3_client, spark, logger
):
    if update_schema:
        logger.info("Updating schemas...")
        # After Spark job completion, write the schemas to S3
        for schema_path, input_path in zip(schema_paths, input_paths):
            logger.info(f"Writing schema for {input_path} to {schema_path}")
            df = spark.read.json(input_path)
            schema_json = df.schema.json()
            s3_client.put_object(
                Bucket=datalake_bucket, Key=schema_path, Body=schema_json
            )
            logger.info(f"Schema {schema_json} updated.")


def lambda_handler(event, context):
    logger = setup_logging()
    datalake_bucket = os.environ.get("DATALAKE_BUCKET")
    update_schema = os.environ.get("UPDATE_SCHEMA", "false").lower() == "true"
    s3_client = boto3.client("s3")
    participant_id = event.get("participantId")

    input_paths, output_paths, schema_paths, uploaded_files_info = extract_paths(
        event, datalake_bucket, participant_id
    )

    os.environ["input_paths"] = ",".join(input_paths)
    os.environ["output_paths"] = ",".join(output_paths)

    spark = setup_spark_session()

    process_input_output_paths(input_paths, output_paths, spark, logger)

    update_schemas(
        update_schema,
        schema_paths,
        input_paths,
        datalake_bucket,
        s3_client,
        spark,
        logger,
    )

    spark.stop()

    return {
        "statusCode": 200,
        "body": f"Spark script executed for {len(input_paths)} files.",
        "participantId": participant_id,
        "uploadedFiles": uploaded_files_info,
    }
