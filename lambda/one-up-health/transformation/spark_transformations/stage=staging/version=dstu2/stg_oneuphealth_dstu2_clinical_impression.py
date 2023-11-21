import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_timestamp, current_timestamp, date_format
import sys


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


def read_input_output_paths():
    input_paths = os.environ["input_paths"].split(",")
    output_paths = os.environ["output_paths"].split(",")
    logger.info(f"input_paths: {input_paths}")
    logger.info(f"output_paths: {output_paths}")
    return input_paths, output_paths


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


def read_parquet_file(spark, file_path):
    df = spark.read.parquet(file_path)
    return df


def write_parquet_file(df, file_path, mode="overwrite"):
    df.write.mode(mode).parquet(file_path)


logger = setup_logging()
logger.info("start...................")

spark = setup_spark_session()

input_paths, output_paths = read_input_output_paths()

for input_path, output_path in zip(input_paths, output_paths):
    participant_id = os.environ["participant_id"]

    # Read the Parquet file
    df = read_parquet_file(spark, input_path)
    logger.info(f"Read Parquet file successfully from {input_path}")

    # Add the participant_id as a column
    df = df.withColumn("participant_id", lit(participant_id))

    # Cast effective_date_time and meta__last_updated from string to datetime
    timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
    if "date" in df.columns:
        df = df.withColumn("date", to_timestamp(df["date"], timestamp_format))

    # Check if 'meta__last_updated' column exists
    if "meta__last_updated" in df.columns:
        df = df.withColumn(
            "meta__last_updated",
            to_timestamp(df["meta__last_updated"], timestamp_format),
        )

    # Add  medical_last_updated column with the current timestamp
    df = df.withColumn(
        " medical_last_updated", date_format(current_timestamp(), timestamp_format)
    )

    # Write the DataFrame back to Parquet
    write_parquet_file(df, output_path)
    logger.info(f"Wrote Parquet file successfully to {output_path}")
