import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    lit,
    to_timestamp,
    current_timestamp,
    date_format,
    concat,
)
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

participant_id = os.environ["participant_id"]

spark = setup_spark_session()

input_paths, output_paths = read_input_output_paths()

for input_path, output_path in zip(input_paths, output_paths):
    # Read the Parquet file
    df = read_parquet_file(spark, input_path)
    logger.info(f"Read Parquet file successfully from {input_path}")

    # Add the participant_id as a column
    df = df.withColumn("participant_id", lit(participant_id))

    # Cast effective_date_time and meta__last_updated from string to datetime
    timestamp_format = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

    if "date_written" in df.columns:
        df = df.withColumn(
            "date_written", concat(df["date_written"], lit("T00:00:00.000Z"))
        )
        df = df.withColumn(
            "date_written", to_timestamp(df["date_written"], timestamp_format)
        )
    else:
        df = df.withColumn("date_written", lit("----"))

    if "dispense_request__validity_period__start" in df.columns:
        df = df.withColumn(
            "dispense_request__validity_period__start",
            concat(
                df["dispense_request__validity_period__start"], lit("T00:00:00.000Z")
            ),
        )
        df = df.withColumn(
            "dispense_request__validity_period__start",
            to_timestamp(
                df["dispense_request__validity_period__start"], timestamp_format
            ),
        )
    else:
        df = df.withColumn("dispense_request__validity_period__start", lit("----"))

    if "meta__last_updated" in df.columns:
        df = df.withColumn(
            "meta__last_updated",
            to_timestamp(df["meta__last_updated"], timestamp_format),
        )
    else:
        df = df.withColumn("meta__last_updated", lit("----"))

    # Add  medical_last_updated column with the current timestamp
    df = df.withColumn(
        " medical_last_updated", date_format(current_timestamp(), timestamp_format)
    )

    if "prior_prescription__display" not in df.columns:
        df = df.withColumn("prior_prescription__display", lit("----"))
    if "medication_reference__reference" not in df.columns:
        df = df.withColumn("medication_reference__reference", lit("----"))
    if "dispense_request__quantity__unit" not in df.columns:
        df = df.withColumn("dispense_request__quantity__unit", lit("----"))

    # Write the DataFrame back to Parquet
    write_parquet_file(df, output_path)
    logger.info(f"Wrote Parquet file successfully to {output_path}")
