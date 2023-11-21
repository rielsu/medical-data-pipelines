from pyspark.sql import SparkSession
from pyspark.sql.types import *
import sys
import os
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType


class JSONFlattener:
    """
    This class provides methods to flatten nested JSON structures into
    a Spark DataFrame. The primary motivation is to simplify querying
    and data transformations on nested JSON structures.
    """

    def __init__(self, spark: SparkSession):
        """
        Constructor method.

        Args:
            spark (SparkSession): The SparkSession object.
        """
        self.spark = spark

    def _flatten(self, df, prefix=""):
        """
        Recursive method to flatten a DataFrame.

        Args:
            df (DataFrame): The DataFrame to flatten.
            prefix (str): The prefix to use for nested columns.

        Returns:
            DataFrame: The flattened DataFrame.
        """
        # Check if DataFrame has any struct or array type columns left
        if not any(
            [
                isinstance(field.dataType, (StructType, ArrayType))
                for field in df.schema.fields
            ]
        ):
            return df

        # Explode arrays
        for field in df.schema.fields:
            if isinstance(field.dataType, ArrayType):
                df = df.withColumn(field.name, explode_outer(col(field.name)))

        # Recursively flatten struct fields
        flat_cols = []
        for field in df.schema.fields:
            col_name = field.name
            if isinstance(field.dataType, StructType):
                for nested_field in field.dataType.fields:
                    nested_col_name = col_name + "." + nested_field.name
                    flat_name = prefix + col_name + "_" + nested_field.name
                    flat_cols.append(col(nested_col_name).alias(flat_name))
            else:
                flat_cols.append(col(col_name).alias(prefix + col_name))

        df = df.select(*flat_cols)

        return self._flatten(df, prefix=prefix)

    def flatten_json(self, input_dir):
        """
        Load a JSON file into a DataFrame and flatten it.

        Args:
            input_dir (str): Path to the JSON file.

        Returns:
            DataFrame: The flattened DataFrame.
        """

        df = self.spark.read.json(input_dir)
        return self._flatten(df)


print("start...................")

# Read lists of input paths and output paths from environment variables
input_paths = os.environ["input_paths"].split(",")
output_paths = os.environ["output_paths"].split(",")

print("input_paths ", input_paths)
print("output_paths ", output_paths)

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

flattener = JSONFlattener(spark)

# Ensure input_paths and output_paths have the same length
if len(input_paths) != len(output_paths):
    raise ValueError("The number of input paths and output paths must be the same.")

for input_path, target_path in zip(input_paths, output_paths):
    print(" ******* Processing Input path ", input_path)
    print(" ******* Target path ", target_path)

    print("Started Reading the ndjson file from S3 location ", input_path)
    flat_df = flattener.flatten_json(input_path)

    print("Started Writing the parquet file to Target S3 location ", target_path)
    flat_df.write.mode("overwrite").parquet(target_path)

spark.stop()
