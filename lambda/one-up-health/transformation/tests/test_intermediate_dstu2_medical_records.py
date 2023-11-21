import os
import unittest
import shutil
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
import os
import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    lit,
    concat,
    concat_ws,
    format_string,
    current_timestamp,
    date_format,
    to_timestamp,
    col,
    when,
    coalesce,
)
import sys
from functools import reduce


def read_parquet_file(spark, file_path):
    df = spark.read.parquet(file_path)
    return df


def write_parquet_file(df, file_path, mode="overwrite"):
    df.write.mode(mode).parquet(file_path)


def union_all(dfs):
    return reduce(DataFrame.unionByName, dfs)


def transform_based_on_resource_type(df):
    """
    Apply transformation based on the resource_type value in the DataFrame
    """
    # Check for "Observation" resource_type and apply specific transformation
    if (
        "Observation"
        in df.select("resource_type").distinct().rdd.flatMap(lambda x: x).collect()
    ):
        return df.filter(col("resource_type") == "Observation").select(
            col("participant_id"),
            col("id"),
            coalesce(
                col("effective_date_time").cast("string"), col("issued").cast("string")
            ).alias("date"),
            col("resource_type"),
            col("code__text").alias("name"),
            col("reference_range__low")
            .getItem(0)
            .getField("value")
            .cast("string")
            .alias("lower_bound"),
            col("reference_range__high")
            .getItem(0)
            .getField("value")
            .cast("string")
            .alias("upper_bound"),
            col("value_quantity__value").cast("string").alias("actual_value"),
            col("value_quantity__unit").cast("string").alias("unit_type"),
            when(
                col("value_quantity__value").isNotNull()
                & col("value_quantity__unit").isNotNull(),
                concat(
                    col("value_quantity__value").cast("string"),
                    lit(" "),
                    col("value_quantity__unit"),
                ),
            )
            .otherwise(
                when(
                    col("value_codeable_concept__text").isNotNull(),
                    col("value_codeable_concept__text"),
                ).otherwise(col("value_string"))
            )
            .alias("summary"),
            col("meta__last_updated").cast("string").alias("ouh_last_updated"),
            col(" medical_last_updated").cast("string").alias("last_updated"),
        )
    elif (
        "Immunization"
        in df.select("resource_type").distinct().rdd.flatMap(lambda x: x).collect()
    ):
        return df.filter(col("resource_type") == "Immunization").select(
            col("participant_id"),
            col("date").cast("string"),
            col("id"),
            col("resource_type"),
            col("vaccine_code__text").alias("name"),
            lit(None).cast("string").alias("lower_bound"),
            lit(None).cast("string").alias("upper_bound"),
            lit(None).cast("string").alias("actual_value"),
            lit(None).cast("string").alias("unit_type"),
            col("vaccine_code__text").alias("summary"),
            col("meta__last_updated").cast("string").alias("ouh_last_updated"),
            col(" medical_last_updated").cast("string").alias("last_updated"),
        )
    elif (
        "ClinicalImpression"
        in df.select("resource_type").distinct().rdd.flatMap(lambda x: x).collect()
    ):
        return df.filter(col("resource_type") == "ClinicalImpression").select(
            col("participant_id"),
            col("date").cast("string"),
            col("id"),
            col("resource_type"),
            col("description").alias("name"),
            lit(None).cast("string").alias("lower_bound"),
            lit(None).cast("string").alias("upper_bound"),
            lit(None).cast("string").alias("actual_value"),
            lit(None).cast("string").alias("unit_type"),
            concat_ws("", col("description"), col("summary")).alias("summary"),
            col("meta__last_updated").cast("string").alias("ouh_last_updated"),
            col(" medical_last_updated").cast("string").alias("last_updated"),
        )
    elif (
        "MedicationOrder"
        in df.select("resource_type").distinct().rdd.flatMap(lambda x: x).collect()
    ):
        return df.filter(col("resource_type") == "MedicationOrder").select(
            col("participant_id"),
            col("dispense_request__validity_period__start")
            .cast("string")
            .alias("date"),
            col("id"),
            col("resource_type"),
            col("medication_reference__display").alias("name"),
            lit(None).cast("string").alias("lower_bound"),
            lit(None).cast("string").alias("upper_bound"),
            lit(None).cast("string").alias("actual_value"),
            lit(None).cast("string").alias("unit_type"),
            concat_ws(
                " ",
                col("prior_prescription__display"),
                col("medication_reference__reference"),
                col("dispense_request__quantity__unit"),
            ).alias("summary"),
            col("meta__last_updated").cast("string").alias("ouh_last_updated"),
            col(" medical_last_updated").cast("string").alias("last_updated"),
        )


spark = (
    SparkSession.builder.appName("PySpark I/O Testing").master("local[*]").getOrCreate()
)
input_paths = [
    "tests/test_data/staging_clinical_impresions.parquet",
    "tests/test_data/staging_observations.parquet",
    "tests/test_data/staging_immunizations.parquet",
    "tests/test_data/staging_medication_order.parquet",
]

dataframes = [read_parquet_file(spark, input_path) for input_path in input_paths]

transformed_dataframes = [transform_based_on_resource_type(df) for df in dataframes]

final_df = union_all(transformed_dataframes)

distinct_resource_types_df = final_df.select("resource_type").distinct()

# Show the distinct resource types
distinct_resource_types_df.show()

final_df.printSchema()

write_parquet_file(final_df, "medical_records.parquet")
