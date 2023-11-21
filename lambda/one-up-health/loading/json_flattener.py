from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, explode_outer
from pyspark.sql.types import StructType, ArrayType, StringType
import re


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

    def camel_to_snake(self, name):
        """
        Convert camelCase string to snake_case.
        """
        s1 = re.sub("(.)([A-Z][a-z]+)", r"\1_\2", name)
        return re.sub("([a-z0-9])([A-Z])", r"\1_\2", s1).lower()

    def _flatten(self, df: DataFrame, prefix: str = "") -> DataFrame:
        """
        Recursive method to flatten a DataFrame.
        """
        # Check if DataFrame has any struct or array type columns left
        if not any(
            [
                isinstance(field.dataType, (StructType, ArrayType))
                for field in df.schema.fields
            ]
        ):
            return df

        # Explode arrays and flatten structs
        flat_cols = []
        for field in df.schema.fields:
            col_name = field.name
            if isinstance(field.dataType, ArrayType):
                df = df.withColumn(col_name, explode_outer(col(col_name)))
            elif isinstance(field.dataType, StructType):
                for nested_field in field.dataType.fields:
                    nested_col_name = col_name + "." + nested_field.name
                    flat_name = prefix + self.camel_to_snake(
                        col_name + "__" + nested_field.name
                    )
                    flat_cols.append(col(nested_col_name).alias(flat_name))
            else:
                flat_cols.append(
                    col(col_name).alias(prefix + self.camel_to_snake(col_name))
                )

        df = df.select(*flat_cols)

        return self._flatten(df, prefix=prefix)

    def flatten_json(self, input_dir: str) -> DataFrame:
        """
        Load a JSON file into a DataFrame and flatten it.

        Args:
            input_dir (str): Path to the JSON file.

        Returns:
            DataFrame: The flattened DataFrame.
        """

        df = self.spark.read.json(input_dir)
        return self._flatten(df)
