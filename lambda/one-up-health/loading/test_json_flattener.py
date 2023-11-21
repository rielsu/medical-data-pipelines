import unittest
import os
from pyspark.sql import SparkSession
from json_flattener import JSONFlattener
import json


class TestJSONFlattener(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder.appName("JSONFlattenerTest")
            .master("local[2]")
            .getOrCreate()
        )

        # Define the path where the NDJSON file will be stored
        cls.json_dir = "test_data/flat_json_test_data.ndjson"

        # Create test data directory if it doesn't exist
        os.makedirs(os.path.dirname(cls.json_dir), exist_ok=True)

        # Define the data to be written to the NDJSON file
        data = {
            "user_id": "124",
            "observations": {
                "date": "2021-01-02",
                "value": {"quantity": 3, "unit": "mg"},
            },
            "userProfile": {"firstName": "John", "lastName": "Doe"},
        }

        # Write the data to the NDJSON file
        with open(cls.json_dir, "w") as f:
            f.write(json.dumps(data) + "\n")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()
        os.remove(cls.json_dir)

    def test_flatten_json(self):
        # Initialize JSONFlattener
        flattener = JSONFlattener(self.spark)

        # Read the JSON file into a DataFrame
        df = self.spark.read.json(self.json_dir)

        # Flatten the DataFrame
        flattened_df = flattener.flatten_json(self.json_dir)

        # Expected schema after flattening, including camelCase fields
        expected_columns = sorted(
            [
                "user_id",
                "observations__date",
                "observations__value__quantity",
                "observations__value__unit",
                "user_profile__first_name",
                "user_profile__last_name",
            ]
        )

        # Get the actual columns after flattening
        actual_columns = sorted([col.name for col in flattened_df.schema.fields])

        # Assert the schema is as expected
        self.assertEqual(actual_columns, expected_columns)

        # Assert the values are as expected, including camelCase fields
        expected_values = [
            (
                "124",
                "2021-01-02",
                3.0,
                "mg",
                "John",
                "Doe",
            ),  # Add expected values for camelCase fields
            # ... include other expected values for other rows
        ]

        actual_values = [
            (
                row["user_id"],
                row["observations__date"],
                row["observations__value__quantity"],
                row["observations__value__unit"],
                row["user_profile__first_name"],
                row["user_profile__last_name"],
            )
            for row in flattened_df.collect()
        ]

        self.assertEqual(actual_values, expected_values)


if __name__ == "__main__":
    unittest.main()
