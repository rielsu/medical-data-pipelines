import boto3
import json

s3_client = boto3.client("s3")
bucket_name = " medical-dev-data-lake"
prefix = "format=parquet/source=oneuphealth/schemas/level=source/version=dstu2/"

# List objects in the schema directory
objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

table_definitions = {}

for obj in objects.get("Contents", []):
    # Read each schema file
    schema_file = obj["Key"]
    if schema_file.endswith("_schema.json"):
        response = s3_client.get_object(Bucket=bucket_name, Key=schema_file)
        schema_content = response["Body"].read().decode("utf-8")
        schema_json = json.loads(schema_content)

        # Extract table name and schema
        table_name = schema_file.split("/")[-1].replace("_schema.json", "")
        table_definitions[table_name] = schema_json["fields"]

# Write the table definitions to a local file
with open("glue_table_definitions.json", "w") as f:
    json.dump(table_definitions, f)
