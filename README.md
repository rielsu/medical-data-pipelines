# Medical Data Pipeline on AWS
This repository contains the code for a robust and scalable medical data pipeline built on AWS. The pipeline is designed to extract, preprocess, load, and transform data, making it suitable for further analysis or machine learning tasks.

## Key Features
- Data Extraction: The pipeline extracts data from various sources using AWS Lambda functions.
- Data Preprocessing: The data is then preprocessed, cleaned, and formatted for further processing.
- Data Loading: The preprocessed data is loaded into an AWS S3 data lake.
- Data Transformation: The data is transformed into a suitable format for analysis or machine learning tasks.
## AWS Resources Used
- AWS S3: Used as a data lake for storing raw and processed data.
- AWS Lambda: Serverless functions are used for data extraction, preprocessing, loading, and transformation tasks.
- AWS Step Functions: Used to orchestrate the execution of the Lambda functions in a specific order.
- AWS IAM: Manages access permissions for the AWS resources.
- AWS Secrets Manager: Stores and retrieves API keys and other secrets used by the Lambda functions.
- AWS ECR Assets: Docker images for the Lambda functions are stored in ECR.

## AWS Lambda EtLT Pattern Functions

This repository contains a set of AWS Lambda functions designed to implement the EtLT (extraction, Trasnform, loading, and transformation) pattern for data processing workflows. The functions are orchestrated to extract data from various sources, transform it into a consistent format, load it into an efficient storage format, and finally perform advanced transformations and analytics.

![AWS Lambda EtLT Pattern Workflow](lambda/EtLT.jpg "AWS Lambda  EtLT Pattern Workflow")


## Overview

The EtLT pattern is implemented through four main types of Lambda functions:

1. **Extract**: Responsible for data extraction from source systems and storing it in a raw format.
2. **transform**: Handles field renaming and parsing data into flattened JSON files, and extracts schema information.
3. **Load**: Parses JSON files into Apache Parquet format using a defined schema for optimized storage.
4. **Transform**: Performs type casting, aggregations, and joining operations using the SOAL (Spark on AWS Lambda) framework.

## Architecture

The data processing workflow is as follows:

1. **extraction Lambda**: Triggered by an event, this function extracts data from the source system and stores it in an S3 bucket in raw format.
2. **preprocessing Lambda**: This function is invoked after the raw data is stored. It transforms the data into a flattened JSON structure with renamed fields and stores the transformed data in a separate S3 location.
3. **loading Lambda**: Once the data is transformed into JSON, this function converts it into Parquet format using a predefined schema and stores the Parquet files in another S3 bucket.
4. **transformation Lambda**: The final Lambda function in the workflow uses the SOAL framework to perform Spark-based transformations, such as type casting, aggregations, and joins. The results are stored in a staging area for further analysis or consumption by other systems.

## Prerequisites

Before deploying these Lambda functions, ensure you have the following:

- An AWS account with appropriate permissions to create and manage Lambda functions, S3 buckets, and IAM roles.
- The AWS CLI installed and configured with your credentials.
- Knowledge of the SOAL framework for running Spark jobs on AWS Lambda.


## Configuration

Each Lambda function can be configured with environment variables:


- `RAW_BUCKET`: The S3 bucket where raw data is stored by the Extract Lambda.
- `PREPROCESSED_BUCKET`: The S3 bucket where the Transform Lambda stores flattened JSON files.
- `PARQUET_BUCKET`: The S3 bucket where the Load Lambda stores Parquet files.
- `STAGING_BUCKET`: The S3 bucket used by the final Transform Lambda to store processed data.

## Usage

The workflow is event-driven and typically starts with the Extract Lambda function, which can be triggered by a scheduled event or another AWS service. Each subsequent step in the workflow is triggered by the completion of the previous step.

## Monitoring and Logging

AWS CloudWatch is used for monitoring and logging the Lambda functions. Ensure that the Lambda functions are configured to send logs to CloudWatch for monitoring and troubleshooting.

## Security

Ensure that all S3 buckets and Lambda functions follow the best security practices, such as:

- Enabling server-side encryption for S3 buckets.
- Using IAM roles with the principle of least privilege.
- Securing your Lambda functions against unauthorized invocations.

## Support

For support with these Lambda functions or the EtLT pattern, please open an issue in this repository or contact your AWS support representative.

## Contributing

Contributions to this project are welcome. Please fork the repository, make your changes, and submit a pull request.

## License

This project is licensed under the MIT License
