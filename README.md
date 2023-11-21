# Medical Data Pipeline on AWS
This repository contains the code for a robust and scalable medical data pipeline built on AWS. The pipeline is designed to extract, preprocess, load, and transform data, making it suitable for further analysis or machine learning tasks.

## Key Features
Data Extraction: The pipeline extracts data from various sources using AWS Lambda functions.
Data Preprocessing: The data is then preprocessed, cleaned, and formatted for further processing.
Data Loading: The preprocessed data is loaded into an AWS S3 data lake.
Data Transformation: The data is transformed into a suitable format for analysis or machine learning tasks.
## AWS Resources Used
AWS S3: Used as a data lake for storing raw and processed data.
AWS Lambda: Serverless functions are used for data extraction, preprocessing, loading, and transformation tasks.
AWS Step Functions: Used to orchestrate the execution of the Lambda functions in a specific order.
AWS IAM: Manages access permissions for the AWS resources.
AWS Secrets Manager: Stores and retrieves API keys and other secrets used by the Lambda functions.
AWS ECR Assets: Docker images for the Lambda functions are stored in ECR.

