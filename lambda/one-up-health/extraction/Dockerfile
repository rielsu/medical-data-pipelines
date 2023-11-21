FROM --platform=linux/amd64 python:3.11-slim

# Set the working directory
WORKDIR /app

# Copy only the specified files
COPY lambda_function.py one_up_health_client.py requirements.txt /app/

# Install the required libraries and AWS Lambda Python runtime interface client
RUN pip install -r requirements.txt

# Set the entry point to the runtime interface client and specify the handler
ENTRYPOINT [ "/usr/local/bin/python", "-m", "awslambdaric" ]
CMD [ "lambda_function.lambda_handler" ]
