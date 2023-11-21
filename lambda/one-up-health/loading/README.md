sam package --template-file template.yml --output-template-file packaged.yml --s3-bucket  medical-sam-packages --profile neptune-sandbox

sam deploy --template-file packaged.yml --stack-name YOUR_STACK_NAME --capabilities CAPABILITY_IAM

aws ecr get-login-password --region us-east-2 --profile neptune-sandbox | docker login --username AWS --password-stdin 150313349440.dkr.ecr.us-east-2.amazonaws.com


cdk synth --no-staging

sam local invoke TransformationLambda -e ../lambda/one-up-health/transformation/event.json --profile neptune-sandbox -t ./cdk.out/OneUpHealthDataPipelineStack.template.json