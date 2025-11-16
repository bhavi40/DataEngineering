import boto3
import json

def lambda_handler(event, context):

    sf = boto3.client("stepfunctions")

    response = sf.start_execution(
        stateMachineArn="arn:aws:states:us-east-2:104044935003:stateMachine:de-youtube-data-workflow",
        input=json.dumps(event)
    )

    print("Started Step Function:", response)

    return response