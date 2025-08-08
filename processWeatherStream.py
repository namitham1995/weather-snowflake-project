import os
import json
import boto3
import time

s3 = boto3.client('s3', region_name='ap-southeast-1')
sqs = boto3.client('sqs', region_name='ap-southeast-1')

bucket_name = 'weather-raw-json-namitha-01'
QUEUE_URL = 'https://sqs.ap-southeast-1.amazonaws.com/021740144457/weather-processing-queue'

# NEW: Helper function to convert DynamoDB JSON to normal JSON
def convert_dynamodb_json(dynamo_json):
    def convert(value):
        if "N" in value:
            return float(value["N"])
        elif "S" in value:
            return value["S"]
        elif "BOOL" in value:
            return value["BOOL"]
        elif "NULL" in value:
            return None
        elif "M" in value:
            return {k: convert(v) for k, v in value["M"].items()}
        elif "L" in value:
            return [convert(v) for v in value["L"]]
        else:
            return list(value.values())[0]  # fallback

    return {k: convert(v) for k, v in dynamo_json.items()}

def lambda_handler(event, context):
    for record in event['Records']:
        new_image = record.get('dynamodb', {}).get('NewImage', {})
        
        normal_json = convert_dynamodb_json(new_image)  # ✅ Proper conversion

        city = normal_json.get("city", "unknown")
        timestamp = str(normal_json.get("timestamp", int(time.time())))
        city_safe = city.replace(" ", "_").lower()
        filename = f"{city_safe}/{timestamp}.json"

        # ✅ Upload to S3
        s3.put_object(
            Bucket=bucket_name,
            Key=filename,
            Body=json.dumps(normal_json),
            ContentType='application/json'
        )
        print(f"✅ Uploaded to S3: {filename}")

        # ✅ Send to SQS
        response = sqs.send_message(
            QueueUrl=QUEUE_URL,
            MessageBody=json.dumps(normal_json)
        )
        print(f"📩 Sent to SQS: {response.get('MessageId')}")

    return {
        "statusCode": 200,
        "body": "Stream processed & saved to S3 and SQS."
    }
