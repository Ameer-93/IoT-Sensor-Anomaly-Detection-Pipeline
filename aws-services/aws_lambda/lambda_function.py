import boto3
import csv
import json
import urllib.parse

firehose_client = boto3.client("firehose", region_name="eu-north-1")
s3_client = boto3.client("s3")

def lambda_handler(event, context):
    # Loop through all S3 records from the event
    for record in event["Records"]:
        bucket_name = record["s3"]["bucket"]["name"]
        file_key = urllib.parse.unquote_plus(record["s3"]["object"]["key"])
        
        print(f"Processing new file: s3://{bucket_name}/{file_key}")

        # Get the file content
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=file_key)
        lines = s3_object["Body"].read().decode("utf-8").splitlines()
        reader = csv.DictReader(lines)

        count = 0
        for row in reader:
            firehose_client.put_record(
                DeliveryStreamName="IoT-Anomaly-Stream",
                Record={"Data": json.dumps(row) + "\n"}
            )
            count += 1
            if count % 10 == 0:
                print(f"{count} rows sent to Firehose")

        print(f"✅ File {file_key} completed: {count} records uploaded")

    return {
        "statusCode": 200,
        "body": f"Uploaded records from {len(event['Records'])} new file(s) to Firehose"
    }
