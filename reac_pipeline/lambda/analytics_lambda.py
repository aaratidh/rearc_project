import json
import boto3
import os

s3 = boto3.client("s3")

def handler(event, context):
    print("Analytics Lambda triggered")

    for record in event["Records"]:
        body = json.loads(record["body"])
        s3_event = json.loads(body["Message"])

        for rec in s3_event["Records"]:
            bucket = rec["s3"]["bucket"]["name"]
            key = rec["s3"]["object"]["key"]

            print(f"Reading s3://{bucket}/{key}")
            obj = s3.get_object(Bucket=bucket, Key=key)
            payload = json.loads(obj["Body"].read())

            records = payload.get("data", [])
            if not records:
                print("No 'data' array found — skipping")
                continue

            valid_rows = []
            for row in records:
                if "Year" in row and "Population" in row:
                    valid_rows.append(row)
                else:
                    print(f"Invalid row schema: {list(row.keys())}")

            populations = [
                int(r["Population"])
                for r in valid_rows
                if 2013 <= int(r["Year"]) <= 2018
            ]

            if populations:
                mean_pop = sum(populations) / len(populations)
                print(f"Mean US population (2013–2018): {mean_pop}")
            else:
                print("No valid population records found")

    print("Analytics Lambda completed")