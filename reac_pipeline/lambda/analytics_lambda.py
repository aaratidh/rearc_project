import json
import boto3
from statistics import mean
from urllib.parse import unquote_plus

# Simulated SQL query for logging
QUERY_TEXT = """
Query: Calculate mean population between 2013 and 2018
  - Filter rows where Year BETWEEN 2013 AND 2018
  - Group by Nation
  - Compute AVG(Population) per Nation
"""

s3 = boto3.client("s3")


def extract_s3_info(event):
    """
    Supports:
    1. SQS -> S3 event (production)
    2. Direct S3 event
    3. Local testing payload
    """

    # ---- Case 1: SQS event ----
    if "Records" in event and "body" in event["Records"][0]:
        body = event["Records"][0]["body"]
        s3_event = json.loads(body)
        record = s3_event["Records"][0]

    # ---- Case 2: Direct S3 event ----
    elif "Records" in event and "s3" in event["Records"][0]:
        record = event["Records"][0]

    # ---- Case 3: Local manual testing ----
    elif "bucket" in event and "key" in event:
        return event["bucket"], event["key"]

    else:
        raise ValueError("Unsupported event format")

    bucket = record["s3"]["bucket"]["name"]
    key = unquote_plus(record["s3"]["object"]["key"])

    return bucket, key


def handler(event, context):
    print("Starting analytics lambda...")
    print(QUERY_TEXT)
    print("Received event:", json.dumps(event))

    try:
        # -------------------------------
        # Extract S3 location safely
        # -------------------------------
        s3_bucket, s3_key = extract_s3_info(event)

        print(f"Reading file from: s3://{s3_bucket}/{s3_key}")

        # -------------------------------
        # Load JSON from S3
        # -------------------------------
        response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
        raw_data = json.loads(response["Body"].read())

        records = raw_data.get("data", [])
        if not records:
            print("No data found in JSON")
            return {"statusCode": 204, "body": "No records found"}

        # -------------------------------
        # Analytics: Mean population
        # -------------------------------
        filtered = [
            row for row in records
            if 2013 <= int(row["Year"]) <= 2018
        ]

        nation_pop = {}
        for row in filtered:
            nation = row["Nation"]
            pop = int(row["Population"])
            nation_pop.setdefault(nation, []).append(pop)

        mean_results = {
            nation: round(mean(pops))
            for nation, pops in nation_pop.items()
        }

        print("Mean population from 2013–2018 per nation:")
        for nation, avg in mean_results.items():
            print(f"- {nation}: {avg}")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Analytics computed",
                "means": mean_results
            })
        }

    except Exception as e:
        print("ERROR:", str(e))
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
