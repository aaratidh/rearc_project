import json
import hashlib
import requests
import boto3
from datetime import datetime, timezone
from botocore.exceptions import ClientError

s3 = boto3.client("s3")

API_URL = (
    "https://honolulu-api.datausa.io/tesseract/data.jsonrecords"
    "?cube=acs_yg_total_population_1"
    "&drilldowns=Year,Nation"
    "&locale=en"
    "&measures=Population"
)


def fetch_population_data() -> dict:
    response = requests.get(API_URL, timeout=30)
    response.raise_for_status()
    return response.json()


def compute_hash(data: dict) -> str:
    json_bytes = json.dumps(data, sort_keys=True).encode("utf-8")
    return hashlib.sha256(json_bytes).hexdigest()


def object_exists(bucket: str, key: str) -> bool:
    try:
        s3.head_object(Bucket=bucket, Key=key)
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            return False
        raise


def upload_population_json(bucket: str, prefix: str, data: dict) -> str:
    data_hash = compute_hash(data)
    ingestion_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    key = (
        f"{prefix}/ingestion_date={ingestion_date}/"
        f"population_{data_hash}.json"
    )

    if object_exists(bucket, key):
        print("Duplicate population data detected — skipping upload")
        return key

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(data).encode("utf-8"),
        ContentType="application/json"
    )

    print(f"Uploaded population data to s3://{bucket}/{key}")
    return key
