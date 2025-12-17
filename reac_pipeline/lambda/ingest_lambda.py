import os
import json

from lib.population_api import fetch_population_data, upload_population_json
from lib.bls_sync import sync_bls_to_s3


def handler(event, context):
    """
    Runs daily via EventBridge
    Executes:
      - Part 1: BLS sync
      - Part 2: Population API ingestion
    """

    bucket = os.environ["BUCKET"]
    pop_prefix = os.environ.get("POP_PREFIX", "raw/datausa/population/")
    bls_prefix = os.environ.get("BLS_PREFIX", "bls-folder/")

    print("Starting ingestion lambda")

    # ---------- Part 1: BLS ----------
    #sync_bls_to_s3(bucket=bucket, prefix=bls_prefix)
    print("BLS sync completed")

    # ---------- Part 2: Population API ----------
    population_data = fetch_population_data()
    s3_key = upload_population_json(
        bucket=bucket,
        prefix=pop_prefix,
        data=population_data
    )

    print(f"Population data stored at s3://{bucket}/{s3_key}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Ingestion completed",
            "population_key": s3_key
        })
    }
