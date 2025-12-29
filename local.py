# running analytics lambda locally
import os
import json
from analytics_lambda import lambda_handler

# --------------------------------------------------
# Simulate Lambda environment variables
# --------------------------------------------------
os.environ["BUCKET"] = "rearc-part2"

# --------------------------------------------------
# Simulate SQS → S3 event (POINT TO REAL FILE)
# --------------------------------------------------
event = {
    "Records": [
        {
            "body": json.dumps({
                "Records": [
                    {
                        "s3": {
                            "bucket": {
                                "name": "rearc-part2"
                            },
                            "object": {
                                "key": (
                                    "raw/datausa/population/"
                                    "ingestion_date=2025-12-14/"
                                    "population_3c3797c77a24c26d373bade37e26ea9ee61a8dd54910c489f96a9e5e1892a686.json"
                                )
                            }
                        }
                    }
                ]
            })
        }
    ]
}

context = None

if __name__ == "__main__":
    print("Running analytics lambda locally...\n")

    response = lambda_handler(event, context)

    print("\nLambda response:")
    print(response)









# running ingestion lambda
# import os
# from analytics_lambda import lambda_handler
#
# # --------------------------------------------------
# # Simulate AWS Lambda environment variables
# # --------------------------------------------------
# os.environ["BUCKET"] = "rearc-part2"
# os.environ["POP_PREFIX"] = "raw/datausa/population/"
# #os.environ["BLS_PREFIX"] = "bls-folder/"
#
# # --------------------------------------------------
# # Simulate Lambda invocation
# # --------------------------------------------------
# event = {}          # EventBridge sends an empty event
# context = None      # Context not required for local testing
#
# if __name__ == "__main__":
#     print("Running ingestion lambda locally...\n")
#
#     response = lambda_handler(event, context)
#
#     print("\nLambda response:")
#     print(response)
