from aws_cdk import (
    Stack,
    Duration,
    aws_s3 as s3,
    aws_sqs as sqs,
    aws_lambda as _lambda,
    aws_s3_notifications as s3n,
    aws_lambda_event_sources as event_sources,
    aws_events as events,
    aws_events_targets as targets,
)
from constructs import Construct


class RearcPipelineStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # -------------------------------
        # 1) S3 Bucket
        # -------------------------------
        bucket = s3.Bucket(
            self,
            "RearcDataBucket",
            versioned=True
        )

        # -------------------------------
        # 2) SQS Queue
        # -------------------------------
        queue = sqs.Queue(
            self,
            "RearcQueue",
            visibility_timeout=Duration.minutes(10)
        )

        # -------------------------------
        # 3) Lambda Layer (dependencies)
        # -------------------------------
        deps_layer = _lambda.LayerVersion(
            self,
            "DepsLayer",
            code=_lambda.Code.from_asset("layers"),
            compatible_runtimes=[_lambda.Runtime.PYTHON_3_11],
            description="requests, pandas, bs4"
        )

        # -------------------------------
        # 4) Ingest Lambda (Part 1 + 2)
        # -------------------------------
        ingest_lambda = _lambda.Function(
            self,
            "IngestLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="ingest_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(10),
            layers=[deps_layer],
            environment={
                "BUCKET": bucket.bucket_name,
                "POP_PREFIX": "raw/datausa/population/",
                "BLS_PREFIX": "bls-folder/"
            }
        )

        bucket.grant_read_write(ingest_lambda)

        # -------------------------------
        # 5) Schedule Ingest Lambda (daily)
        # -------------------------------
        events.Rule(
            self,
            "DailyIngestSchedule",
            schedule=events.Schedule.rate(Duration.days(1)),
            targets=[targets.LambdaFunction(ingest_lambda)]
        )

        # -------------------------------
        # 6) S3 → SQS notification
        # -------------------------------
        bucket.add_event_notification(
            s3.EventType.OBJECT_CREATED,
            s3n.SqsDestination(queue),
            s3.NotificationKeyFilter(prefix="raw/datausa/population/")
        )

        # -------------------------------
        # 7) Analytics Lambda (Part 3)
        # -------------------------------
        analytics_lambda = _lambda.Function(
            self,
            "AnalyticsLambda",
            runtime=_lambda.Runtime.PYTHON_3_11,
            handler="analytics_lambda.handler",
            code=_lambda.Code.from_asset("lambda"),
            timeout=Duration.minutes(10),
            layers=[deps_layer],
            environment={
                "BUCKET": bucket.bucket_name,
                "POP_PREFIX": "raw/datausa/population/",
                "BLS_KEY": "bls-folder/pr/pr.data.0.Current"
            }
        )

        bucket.grant_read(analytics_lambda)
        queue.grant_consume_messages(analytics_lambda)

        analytics_lambda.add_event_source(
            event_sources.SqsEventSource(queue, batch_size=1)
        )
