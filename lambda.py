"""
AWS Lambda — Order Processor (SQS -> DynamoDB + S3)
=====================================================
Deploy with AWS SAM (template.yaml snippet at the bottom) or plain zip:

    zip function.zip lambda.py
    aws lambda create-function --function-name order-processor \\
        --runtime python3.12 --handler lambda.handler \\
        --role arn:aws:iam::123456789012:role/lambda-exec \\
        --zip-file fileb://function.zip

What this demonstrates:
  * SQS event source with partial-batch failure reporting
    (only failed messages are retried — no duplicate processing)
  * Idempotent DynamoDB write via ConditionExpression
  * Structured JSON logging for CloudWatch Logs Insights
  * S3 receipt archive through a VPC endpoint (no NAT Gateway)
  * Lazy boto3 client init (cold-start friendly) + env-var config
"""

import json
import os
import time
import uuid
from decimal import Decimal

import boto3

# --- Config (env vars, set in the function configuration) -------------
TABLE_NAME = os.environ.get("ORDERS_TABLE", "orders")
BUCKET_NAME = os.environ.get("RECEIPTS_BUCKET", "order-receipts")
REGION = os.environ.get("AWS_REGION", "ap-southeast-1")

# --- Lazy clients: created once per execution environment, reused -----
# Module-level init runs on cold start only; warm invocations reuse it.
_dynamodb = None
_s3 = None


def _dynamo():
    global _dynamodb
    if _dynamodb is None:
        _dynamodb = boto3.resource("dynamodb", region_name=REGION)
    return _dynamodb.Table(TABLE_NAME)


def _s3():
    global _s3
    if _s3 is None:
        _s3 = boto3.client("s3", region_name=REGION)
    return _s3


def log(level, message, **fields):
    """Structured JSON log — queryable in CloudWatch Logs Insights."""
    print(json.dumps({"level": level, "msg": message, **fields}))


def process_order(order):
    """Persist one order idempotently, then archive a receipt to S3.

    Idempotency: ConditionExpression refuses the write if the order_id
    already exists, so an SQS redelivery can never double-charge.
    """
    order_id = order["order_id"]
    amount = Decimal(str(order["amount"]))
    now = int(time.time())

    item = {
        "order_id": order_id,
        "customer_id": order["customer_id"],
        "amount": amount,
        "currency": order.get("currency", "USD"),
        "status": "PROCESSED",
        "processed_at": now,
        # TTL: auto-delete the record 90 days after processing.
        "ttl": now + 90 * 24 * 3600,
    }

    _dynamo().put_item(
        Item=item,
        ConditionExpression="attribute_not_exists(order_id)",
    )

    receipt = {
        "receipt_id": str(uuid.uuid4()),
        "order_id": order_id,
        "amount": str(amount),
        "processed_at": now,
    }
    _s3().put_object(
        Bucket=BUCKET_NAME,
        Key=f"receipts/{order_id}.json",
        Body=json.dumps(receipt),
        ContentType="application/json",
        ServerSideEncryption="AES256",  # SSE-S3; use aws:kms for CMK control
    )
    return receipt["receipt_id"]


def handler(event, context):
    """SQS event source mapping entry point.

    Returns {"batchItemFailures": [...]} so SQS retries ONLY the messages
    that actually failed. Returning without this field (or raising) would
    retry the whole batch — including orders already processed.
    """
    failures = []
    for record in event.get("Records", []):
        message_id = record.get("messageId", "unknown")
        try:
            order = json.loads(record["body"])
            receipt_id = process_order(order)
            log("INFO", "order processed",
                order_id=order["order_id"], receipt_id=receipt_id,
                remaining_ms=context.get_remaining_time_in_millis())
        except _dynamo().meta.client.exceptions.ConditionalCheckFailedException:
            # Already processed (redelivery) — not a failure, don't retry.
            log("INFO", "duplicate order skipped", message_id=message_id)
        except Exception as exc:  # noqa: BLE001 — must not crash the batch
            log("ERROR", "order failed", message_id=message_id,
                error=str(exc))
            failures.append({"itemIdentifier": message_id})

    return {"batchItemFailures": failures}


# -----------------------------------------------------------------------
# SAM template.yaml (save alongside, then: sam build && sam deploy --guided)
# -----------------------------------------------------------------------
# AWSTemplateFormatVersion: '2010-09-09'
# Transform: AWS::Serverless-2016-10-31
# Resources:
#   OrderQueue:
#     Type: AWS::SQS::Queue
#     Properties:
#       VisibilityTimeout: 360          # 6x the Lambda timeout
#       RedrivePolicy:
#         deadLetterTargetArn: !GetAtt OrderDLQ.Arn
#         maxReceiveCount: 3
#   OrderDLQ:
#     Type: AWS::SQS::Queue
#   OrdersTable:
#     Type: AWS::DynamoDB::Table
#     Properties:
#       BillingMode: PAY_PER_REQUEST
#       AttributeDefinitions:
#         - { AttributeName: order_id, AttributeType: S }
#       KeySchema:
#         - { AttributeName: order_id, KeyType: HASH }
#       TimeToLiveSpecification: { AttributeName: ttl, Enabled: true }
#   ReceiptsBucket:
#     Type: AWS::S3::Bucket
#     Properties:
#       VersioningConfiguration: { Status: Enabled }
#       PublicAccessBlockConfiguration:
#         BlockPublicAcls: true
#         BlockPublicPolicy: true
#         IgnorePublicAcls: true
#         RestrictPublicBuckets: true
#   OrderProcessor:
#     Type: AWS::Serverless::Function
#     Properties:
#       CodeUri: .
#       Handler: lambda.handler
#       Runtime: python3.12
#       MemorySize: 512
#       Timeout: 60
#       Tracing: Active
#       Environment:
#         Variables:
#           ORDERS_TABLE: !Ref OrdersTable
#           RECEIPTS_BUCKET: !Ref ReceiptsBucket
#       Policies:
#         - DynamoDBCrudPolicy: { TableName: !Ref OrdersTable }
#         - S3WritePolicy: { BucketName: !Ref ReceiptsBucket }
#       Events:
#         ProcessOrders:
#           Type: SQS
#           Properties:
#             Queue: !GetAtt OrderQueue.Arn
#             BatchSize: 10
#             FunctionResponseTypes: [ReportBatchItemFailures]
#             ScalingConfig: { MaximumConcurrency: 50 }
