import io
import json
import zipfile
import boto3

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
dynamodb = boto3.client("dynamodb", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
sns = boto3.client("sns", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
sqs = boto3.client("sqs", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
firehose = boto3.client("firehose", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
iam = boto3.client("iam", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
awslambda = boto3.client("lambda", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)

def ensure_bucket(name):
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if name not in existing:
        s3.create_bucket(Bucket=name)
    print(f"[S3] Bucket '{name}' pronto.")

def ensure_dynamo_table(name, ttl_attr):
    existing = dynamodb.list_tables().get("TableNames", [])
    if name not in existing:
        dynamodb.create_table(
            TableName=name,
            KeySchema=[
                {"AttributeName": "patient_id", "KeyType": "HASH"},
                {"AttributeName": "timestamp", "KeyType": "RANGE"},
            ],
            AttributeDefinitions=[
                {"AttributeName": "patient_id", "AttributeType": "S"},
                {"AttributeName": "timestamp", "AttributeType": "S"},
            ],
            BillingMode="PAY_PER_REQUEST",
        )
        dynamodb.update_time_to_live(
            TableName=name,
            TimeToLiveSpecification={"Enabled": True, "AttributeName": ttl_attr},
        )
    print(f"[DynamoDB] Tabela '{name}' criada com TTL ativo no atributo '{ttl_attr}'.")

def ensure_iam_role():
    role_name = "clinsense-pipeline-role"
    trust = {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Principal": {"Service": ["firehose.amazonaws.com", "lambda.amazonaws.com"]}, "Action": "sts:AssumeRole"}],
    }
    try:
        resp = iam.create_role(RoleName=role_name, AssumeRolePolicyDocument=json.dumps(trust))
        return resp["Role"]["Arn"]
    except iam.exceptions.EntityAlreadyExistsException:
        return iam.get_role(RoleName=role_name)["Role"]["Arn"]

def setup_all():
    ensure_bucket("clinsense-raw-lake")
    ensure_dynamo_table("clinsense_quente", "ttl")
    ensure_dynamo_table("clinsense_alertas", "ttl")
    role_arn = ensure_iam_role()

    # 1. SNS clinsense-alertas + Fila SQS auditora para comprovar disparo
    topic_arn = sns.create_topic(Name="clinsense-alertas")["TopicArn"]
    queue_url = sqs.create_queue(QueueName="clinsense-alertas-audit-queue")["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)
    print(f"[SNS] Tópico '{topic_arn}' criado e inscrito na fila de auditoria.")

    # 2. Firehose (Pipeline 1 Fria: JSON cru particionado por year/month/day/hour no S3)
    stream_name = "clinsense-cold-firehose"
    if stream_name not in firehose.list_delivery_streams()["DeliveryStreamNames"]:
        firehose.create_delivery_stream(
            DeliveryStreamName=stream_name,
            DeliveryStreamType="DirectPut",
            ExtendedS3DestinationConfiguration={
                "RoleARN": role_arn,
                "BucketARN": "arn:aws:s3:::clinsense-raw-lake",
                "Prefix": "telemetry/year=!{timestamp:yyyy}/month=!{timestamp:MM}/day=!{timestamp:dd}/hour=!{timestamp:HH}/",
                "ErrorOutputPrefix": "errors/!{firehose:error-output-type}/",
                "BufferingHints": {"SizeInMBs": 1, "IntervalInSeconds": 5},
                "CompressionFormat": "UNCOMPRESSED",
            },
        )
    print(f"[Firehose] Stream '{stream_name}' configurado (JSON cru -> S3 year/month/day/hour).")

    # 3. Lambda Detector (Pipeline 2 Quente)
    func_name = "clinsense-detector"
    zip_buf = io.BytesIO()
    with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write("scripts/lambda_detector.py", arcname="lambda_detector.py")
    zip_buf.seek(0)

    try:
        awslambda.delete_function(FunctionName=func_name)
    except Exception:
        pass

    awslambda.create_function(
        FunctionName=func_name,
        Runtime="python3.11",
        Role=role_arn,
        Handler="lambda_detector.handler",
        Code={"ZipFile": zip_buf.read()},
        Timeout=15,
        Environment={"Variables": {"SNS_TOPIC_ARN": topic_arn}},
    )
    waiter = awslambda.get_waiter("function_active_v2")
    waiter.wait(FunctionName=func_name)
    print(f"[Lambda] Função '{func_name}' implantada e ativa.")

if __name__ == "__main__":
    setup_all()