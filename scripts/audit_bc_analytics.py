import os
import json
import tempfile
from datetime import datetime, timezone
import boto3
import duckdb

ENDPOINT_URL = "http://127.0.0.1:4566"
REGION = "us-east-1"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

s3 = boto3.client("s3", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
sqs = boto3.client("sqs", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)

def normalize_s3_key_for_windows(key: str) -> str:
    now = datetime.now(timezone.utc)
    return (
        key.replace("!{timestamp:yyyy}", f"{now.year:04d}")
        .replace("!{timestamp:MM}", f"{now.month:02d}")
        .replace("!{timestamp:dd}", f"{now.day:02d}")
        .replace("!{timestamp:HH}", f"{now.hour:02d}")
        .replace(":", "_")
    )

def run_audit():
    resp = s3.list_objects_v2(Bucket="clinsense-raw-lake", Prefix="telemetry/")
    objects = resp.get("Contents", [])
    print(f"[S3 -> DuckDB] {len(objects)} objetos JSON crus encontrados no Firehose S3.")

    with tempfile.TemporaryDirectory() as tmpdir:
        for obj in objects:
            safe_key = normalize_s3_key_for_windows(obj["Key"])
            local_path = os.path.join(tmpdir, safe_key.replace("/", os.sep))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            s3.download_file("clinsense-raw-lake", obj["Key"], local_path)

        glob_pattern = os.path.join(tmpdir, "telemetry", "**", "*").replace("\\", "/")
        con = duckdb.connect(":memory:")
        print("\n=== 1. DUCKDB ANALYTICS SOBRE JSON CRU (read_json_auto + HIVE PARTITIONS) ===")
        df_bi = con.execute(f"""
            SELECT
                CAST(year AS INTEGER) AS year,
                CAST(month AS INTEGER) AS month,
                CAST(day AS INTEGER) AS day,
                CAST(hour AS INTEGER) AS hour,
                COUNT(*) AS total_leituras,
                ROUND(AVG(heart_rate), 1) AS bpm_medio,
                ROUND(AVG(spo2), 1) AS spo2_medio,
                SUM(CASE WHEN heart_rate > 120 OR heart_rate < 50 OR spo2 < 92 THEN 1 ELSE 0 END) AS criticos
            FROM read_json_auto('{glob_pattern}', hive_partitioning=true)
            GROUP BY year, month, day, hour
        """).df()
        print(df_bi.to_string(index=False))

    # 2. Auditoria DynamoDB clinsense_quente (TTL = 24h)
    quente_items = dynamodb.Table("clinsense_quente").scan().get("Items", [])
    print(f"\n=== 2. DYNAMODB clinsense_quente (TTL 24h): {len(quente_items)} registros ===")
    if quente_items:
        sample = quente_items[0]
        print(f"Amostra -> patient_id={sample['patient_id']} | BPM={sample['heart_rate']} | SpO2={sample['spo2']} | ttl={sample['ttl']}")

    # 3. Auditoria DynamoDB clinsense_alertas (TTL = 30d)
    alerta_items = dynamodb.Table("clinsense_alertas").scan().get("Items", [])
    print(f"\n=== 3. DYNAMODB clinsense_alertas (TTL 30d): {len(alerta_items)} alertas gravados ===")
    for item in alerta_items[:3]:
        print(f"Alerta -> patient_id={item['patient_id']} | motivos={item['reasons']} | ttl={item['ttl']}")

    # 4. Auditoria SNS clinsense-alertas (via fila SQS inscrita)
    q_url = sqs.get_queue_url(QueueName="clinsense-alertas-audit-queue")["QueueUrl"]
    msgs = sqs.receive_message(QueueUrl=q_url, MaxNumberOfMessages=3).get("Messages", [])
    print(f"\n=== 4. NOTIFICAÇÕES PUBLICADAS NO SNS 'clinsense-alertas' ({len(msgs)} amostras) ===")
    for m in msgs:
        body = json.loads(m["Body"])
        print(f"SNS Subject: {body.get('Subject')} | Message: {body.get('Message')}")

if __name__ == "__main__":
    run_audit()