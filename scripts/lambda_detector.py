import json
import os
import time
from decimal import Decimal
import boto3

ENDPOINT_URL = f"http://{os.environ.get('LOCALSTACK_HOSTNAME', 'localhost')}:4566"
REGION = "us-east-1"

dynamodb = boto3.resource("dynamodb", endpoint_url=ENDPOINT_URL, region_name=REGION)
sns = boto3.client("sns", endpoint_url=ENDPOINT_URL, region_name=REGION)

table_quente = dynamodb.Table("clinsense_quente")
table_alertas = dynamodb.Table("clinsense_alertas")
SNS_TOPIC_ARN = os.environ.get("SNS_TOPIC_ARN", "arn:aws:sns:us-east-1:000000000000:clinsense-alertas")

def handler(event, context):
    payload = event if isinstance(event, dict) else json.loads(event)
    now_epoch = int(time.time())

    # 1. Grava cada leitura no DynamoDB clinsense_quente com TTL = 24h (86400s)
    item_quente = json.loads(json.dumps(payload), parse_float=Decimal)
    item_quente["ttl"] = now_epoch + 86400
    table_quente.put_item(Item=item_quente)

    hr = int(payload["heart_rate"])
    spo2 = float(payload["spo2"])
    pid = payload["patient_id"]

    # 2. Detector clínico: BPM > 120 ou < 50, ou SpO2 < 92
    reasons = []
    if hr > 120:
        reasons.append(f"Taquicardia (BPM={hr} > 120)")
    elif hr < 50:
        reasons.append(f"Bradicardia (BPM={hr} < 50)")
    if spo2 < 92.0:
        reasons.append(f"Hipoxemia (SpO2={spo2}% < 92%)")

    if reasons:
        alert_msg = f"ALERTA CLÍNICO [{pid}] @ {payload['timestamp']}: " + " | ".join(reasons)

        # Publica no SNS clinsense-alertas
        sns.publish(
            TopicArn=SNS_TOPIC_ARN,
            Subject=f"ClinSense Alerta - {pid}",
            Message=alert_msg,
        )

        # Grava no DynamoDB clinsense_alertas com TTL = 30 dias (2592000s)
        alert_item = {
            "patient_id": pid,
            "timestamp": payload["timestamp"],
            "heart_rate": hr,
            "spo2": Decimal(str(spo2)),
            "reasons": reasons,
            "ttl": now_epoch + (30 * 86400),
        }
        table_alertas.put_item(Item=alert_item)
        return {"status": "ALERT_TRIGGERED", "patient_id": pid, "reasons": reasons}

    return {"status": "OK", "patient_id": pid}