import os
from decimal import Decimal
import boto3
import duckdb
from fastapi import FastAPI

app = FastAPI(title="ClinSense Grafana Bridge API")

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


def _clean(obj):
    if isinstance(obj, list):
        return [_clean(i) for i in obj]
    if isinstance(obj, dict):
        return {k: _clean(v) for k, v in obj.items()}
    if isinstance(obj, Decimal):
        return float(obj) if obj % 1 else int(obj)
    return obj


@app.get("/api/kpis")
def get_kpis():
    quente = dynamodb.Table("clinsense_quente").scan().get("Items", [])
    alertas = dynamodb.Table("clinsense_alertas").scan().get("Items", [])
    items = _clean(quente)

    pacientes = {i.get("patient_id") for i in items if i.get("patient_id")}
    bpms = [float(i["bpm"]) for i in items if "bpm" in i]
    spo2s = [float(i["spo2"]) for i in items if "spo2" in i]

    return [
        {
            "pacientes_ativos": len(pacientes),
            "leituras_quentes": len(items),
            "alertas_criticos": len(alertas),
            "bpm_medio": round(sum(bpms) / len(bpms), 1) if bpms else 0,
            "spo2_medio": round(sum(spo2s) / len(spo2s), 1) if spo2s else 0,
        }
    ]


@app.get("/api/telemetry")
def get_telemetry():
    items = _clean(dynamodb.Table("clinsense_quente").scan().get("Items", []))
    items.sort(key=lambda x: str(x.get("timestamp", "")))
    return items[-200:]


@app.get("/api/alerts")
def get_alerts():
    items = _clean(dynamodb.Table("clinsense_alertas").scan().get("Items", []))
    items.sort(key=lambda x: str(x.get("timestamp", "")), reverse=True)
    return items[:50]


@app.get("/api/s3-summary")
def get_s3_summary():
    con = duckdb.connect(":memory:")
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(
        """
        SET s3_endpoint='localstack:4566';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
        SET s3_access_key_id='test';
        SET s3_secret_access_key='test';
        SET s3_region='us-east-1';
        """
    )
    try:
        rows = con.execute(
            """
            SELECT
                patient_id,
                COUNT(*) AS total_leituras,
                ROUND(AVG(bpm), 1) AS bpm_medio,
                ROUND(AVG(spo2), 1) AS spo2_medio
            FROM read_json_auto('s3://clinsense-raw-lake/**/*.json', hive_partitioning=true)
            GROUP BY patient_id
            ORDER BY bpm_medio DESC
            LIMIT 15
            """
        ).fetchall()
        return [
            {
                "patient_id": r[0],
                "total_leituras": r[1],
                "bpm_medio": r[2],
                "spo2_medio": r[3],
            }
            for r in rows
        ]
    except Exception:
        return []