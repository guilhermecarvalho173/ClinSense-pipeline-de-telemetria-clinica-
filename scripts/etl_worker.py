import json
import hashlib
import io
from datetime import datetime, timezone
from decimal import Decimal
import boto3
import pandas as pd
import paho.mqtt.client as mqtt

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
LGPD_SALT = "ClinSense_SCS_2026_SecretSalt"
BATCH_SIZE = 20

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

live_table = dynamodb.Table("ClinSense_LiveTelemetry")
registry_table = dynamodb.Table("ClinSense_PatientRegistry")

# Cache local do cadastro para enriquecimento e generalização
patient_cache = {}
curated_buffer = []

def load_registry():
    resp = registry_table.scan()
    for item in resp.get("Items", []):
        patient_cache[item["patient_id"]] = item
    print(f"[ETL] Cadastro carregado: {len(patient_cache)} pacientes em memória.")

def pseudonymize_id(patient_id: str) -> str:
    raw = f"{LGPD_SALT}:{patient_id}".encode("utf-8")
    return "ANON_" + hashlib.sha256(raw).hexdigest()[:16]

def generalize_age(age: int) -> str:
    decade = (int(age) // 10) * 10
    return f"{decade}-{decade + 9}"

def generalize_cep(cep: str) -> str:
    # Mantém apenas os 3 primeiros dígitos (região de São Caetano do Sul: 095xx)
    return f"{str(cep)[:3]}xx-xxx"

def flush_curated_batch():
    global curated_buffer
    if not curated_buffer:
        return

    df = pd.DataFrame(curated_buffer)
    parquet_buf = io.BytesIO()
    df.to_parquet(parquet_buf, index=False, engine="pyarrow")
    parquet_buf.seek(0)

    ts_key = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S_%f")
    s3_key = f"telemetry/date={datetime.now(timezone.utc).strftime('%Y-%m-%d')}/batch_{ts_key}.parquet"

    s3.put_object(
        Bucket="clinsense-industria-x-curated",
        Key=s3_key,
        Body=parquet_buf.getvalue(),
    )
    print(f"[S3 Curated - LGPD] Lote Parquet salvo ({len(curated_buffer)} registros): {s3_key}")
    curated_buffer = []

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[MQTT] Worker ETL conectado ao broker (código {rc}). Inscrito em 'clinsense/telemetry/#'")
    client.subscribe("clinsense/telemetry/#")

def on_message(client, userdata, msg):
    payload = json.loads(msg.payload.decode("utf-8"))
    pid = payload["patient_id"]
    ts = payload["timestamp"]

    # 1. Hot Path (Camada Quente identificada para equipe clínica)
    dynamo_item = json.loads(json.dumps(payload), parse_float=Decimal)
    live_table.put_item(Item=dynamo_item)

    if payload.get("anomaly"):
        print(f"[ALERTA CLÍNICO - HOT PATH] Paciente {pid} | FC={payload['heart_rate']} bpm | SpO2={payload['spo2']}%")

    # 2. Raw Lake (Backup interno bruto)
    raw_key = f"raw/{pid}/{ts.replace(':', '-')}.json"
    s3.put_object(
        Bucket="clinsense-raw-lake",
        Key=raw_key,
        Body=json.dumps(payload).encode("utf-8"),
    )

    # 3. Curated Lake (Anonimização LGPD para Indústria X)
    reg = patient_cache.get(pid, {"age": 50, "cep": "09500-000", "condition": "Desconhecida", "location": "Domiciliar"})
    curated_record = {
        "anon_patient_hash": pseudonymize_id(pid),
        "age_bracket": generalize_age(reg["age"]),
        "cep_prefix": generalize_cep(reg["cep"]),
        "condition": reg["condition"],
        "monitoring_env": reg["location"],
        "timestamp": ts,
        "heart_rate": int(payload["heart_rate"]),
        "spo2": float(payload["spo2"]),
        "skin_temp_c": float(payload["skin_temp_c"]),
        "steps_min": int(payload["steps_min"]),
        "anomaly_flag": bool(payload.get("anomaly", False)),
    }
    curated_buffer.append(curated_record)

    if len(curated_buffer) >= BATCH_SIZE:
        flush_curated_batch()

if __name__ == "__main__":
    load_registry()
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="clinsense-etl-worker")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883, 60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        flush_curated_batch()
        print("\n[ETL] Worker encerrado com segurança.")