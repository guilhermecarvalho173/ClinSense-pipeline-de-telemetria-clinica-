import io
import json
import time
from datetime import datetime, timezone
from decimal import Decimal
import boto3
import pandas as pd
import paho.mqtt.client as mqtt

# Configurações MQTT
MQTT_BROKER = "localhost"
MQTT_PORT = 1883
MQTT_TOPIC = "clinsense/telemetry/#"

# Configurações AWS / LocalStack
ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
DYNAMO_TABLE = "ClinSense_LiveTelemetry"
RAW_BUCKET = "clinsense-raw-lake"

# Configurações de Micro-batch (Cold Path)
BATCH_SIZE = 40          # 1 ciclo completo das 40 pulseiras
FLUSH_INTERVAL_SEC = 5   # Tempo máximo antes de descarregar lote parcial

# Clientes AWS
dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)
live_table = dynamodb.Table(DYNAMO_TABLE)

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

# Buffer em memória para o Data Lake
parquet_buffer = []
last_flush_time = time.time()


def write_hot_path(payload: dict) -> None:
    """Grava leitura individual no DynamoDB em tempo real e alerta anomalias."""
    item = {
        "patient_id": str(payload["patient_id"]),
        "timestamp": str(payload["timestamp"]),
        "device_model": str(payload.get("device_model", "ClinSense-ESP32S3-v2")),
        "heart_rate": int(payload["heart_rate"]),
        "spo2": Decimal(str(payload["spo2"])),
        "skin_temp_c": Decimal(str(payload["skin_temp_c"])),
        "steps_min": int(payload["steps_min"]),
        "anomaly": bool(payload.get("anomaly", False)),
    }
    live_table.put_item(Item=item)

    if item["anomaly"]:
        print(
            f"[ALERTA CLÍNICO - HOT PATH] Paciente {item['patient_id']} | "
            f"FC: {item['heart_rate']} bpm | SpO2: {item['spo2']}% | "
            f"Temp: {item['skin_temp_c']}°C"
        )


def flush_cold_path() -> None:
    """Converte o buffer acumulado para Parquet e envia ao S3 Raw Lake."""
    global parquet_buffer, last_flush_time
    if not parquet_buffer:
        return

    batch = parquet_buffer.copy()
    parquet_buffer.clear()
    last_flush_time = time.time()

    df = pd.DataFrame(batch)
    now = datetime.now(timezone.utc)
    s3_key = (
        f"telemetry/year={now.year}/month={now.month:02d}/day={now.day:02d}/"
        f"clinsense_batch_{now.strftime('%H%M%S_%f')}.parquet"
    )

    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False, engine="pyarrow", compression="snappy")
    out_buffer.seek(0)

    s3.put_object(
        Bucket=RAW_BUCKET,
        Key=s3_key,
        Body=out_buffer.getvalue(),
        ContentType="application/octet-stream",
    )
    print(
        f"[S3 COLD PATH] Lote de {len(batch)} registros salvo em "
        f"s3://{RAW_BUCKET}/{s3_key}"
    )


def on_connect(client, userdata, flags, reason_code, properties=None):
    if reason_code == 0:
        print(f"[INGESTOR] Conectado ao broker MQTT ({MQTT_BROKER}:{MQTT_PORT}).")
        client.subscribe(MQTT_TOPIC, qos=1)
        print(f"[INGESTOR] Assinando tópico '{MQTT_TOPIC}'...")
    else:
        print(f"[INGESTOR] Falha na conexão MQTT. Código: {reason_code}")


def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode("utf-8"))

        # 1. Via Rápida (Hot Path -> DynamoDB)
        write_hot_path(payload)

        # 2. Via Analítica (Cold Path -> Buffer Parquet -> S3)
        parquet_buffer.append(payload)
        if len(parquet_buffer) >= BATCH_SIZE:
            flush_cold_path()

    except Exception as e:
        print(f"[ERRO INGESTÃO] Falha ao processar mensagem: {e}")


if __name__ == "__main__":
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id="clinsense-cloud-ingestor",
    )
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    print("[INGESTOR] Worker ativo. Aguardando telemetria das pulseiras (Ctrl+C para encerrar)...")
    try:
        while True:
            time.sleep(1)
            # Garante que lotes menores que BATCH_SIZE também sejam enviados ao S3
            if parquet_buffer and (time.time() - last_flush_time >= FLUSH_INTERVAL_SEC):
                flush_cold_path()
    except KeyboardInterrupt:
        print("\n[INGESTOR] Encerrando worker e descarregando buffer restante...")
        flush_cold_path()
    finally:
        client.loop_stop()
        client.disconnect()
        print("[INGESTOR] Worker finalizado com segurança.")