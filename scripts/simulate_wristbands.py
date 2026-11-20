import json
import random
import ssl
import time
from datetime import datetime, timezone
import paho.mqtt.client as mqtt

BROKER = "127.0.0.1"
PORT = 8883

CA_CERT = "certs/ca.crt"
SHARED_CERT = "certs/shared-device.crt"
SHARED_KEY = "certs/shared-device.key"

patient_ids = [f"P{i:03d}" for i in range(1, 40)] + ["P991"]

def generate_reading(pid: str) -> dict:
    is_anomaly = False
    if pid == "P991":
        is_anomaly = True
        hr = random.randint(132, 165)
        spo2 = round(random.uniform(86.0, 91.5), 1)
        temp = round(random.uniform(37.6, 38.4), 1)
        steps = random.randint(0, 5)
    else:
        hr = random.randint(62, 95)
        spo2 = round(random.uniform(95.0, 99.5), 1)
        temp = round(random.uniform(36.1, 37.1), 1)
        steps = random.randint(0, 45)

    return {
        "patient_id": pid,
        "device_model": "ClinSense-ESP32S3-v2",
        "auth_cert_cn": "clinsense-esp32-shared-cert",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "heart_rate": hr,
        "spo2": spo2,
        "skin_temp_c": temp,
        "steps_min": steps,
        "anomaly": is_anomaly,
    }

if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="clinsense-esp32-fleet-tls")
    client.tls_set(
        ca_certs=CA_CERT,
        certfile=SHARED_CERT,
        keyfile=SHARED_KEY,
        tls_version=ssl.PROTOCOL_TLS_CLIENT,
    )

    print(f"[MQTTS - B.b] Conectando via TLS na porta {PORT} com certificado compartilhado ({SHARED_CERT})...")
    client.connect(BROKER, PORT, 60)
    client.loop_start()

    try:
        for cycle in range(1, 4):
            for pid in patient_ids:
                data = generate_reading(pid)
                topic = f"clinsense/telemetry/{pid}"
                client.publish(topic, json.dumps(data), qos=1)
            print(f"[MQTTS - B.b] Ciclo {cycle}/3 publicado via TLS (40 leituras enviadas).")
            time.sleep(2)
    finally:
        client.loop_stop()
        client.disconnect()
        print("[MQTTS - B.b] Transmissão TLS concluída.")