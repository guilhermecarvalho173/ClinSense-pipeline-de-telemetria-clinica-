import json
from concurrent.futures import ThreadPoolExecutor
import boto3
import paho.mqtt.client as mqtt

ENDPOINT_URL = "http://127.0.0.1:4566"
REGION = "us-east-1"
CREDS = {"aws_access_key_id": "test", "aws_secret_access_key": "test"}

firehose = boto3.client("firehose", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
awslambda = boto3.client("lambda", endpoint_url=ENDPOINT_URL, region_name=REGION, **CREDS)
executor = ThreadPoolExecutor(max_workers=16)

def pipeline_1_cold_firehose(raw_json_str: str):
    try:
        firehose.put_record(
            DeliveryStreamName="clinsense-cold-firehose",
            Record={"Data": (raw_json_str + "\n").encode("utf-8")},
        )
    except Exception as e:
        print(f"[ERRO FIREHOSE] {e}")

def pipeline_2_hot_lambda(raw_json_str: str):
    try:
        resp = awslambda.invoke(
            FunctionName="clinsense-detector",
            InvocationType="RequestResponse",
            Payload=raw_json_str.encode("utf-8"),
        )
        raw_res = resp["Payload"].read().decode("utf-8")
        result = json.loads(raw_res)
        if isinstance(result, dict) and result.get("status") == "ALERT_TRIGGERED":
            print(f"[PIPELINE 2 - QUENTE/ALERTA] {result['patient_id']} -> {result['reasons']}")
        elif isinstance(result, dict) and "errorMessage" in result:
            print(f"[ERRO LAMBDA] {result['errorMessage']}")
    except Exception as e:
        print(f"[ERRO INVOKE LAMBDA] {e}")

def on_connect(client, userdata, flags, rc, properties=None):
    print(f"[BRIDGE B.c] Conectada ao Mosquitto (rc={rc}). Inscrevendo em clinsense/telemetry/#...")
    client.subscribe("clinsense/telemetry/#", qos=1)

def on_message(client, userdata, msg):
    raw_json_str = msg.payload.decode("utf-8")
    executor.submit(pipeline_1_cold_firehose, raw_json_str)
    executor.submit(pipeline_2_hot_lambda, raw_json_str)

if __name__ == "__main__":
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="clinsense-bc-parallel-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("127.0.0.1", 1883, 60)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        executor.shutdown(wait=True)
        print("\n[BRIDGE B.c] Encerrada.")