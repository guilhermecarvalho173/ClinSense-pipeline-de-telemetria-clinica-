import boto3
from botocore.exceptions import ClientError

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"

dynamodb = boto3.client(
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

def create_buckets():
    buckets = ["clinsense-raw-lake", "clinsense-industria-x-curated"]
    for bucket in buckets:
        try:
            s3.create_bucket(Bucket=bucket)
            print(f"[S3] Bucket '{bucket}' criado com sucesso.")
        except ClientError as e:
            print(f"[S3] Aviso em '{bucket}': {e}")

def create_tables():
    # 1. Tabela Hot Path (Telemetria em Tempo Real)
    try:
        dynamodb.create_table(
            TableName="ClinSense_LiveTelemetry",
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
        print("[DynamoDB] Tabela 'ClinSense_LiveTelemetry' criada.")
    except ClientError as e:
        print(f"[DynamoDB] Aviso em 'ClinSense_LiveTelemetry': {e}")

    # 2. Tabela de Cadastro Clínico (Apenas uso interno - não vai para a Indústria X)
    try:
        dynamodb.create_table(
            TableName="ClinSense_PatientRegistry",
            KeySchema=[{"AttributeName": "patient_id", "KeyType": "HASH"}],
            AttributeDefinitions=[{"AttributeName": "patient_id", "AttributeType": "S"}],
            BillingMode="PAY_PER_REQUEST",
        )
        print("[DynamoDB] Tabela 'ClinSense_PatientRegistry' criada.")
    except ClientError as e:
        print(f"[DynamoDB] Aviso em 'ClinSense_PatientRegistry': {e}")

def seed_patients():
    table = boto3.resource(
        "dynamodb",
        endpoint_url=ENDPOINT_URL,
        region_name=REGION,
        aws_access_key_id="test",
        aws_secret_access_key="test",
    ).Table("ClinSense_PatientRegistry")

    # Cadastra os 40 pacientes (P001 a P039 + P991 anômalo em São Caetano do Sul)
    patients = []
    ceps = ["09510-000", "09520-120", "09530-210", "09540-300", "09550-410"]
    for i in range(1, 40):
        pid = f"P{i:03d}"
        patients.append({
            "patient_id": pid,
            "age": 35 + (i % 45),
            "cep": ceps[i % len(ceps)],
            "condition": "Hipertensao" if i % 2 == 0 else "DPOC",
            "location": "Laboratorio" if i <= 10 else "Domiciliar"
        })

    # Paciente anômalo P991
    patients.append({
        "patient_id": "P991",
        "age": 68,
        "cep": "09520-120",
        "condition": "Insuficiencia Cardiaca",
        "location": "Domiciliar"
    })

    with table.batch_writer() as batch:
        for p in patients:
            batch.put_item(Item=p)
    print(f"[DynamoDB] {len(patients)} pacientes registrados com sucesso.")

if __name__ == "__main__":
    create_buckets()
    create_tables()
    seed_patients()