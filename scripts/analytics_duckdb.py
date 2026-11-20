import io
import boto3
import duckdb
import pandas as pd

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
BUCKET_CURATED = "clinsense-industria-x-curated"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

def load_curated_parquet_from_s3() -> pd.DataFrame:
    resp = s3.list_objects_v2(Bucket=BUCKET_CURATED)
    objects = resp.get("Contents", [])
    if not objects:
        raise RuntimeError("Nenhum arquivo Parquet encontrado no bucket curado.")

    print(f"[S3 -> DuckDB] Lendo {len(objects)} arquivos Parquet do bucket '{BUCKET_CURATED}'...")
    frames = []
    for obj in objects:
        key = obj["Key"]
        if key.endswith(".parquet"):
            body = s3.get_object(Bucket=BUCKET_CURATED, Key=key)["Body"].read()
            frames.append(pd.read_parquet(io.BytesIO(body)))

    return pd.concat(frames, ignore_index=True)

if __name__ == "__main__":
    curated_df = load_curated_parquet_from_s3()
    con = duckdb.connect(database=":memory:")
    con.register("telemetry_curated", curated_df)

    print("\n=== 1. AUDITORIA LGPD: AMOSTRA DO SCHEMA ANONIMIZADO (INDÚSTRIA X) ===")
    sample = con.execute("""
        SELECT anon_patient_hash, age_bracket, cep_prefix, condition, heart_rate, spo2, anomaly_flag
        FROM telemetry_curated
        LIMIT 5
    """).df()
    print(sample.to_string(index=False))

    print("\n=== 2. AGREGAÇÃO BI POR CONDIÇÃO CLÍNICA E FAIXA ETÁRIA ===")
    bi_summary = con.execute("""
        SELECT
            condition AS condicao_clinica,
            age_bracket AS faixa_etaria,
            cep_prefix AS regiao_cep,
            COUNT(*) AS total_leituras,
            ROUND(AVG(heart_rate), 1) AS fc_media_bpm,
            ROUND(AVG(spo2), 1) AS spo2_medio_pct,
            SUM(CASE WHEN anomaly_flag THEN 1 ELSE 0 END) AS eventos_anomalos
        FROM telemetry_curated
        GROUP BY condition, age_bracket, cep_prefix
        ORDER BY eventos_anomalos DESC, fc_media_bpm DESC
    """).df()
    print(bi_summary.to_string(index=False))

    print("\n=== 3. ISOLAMENTO DE ANOMALIAS (SEM EXPOR O ID REAL P991) ===")
    anomalies = con.execute("""
        SELECT
            anon_patient_hash,
            age_bracket,
            cep_prefix,
            condition,
            COUNT(*) AS alertas,
            MAX(heart_rate) AS pico_fc_bpm,
            MIN(spo2) AS minimo_spo2_pct
        FROM telemetry_curated
        WHERE anomaly_flag = TRUE
        GROUP BY anon_patient_hash, age_bracket, cep_prefix, condition
    """).df()
    print(anomalies.to_string(index=False))