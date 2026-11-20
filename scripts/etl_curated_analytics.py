import hashlib
import io
from datetime import datetime, timezone
import boto3
import duckdb
import pandas as pd

# Configurações AWS / LocalStack
ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
RAW_BUCKET = "clinsense-raw-lake"
CURATED_BUCKET = "clinsense-industria-x-curated"
REGISTRY_TABLE = "ClinSense_PatientRegistry"
LGPD_SALT = "clinsense-lgpd-secret-salt-2026"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


def extract_raw_lake() -> pd.DataFrame:
    """Lê todos os arquivos Parquet do bucket Raw Lake."""
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="telemetry/")
    objects = resp.get("Contents", [])
    if not objects:
        raise RuntimeError(f"Nenhum arquivo Parquet encontrado em s3://{RAW_BUCKET}/telemetry/")

    dfs = []
    for obj in objects:
        body = s3.get_object(Bucket=RAW_BUCKET, Key=obj["Key"])["Body"].read()
        dfs.append(pd.read_parquet(io.BytesIO(body)))

    df_raw = pd.concat(dfs, ignore_index=True)
    print(f"[ETL - EXTRACT] {len(df_raw)} leituras carregadas de {len(objects)} arquivos Parquet.")
    return df_raw


def extract_patient_registry() -> pd.DataFrame:
    """Carrega o cadastro clínico interno do DynamoDB (uso exclusivo interno)."""
    table = dynamodb.Table(REGISTRY_TABLE)
    items = table.scan().get("Items", [])
    df_reg = pd.DataFrame(items)
    df_reg["age"] = df_reg["age"].astype(int)
    print(f"[ETL - REGISTRY] {len(df_reg)} cadastros clínicos carregados para enriquecimento.")
    return df_reg


def anonymize_lgpd(df_raw: pd.DataFrame, df_reg: pd.DataFrame) -> pd.DataFrame:
    """Cruza telemetria + cadastro e aplica anonimização LGPD para a Indústria X."""
    df = df_raw.merge(df_reg, on="patient_id", how="inner")

    # 1. Pseudonimização irreversível do patient_id
    df["patient_hash"] = df["patient_id"].apply(
        lambda pid: hashlib.sha256(f"{LGPD_SALT}:{pid}".encode()).hexdigest()[:16]
    )

    # 2. Generalização de idade em faixas etárias (k-anonimato)
    df["age_group"] = pd.cut(
        df["age"],
        bins=[0, 44, 59, 120],
        labels=["30-44", "45-59", "60+"],
    ).astype(str)

    # 3. Generalização geográfica (mantém apenas os 3 primeiros dígitos do CEP)
    df["cep_region"] = df["cep"].str[:3] + "xx-xxx"

    # 4. Janela temporal arredondada por hora
    df["hour_window"] = pd.to_datetime(df["timestamp"]).dt.floor("h").astype(str)

    # 5. Seleção estrita de colunas permitidas (minimização de dados - LGPD Art. 6º)
    curated_cols = [
        "patient_hash",
        "age_group",
        "cep_region",
        "condition",
        "location",
        "hour_window",
        "heart_rate",
        "spo2",
        "skin_temp_c",
        "steps_min",
        "anomaly",
    ]
    df_curated = df[curated_cols].copy()
    print(f"[ETL - LGPD] Anonimização concluída. Colunas PII removidas ({len(df_curated)} linhas).")
    return df_curated


def load_curated_s3(df_curated: pd.DataFrame) -> str:
    """Grava o dataset curado em Parquet no bucket da Indústria X."""
    now = datetime.now(timezone.utc)
    s3_key = f"industria_x/year={now.year}/month={now.month:02d}/day={now.day:02d}/curated_vitals.parquet"

    buf = io.BytesIO()
    df_curated.to_parquet(buf, index=False, engine="pyarrow", compression="snappy")
    buf.seek(0)

    s3.put_object(
        Bucket=CURATED_BUCKET,
        Key=s3_key,
        Body=buf.getvalue(),
        ContentType="application/octet-stream",
    )
    print(f"[ETL - LOAD] Dataset curado salvo em s3://{CURATED_BUCKET}/{s3_key}\n")
    return s3_key


def run_duckdb_analytics(df_curated: pd.DataFrame) -> None:
    """Executa consultas SQL analíticas via DuckDB sobre os dados curados."""
    con = duckdb.connect(database=":memory:")
    con.register("curated_vitals", df_curated)

    print("=== RELATÓRIO ANALÍTICO DUCKDB (INDÚSTRIA X - DADOS ANONIMIZADOS) ===")
    summary_sql = """
        SELECT
            condition AS condicao_clinica,
            age_group AS faixa_etaria,
            COUNT(*) AS total_leituras,
            ROUND(AVG(heart_rate), 1) AS fc_media_bpm,
            ROUND(AVG(spo2), 1) AS spo2_medio_pct,
            SUM(CASE WHEN anomaly THEN 1 ELSE 0 END) AS eventos_criticos
        FROM curated_vitals
        GROUP BY condition, age_group
        ORDER BY eventos_criticos DESC, condicao_clinica, faixa_etaria
    """
    print(con.execute(summary_sql).df().to_string(index=False))

    print("\n=== DETECÇÃO DE COORTES ANÔMALAS (SEM EXPOR IDENTIDADE DO PACIENTE) ===")
    anomaly_sql = """
        SELECT
            patient_hash,
            condition,
            age_group,
            cep_region,
            COUNT(*) AS alertas,
            MAX(heart_rate) AS pico_fc_bpm,
            MIN(spo2) AS menor_spo2_pct
        FROM curated_vitals
        WHERE anomaly = TRUE
        GROUP BY patient_hash, condition, age_group, cep_region
    """
    print(con.execute(anomaly_sql).df().to_string(index=False))


if __name__ == "__main__":
    raw_df = extract_raw_lake()
    reg_df = extract_patient_registry()
    curated_df = anonymize_lgpd(raw_df, reg_df)
    load_curated_s3(curated_df)
    run_duckdb_analytics(curated_df)