import os
import time
import boto3
import duckdb
import pandas as pd
import streamlit as st

ENDPOINT = os.getenv("AWS_ENDPOINT_URL", "http://localstack:4566")
REGION = os.getenv("AWS_DEFAULT_REGION", "us-east-1")

st.set_page_config(page_title="ClinSense Fisio Center — Monitor IoT", layout="wide")
st.title("ClinSense Fisio Center — Telemonitoramento em Tempo Real (40 Pulseiras)")

dynamodb = boto3.resource(
    "dynamodb",
    endpoint_url=ENDPOINT,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)

def carregar_tabela(nome_tabela: str) -> pd.DataFrame:
    try:
        resp = dynamodb.Table(nome_tabela).scan()
        itens = resp.get("Items", [])
        if not itens:
            return pd.DataFrame()
        df = pd.DataFrame(itens)
        for col in ["bpm", "spo2"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce")
        return df
    except Exception as exc:
        st.warning(f"Aguardando tabela {nome_tabela} no LocalStack... ({exc})")
        return pd.DataFrame()

auto_refresh = st.sidebar.toggle("Atualização automática (2s)", value=True)
st.sidebar.markdown("**Regras Clínicas Ativas:**\n- Taquicardia: `BPM > 120`\n- Bradicardia: `BPM < 50`\n- Hipoxemia: `SpO2 < 92%`")

df_quente = carregar_tabela("clinsense_quente")
df_alertas = carregar_tabela("clinsense_alertas")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Leituras Quentes (TTL 24h)", len(df_quente))
col2.metric("Alertas Clínicos (TTL 30d)", len(df_alertas))
col3.metric(
    "BPM Médio (Frota)",
    f"{df_quente['bpm'].mean():.1f}" if not df_quente.empty and "bpm" in df_quente else "—",
)
col4.metric(
    "SpO2 Médio (Frota)",
    f"{df_quente['spo2'].mean():.1f}%" if not df_quente.empty and "spo2" in df_quente else "—",
)

st.divider()

if not df_alertas.empty:
    st.subheader("Alertas Clínicos Críticos Detectados (Lambda + SNS)")
    st.dataframe(df_alertas.sort_values(by=df_alertas.columns[0], ascending=False), use_container_width=True)
else:
    st.info("Nenhum alerta crítico registrado até o momento. Execute simulate_wristbands.py para testar o paciente P991.")

c_esq, c_dir = st.columns(2)

with c_esq:
    st.subheader("Frequência Cardíaca (BPM) por Paciente")
    if not df_quente.empty and {"patient_id", "bpm"}.issubset(df_quente.columns):
        ultimos = df_quente.groupby("patient_id", as_index=False)["bpm"].last().set_index("patient_id")
        st.bar_chart(ultimos)

with c_dir:
    st.subheader("Saturação de Oxigênio (SpO2 %) por Paciente")
    if not df_quente.empty and {"patient_id", "spo2"}.issubset(df_quente.columns):
        ultimos_spo2 = df_quente.groupby("patient_id", as_index=False)["spo2"].last().set_index("patient_id")
        st.bar_chart(ultimos_spo2)

with st.expander("Auditoria Camada Fria — Data Lake S3 via DuckDB"):
    if st.button("Consultar S3 (clinsense-raw-lake) agora"):
        try:
            host_s3 = ENDPOINT.replace("http://", "").replace("https://", "")
            con = duckdb.connect()
            con.execute("INSTALL httpfs; LOAD httpfs;")
            con.execute(f"""
                SET s3_endpoint='{host_s3}';
                SET s3_access_key_id='test';
                SET s3_secret_access_key='test';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            res = con.execute("""
                SELECT patient_id, COUNT(*) AS leituras, ROUND(AVG(bpm), 1) AS bpm_medio, ROUND(AVG(spo2), 1) AS spo2_medio
                FROM read_json_auto('s3://clinsense-raw-lake/**/*.json', hive_partitioning=true)
                GROUP BY patient_id
                ORDER BY bpm_medio DESC
            """).df()
            st.dataframe(res, use_container_width=True)
        except Exception as e:
            st.error(f"Erro ao consultar S3 via DuckDB (aguarde o flush de 5s do Firehose): {e}")

if auto_refresh:
    time.sleep(2)
    st.rerun()