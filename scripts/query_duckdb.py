import os
import tempfile
import boto3
import duckdb

ENDPOINT_URL = "http://localhost:4566"
REGION = "us-east-1"
RAW_BUCKET = "clinsense-raw-lake"

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT_URL,
    region_name=REGION,
    aws_access_key_id="test",
    aws_secret_access_key="test",
)


def is_parquet_file(filepath: str) -> bool:
    """Verifica os magic bytes 'PAR1' no início do arquivo."""
    with open(filepath, "rb") as f:
        return f.read(4) == b"PAR1"


def main():
    resp = s3.list_objects_v2(Bucket=RAW_BUCKET, Prefix="telemetry/")
    objs = [o for o in resp.get("Contents", []) if o.get("Size", 0) > 0]

    if not objs:
        print(f"[AVISO] Nenhum arquivo encontrado em s3://{RAW_BUCKET}/telemetry/.")
        print("Certifique-se de ter rodado setup_aws.py, o ingestor e simulate_wristbands.py.")
        return

    tmp_dir = os.path.abspath(tempfile.mkdtemp())
    local_files = []

    for idx, o in enumerate(objs):
        local_path = os.path.join(tmp_dir, f"batch_{idx}.dat")
        s3.download_file(RAW_BUCKET, o["Key"], local_path)
        local_files.append(local_path.replace("\\", "/"))

    print(f"[DUCKDB] {len(local_files)} arquivo(s) baixado(s) do S3 (ex: {objs[0]['Key']}).\n")

    con = duckdb.connect()
    if is_parquet_file(local_files[0]):
        con.execute(f"CREATE VIEW clinsense AS SELECT *, heart_rate AS bpm FROM read_parquet({local_files})")
    else:
        con.execute(f"CREATE VIEW clinsense AS SELECT *, heart_rate AS bpm FROM read_json_auto({local_files})")

    print("duckdb> SELECT patient_id, AVG(bpm) FROM clinsense GROUP BY patient_id;\n")
    query = """
        SELECT
            patient_id,
            ROUND(AVG(bpm), 1) AS "AVG(bpm)",
            ROUND(AVG(spo2), 1) AS "AVG(spo2)",
            COUNT(*) AS leituras
        FROM clinsense
        GROUP BY patient_id
        ORDER BY "AVG(bpm)" DESC
        LIMIT 10
    """
    df = con.execute(query).df()
    print(df.to_string(index=False))


if __name__ == "__main__":
    main()