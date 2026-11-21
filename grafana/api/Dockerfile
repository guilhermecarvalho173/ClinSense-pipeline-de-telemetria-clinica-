FROM python:3.11-slim

WORKDIR /app
RUN pip install --no-cache-dir fastapi uvicorn boto3 duckdb

COPY metrics_api.py .

EXPOSE 8000
CMD ["uvicorn", "metrics_api:app", "--host", "0.0.0.0", "--port", "8000"]
