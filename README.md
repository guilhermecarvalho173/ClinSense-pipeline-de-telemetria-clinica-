# ClinSense Fisio Center — Plataforma IoT & Big Data de Telemonitoramento

Plataforma de telemonitoramento biomédico contínuo para **40 pacientes crônicos** (pulseiras vestíveis ESP32-S3), construída sobre arquitetura orientada a eventos com ingestão segura **MQTTS (mTLS X.509)** e processamento paralelo em camadas quente e fria (**Caminho B**: Eclipse Mosquitto + LocalStack 3.8 + Terraform + DuckDB).

---

## 1. Visão Geral da Arquitetura (Caminho B)

A solução adota o padrão **Arquitetura Lambda** em execução local containerizada (sem custo de nuvem e sem dependência de MinIO ou clusters Kafka/Spark), dividindo o fluxo de telemetria em duas pipelines paralelas após a recepção segura no *broker* MQTT:

```text
[40 Pulseiras ESP32-S3]
         │
         ▼  MQTTS / mTLS (Porta 8883 - X.509 Client Cert)
┌─────────────────────────┐
│   Eclipse Mosquitto     │
│  (clinsense-mosquitto)  │
└────────┬────────────────┘
         │  MQTT Interno (Porta 1883)
         ▼
┌─────────────────────────┐
│  MQTT Parallel Bridge   │  (ThreadPoolExecutor - 16 workers)
└───┬─────────────────┬───┘
    │                 │
    │ Pipeline 1      │ Pipeline 2
    │ (Camada Fria)   │ (Camada Quente & Alerta)
    ▼                 ▼
[Data Firehose]   [AWS Lambda: clinsense-detector]
    │                 ├──► [DynamoDB: clinsense_quente] (TTL 24h)
    ▼                 └──► Se BPM > 120, BPM < 50 ou SpO2 < 92%:
[S3: clinsense-raw-lake]       ├──► [SNS: clinsense-alertas] ──► [SQS Audit]
(JSON cru particionado         └──► [DynamoDB: clinsense_alertas] (TTL 30d)
 year/month/day/hour)
    │
    ▼
[DuckDB SQL] ──► Auditoria Analítica Zero-ETL (audit_bc_analytics.py)
```

### Componentes Principais

| Camada | Tecnologia / Serviço | Papel na Arquitetura |
| :--- | :--- | :--- |
| **Borda & Segurança** | Eclipse Mosquitto 2.0 + X.509 mTLS | Autentica dispositivos na porta `8883` exigindo certificado cliente assinado pela CA raiz (`ClinSense-Root-CA`) e expõe listener interno na porta `1883`. |
| **Ingestão Paralela** | Python (`paho-mqtt` + `boto3`) | Consome `clinsense/telemetry/#` e despacha simultaneamente cada leitura para o Firehose (camada fria) e para a Lambda (camada quente). |
| **Camada Quente & Alertas** | AWS Lambda + DynamoDB + SNS/SQS | Persiste leituras em `clinsense_quente` (TTL 24h) e avalia regras clínicas ($BPM > 120$, $BPM < 50$ ou $SpO_2 < 92\%$), disparando alertas no SNS `clinsense-alertas` e gravando em `clinsense_alertas` (TTL 30 dias). |
| **Camada Fria (Data Lake)** | Amazon Data Firehose + Amazon S3 | Grava registros em JSON cru (NDJSON) no bucket `clinsense-raw-lake` com particionamento temporal (`year/month/day/hour`) e *buffer* de 1 MB ou 5 segundos. |
| **Analytics & Auditoria** | DuckDB | Executa consultas SQL diretamente sobre os objetos JSON particionados do S3 (`read_json_auto(..., hive_partitioning=true)`), sem necessidade de jobs ETL intermediários para Parquet. |
| **Infraestrutura como Código** | Terraform (`terraform/main.tf`) | Provisiona declarativamente no LocalStack recursos auditáveis (`clinsense-tf-raw-lake`, `clinsense_tf_quente` com TTL e `clinsense-tf-alertas`). |

---

## 2. Estrutura do Repositório

```
clinsense-iot-bigdata/
├── certs/                         # Certificados e chaves X.509 gerados localmente (CA, Broker e Frota)
│   ├── ca.crt / ca.key            # Autoridade Certificadora raiz (ClinSense-Root-CA)
│   ├── broker.crt / broker.key    # Certificado TLS do broker Mosquitto (SAN localhost / 127.0.0.1)
│   └── shared-device.crt / .key   # Certificado X.509 compartilhado pelas 40 pulseiras ESP32-S3
├── docs/
│   └── ADR-001-pipeline.md        # Decisão arquitetural: Caminho B + JSON cru vs. Kafka + Parquet
├── lambda/                        # Diretório auxiliar para artefatos e empacotamento Lambda
├── mosquitto/
│   └── mosquitto.conf             # Configuração dual-listener (1883 interno e 8883 mTLS obrigatório)
├── scripts/
│   ├── generate_certs.py          # Gera CA raiz, cert do broker (SAN localhost) e cert compartilhado da frota
│   ├── setup_bc_pipeline.py       # Provisiona S3, DynamoDB (TTL), SNS, SQS, Firehose e Lambda no LocalStack
│   ├── mqtt_parallel_bridge.py    # Bridge multithread que alimenta Firehose e Lambda em paralelo
│   ├── lambda_detector.py         # Função Lambda de ingestão quente e detecção de anomalias clínicas
│   ├── simulate_wristbands.py     # Simulador MQTTS (porta 8883) de 40 pulseiras (3 ciclos = 120 leituras)
│   ├── audit_bc_analytics.py      # Auditoria end-to-end: DuckDB no S3 + scans no DynamoDB + fila SQS/SNS
│   ├── down.ps1                   # Encerramento e limpeza completa de contêineres/volumes (Windows)
│   └── down.sh                    # Encerramento e limpeza completa de contêineres/volumes (Linux/macOS)
├── terraform/
│   └── main.tf                    # IaC declarativa para LocalStack (S3, DynamoDB com TTL e SNS)
├── docker-compose.yml             # Orquestração do Eclipse Mosquitto 2.0 e LocalStack 3.8
├── .env.example                   # Modelo de variáveis de ambiente locais
├── .gitignore                     # Exclusão de chaves (*.key), .venv e arquivos de estado do Terraform
└── README.md                      # Documentação principal do projeto
---
```
## 3. Pré-requisitos e Preparação (Windows PowerShell)

Certifique-se de ter **Docker Desktop** em execução, **Python 3.11+** e **Terraform** instalados.

1. **Liberar execução de scripts locais no PowerShell (necessário para `.venv` e `down.ps1`):**

   ```powershell
   Set-ExecutionPolicy -ExecutionPolicy RemoteSigned -Scope CurrentUser
   

2. **Criar/ativar o ambiente virtual e instalar as dependências Python:**
   ```powershell
   if (-not (Test-Path .\.venv)) { python -m venv .venv }
   .\.venv\Scripts\Activate.ps1
   pip install cryptography boto3 paho-mqtt duckdb pandas
   ```

3. **Instalar o Terraform (caso ainda não esteja instalado no sistema):**
   ```powershell
   if (-not (Get-Command terraform -ErrorAction SilentlyContinue)) {
       winget install -e --id Hashicorp.Terraform
       $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
   }
   terraform -version
   ```

---

## 4. Guia de Execução Passo a Passo

### Etapa 1 — Gerar os Certificados mTLS X.509
```powershell
New-Item -ItemType Directory -Force -Path certs | Out-Null
python scripts/generate_certs.py
```
> Cria 6 arquivos em `certs/`: `ca.crt`, `ca.key`, `broker.crt`, `broker.key`, `shared-device.crt` e `shared-device.key`.

### Etapa 2 — Subir Mosquitto e LocalStack 3.8
```powershell
docker compose up -d
docker compose restart mosquitto
Start-Sleep -Seconds 8
docker ps
```
> Confirme que `clinsense-mosquitto` e `clinsense-localstack` estão com status `Up` e `(healthy)`.

### Etapa 3 — Provisionar a Pipeline no LocalStack
```powershell
python scripts/setup_bc_pipeline.py
```
> Provisiona o bucket `clinsense-raw-lake`, as tabelas `clinsense_quente` (TTL 24h) e `clinsense_alertas` (TTL 30d), o tópico SNS `clinsense-alertas` inscrito na fila SQS de auditoria, o Firehose `clinsense-cold-firehose` e a Lambda `clinsense-detector`.

### Etapa 4 — Iniciar a Bridge Paralela (Terminal 1)
Mantenha este terminal aberto durante a ingestão:
```powershell
python scripts/mqtt_parallel_bridge.py
```

### Etapa 5 — Simular a Frota de 40 Pulseiras via MQTTS (Terminal 2)
Abra um segundo terminal PowerShell na raiz do projeto, ative o `.venv` e execute o simulador:
```powershell
.\.venv\Scripts\Activate.ps1
python scripts/simulate_wristbands.py
```
> Envia 3 ciclos de 40 leituras (120 mensagens no total) via TLS na porta `8883`. O paciente `P991` simula taquicardia e hipoxemia, disparando 3 alertas imediatos no Terminal 1. **Aguarde 6 segundos** após o término para o *flush* do Firehose no S3.

### Etapa 6 — Executar Auditoria Analítica (DuckDB + DynamoDB + SNS)
No **Terminal 2**:
```powershell
python scripts/audit_bc_analytics.py
```
> Valida os 120 registros no Data Lake S3 via SQL no **DuckDB**, os 120 itens com TTL em `clinsense_quente`, os 3 alertas críticos de `P991` em `clinsense_alertas` e as notificações publicadas no SNS.

### Etapa 7 — Validar Infraestrutura como Código (Terraform)
No **Terminal 2**, entre na pasta `terraform/`, aplique os recursos e retorne à raiz:
```powershell
cd .\terraform
terraform init
terraform plan
terraform apply -auto-approve
cd ..
```

### Etapa 8 — Encerramento e Limpeza Total
Encerre a Bridge no **Terminal 1** (`Ctrl + C`) e execute no **Terminal 2**:
```powershell
.\scripts\down.ps1
```

---

## 5. Decisões Arquiteturais e Governança

### Log Imutável para Replay (Arquitetura Lambda vs. Kappa)
Em nossa arquitetura **Lambda**, o **object store S3 (`clinsense-raw-lake`)** alimentado pelo Firehose em JSON cru particionado desempenha o papel de **log imutável e fonte da verdade (*source of truth*)** para reprocessamento (*replay*) histórico. Caso a regra clínica do detector mude amanhã e exija recalcular os últimos 6 meses de telemetria, basta executar um job batch/DuckDB diretamente sobre os objetos imutáveis do S3.

O **DynamoDB Streams sozinho não atende a esse cenário** porque retém alterações por no máximo **24 horas** (além de a tabela `clinsense_quente` expirar registros via TTL em 24h), inviabilizando qualquer *replay* de longo prazo que uma arquitetura Kappa pura exigiria de um log distribuído com retenção estendida.

### Resumo do ADR-001 (`docs/ADR-001-pipeline.md`)
* **Eliminação de *Bill Shock* e Serviços sem *Always Free*:** Substitui Kinesis Data Streams (cobrado por *shard-hour*), Amazon Timestream e clusters Kafka/Spark por serviços *serverless* (`Firehose + S3 + Lambda + DynamoDB com TTL`) validados localmente no LocalStack 3.8.
* **Prevenção de *Vendor Lock-in*:** O uso de MQTT padrão aberto (Eclipse Mosquitto com mTLS X.509) mantém o firmware das pulseiras ESP32-S3 desacoplado do provedor de nuvem (lição aprendida com a descontinuação do Google Cloud IoT Core em 2023).
* **Conformidade LGPD (Art. 6º, 16 e 46):** Criptografia mTLS em trânsito, minimização e expiração automática de dados operacionais via TTL nativo no DynamoDB (24h para telemetria quente e 30 dias para histórico de alertas clínicos).