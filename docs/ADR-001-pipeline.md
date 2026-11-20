\# ADR-001: Escolha do Caminho B e Pipeline Paralela (Buffer JSON + NoSQL Quente) em vez de Kafka + Parquet



\* \*\*Status:\*\* Aceito

\* \*\*Data:\*\* 2026-11-19

\* \*\*Projeto:\*\* ClinSense Fisio Center — Telemonitoramento IoT \& Big Data (40 pacientes crônicos)



\---



\## 1. Contexto

O sistema ClinSense precisa ingerir telemetria biomédica contínua (frequência cardíaca, $SpO\_2$, temperatura cutânea e passos) proveniente de 40 pulseiras vestíveis ESP32-S3 conectadas via MQTTS (porta 8883), atendendo simultaneamente a dois requisitos operacionais:

1\. \*\*Detecção clínica em tempo real\*\* ($BPM > 120$, $BPM < 50$ ou $SpO\_2 < 92\\%$) com retenção operacional curta (24h para leitura quente e 30 dias para alertas);

2\. \*\*Armazenamento histórico imutável\*\* para auditoria clínica, conformidade com a LGPD (Art. 16 e Art. 46) e consultas analíticas ad-hoc.



Avaliou-se o provisionamento de um cluster Apache Kafka (ou Amazon MSK / Kinesis Data Streams) com jobs de compactação contínua para formato colunar Apache Parquet versus uma arquitetura orientada a eventos com \*\*Caminho B (Mosquitto + Bridge Python + LocalStack S3/Firehose/Lambda/DynamoDB/SNS)\*\* gravando \*\*JSON cru particionado\*\* na camada fria e \*\*NoSQL (DynamoDB com TTL)\*\* na camada quente.



\---



\## 2. Decisão

Adotamos o \*\*Caminho B (execução local containerizada via Eclipse Mosquitto + LocalStack 3.8)\*\* utilizando \*\*duas pipelines paralelas sem broker distribuído intermediário e sem conversão Parquet\*\*:

\* \*\*Pipeline 1 (Fria):\*\* Bridge Python $\\rightarrow$ Amazon Data Firehose $\\rightarrow$ Amazon S3 (`clinsense-raw-lake`) gravando JSON cru (NDJSON) particionado temporalmente (`year/month/day/hour`) e consultado diretamente via DuckDB (`read\_json\_auto`).

\* \*\*Pipeline 2 (Quente/Alerta):\*\* Bridge Python $\\rightarrow$ AWS Lambda (`clinsense-detector`) $\\rightarrow$ DynamoDB (`clinsense\_quente` com TTL de 24h) + disparo de alerta via SNS (`clinsense-alertas`) e persistência em `clinsense\_alertas` (TTL de 30 dias).



\---



\## 3. Consequências e Análise Técnica



\### (i) Custo e Risco Financeiro

\* \*\*Positivo:\*\* O Caminho B elimina 100% o risco de faturamento inesperado em nuvem (\*bill shock\*) durante o desenvolvimento e validação acadêmica. Em produção equivalente na AWS, o modelo \*serverless\* (\*pay-per-request\* no DynamoDB, Firehose por GB ingerido e Lambda por invocação) escala a zero quando não há tráfego, ao contrário de um cluster Kafka/MSK ou EMR/Spark para conversão Parquet, que exige nós computacionais ativos 24/7 com custo fixo elevado para apenas 40 dispositivos.



\### (ii) Ausência de Free Tier em Kinesis Data Streams (KDS) e Amazon Timestream

\* \*\*Positivo:\*\* Tanto o \*\*Amazon Kinesis Data Streams (KDS)\*\* quanto o \*\*Amazon Timestream\*\* não possuem camada gratuita permanente (\*Always Free\*) na AWS — o KDS cobra por \*shard-hour\* provisionado continuamente e o Timestream cobra por volume de ingestão e armazenamento em memória/magnético desde a primeira hora de uso. Ao substituir o KDS pela ingestão direta via Bridge $\\rightarrow$ Firehose/Lambda e substituir o Timestream pelo \*\*DynamoDB com TTL nativo\*\*, evitamos serviços pagos por hora ociosa.



\### (iii) Lock-in Cloud (Lição do Google Cloud IoT Core descontinuado em 2023)

\* \*\*Positivo:\*\* A descontinuação abrupta do \*\*Google Cloud IoT Core em agosto de 2023\*\* demonstrou o risco crítico de acoplar dispositivos embarcados a \*brokers\* IoT proprietários de nuvem. Ao adotar \*\*MQTT padrão aberto (Eclipse Mosquitto com mTLS X.509)\*\* na borda, os firmwares das pulseiras ESP32-S3 permanecem completamente desacoplados do provedor cloud. Caso seja necessário migrar do LocalStack/AWS para outra nuvem ou servidor \*on-premises\* da clínica, altera-se apenas a camada de integração (Bridge), sem regravar o firmware dos 40 dispositivos.



\### (iv) Por que o Caminho B atende apesar das limitações

\* \*\*Limitações aceitas:\*\* O JSON cru ocupa mais espaço em disco que o Parquet colunar e possui menor taxa de compressão para varreduras de bilhões de linhas; além disso, o LocalStack Community não oferece SLA de alta disponibilidade multi-AZ nem \*Dynamic Partitioning\* nativo por campos internos do payload.

\* \*\*Justificativa de adequação:\*\* Para uma frota de 40 pulseiras (gerando dezenas de milhares de leituras/dia, na ordem de poucos megabytes/mês), a leitura direta via \*\*DuckDB (`read\_json\_auto` com partições Hive `year/month/day/hour`)\*\* executa agregações analíticas em milissegundos sem o custo operacional de pipelines ETL/Spark para Parquet, enquanto o DynamoDB garante latência de milissegundos para alertas clínicos críticos e expiração automática de dados pessoais via TTL (LGPD).

