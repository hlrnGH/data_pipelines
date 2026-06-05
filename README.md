# SPOTIFY — Plateforme de streaming musical distribuée — Groupe M

> Formation Data & IA — Master 1 | HETIC 2025/2026

## Architecture

```mermaid
flowchart TD
    subgraph SOURCES["Sources de données"]
        MINIO_RAW["MinIO · labels-raw/*.json"]
        P2P["Simulateur P2P · Python"]
    end

    subgraph BATCH["Batch — Apache Airflow"]
        DAG1["catalog_ingestion\n 02h"]
        DAG2["streaming_events\n toutes les 5 min"]
        DAG3["aggregation\n 04h"]
        DAG4["recommendation\n 05h"]
        DAG5["dlq_reprocessing\n toutes les heures"]
    end

    subgraph STREAMING["Phase 2 — Kafka + Spark"]
        KAFKA["Apache Kafka\n3 brokers KRaft"]
        SPARK["Spark Structured Streaming\ntrends · fraud · enrichment"]
    end

    subgraph STOCKAGE["Stockage"]
        PG[("PostgreSQL 15")]
        REDIS[("Redis 7")]
        MINIO[("MinIO\nparquet · checkpoints")]
    end

    MINIO_RAW --> DAG1 --> PG
    P2P -->|pub/sub| REDIS --> DAG2 --> PG
    PG --> DAG3 --> PG & REDIS
    PG --> DAG4 --> PG & REDIS
    PG --> DAG5 --> PG
    P2P -->|produce| KAFKA --> SPARK
    SPARK --> PG & REDIS & MINIO
```


---

## Démarrage rapide

```bash
git clone https://github.com/hlrnGH/data_pipelines.git
cd data_pipelines
cp .env.example .env
docker compose up -d
```

- Airflow : http://localhost:8080 (admin / admin)
- MinIO : http://localhost:9001 (minioadmin / minioadmin)
- Kafka UI : http://localhost:8090 (Phase 2)

---

## Organisation Git

```
main
├── feat/batch-pipelines    ← Phase 1 (#1 → #10)
├── feat/kafka-streaming    ← Phase 2 (#11 → #20)
└── feat/inter-group        ← Phase 3 (#21 → #25)
```

## Équipe

| Membre            | Tickets     |
| ----------------- | ----------- |
| Nassim (référent) | #1, #2, #10, #12, #15, #17, #18, #19, #20 |
| Rodrigue          | #3, #4, #6, #11, #13  |
| Omar              | #5, #7, #14, #16      |
| Harold            | #8          |
| Jiek              | #9,          |
