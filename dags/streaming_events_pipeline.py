"""
DAG : streaming_events_pipeline
=================================
Consomme les événements d'écoute depuis Redis, les valide,
les enrichit avec le catalogue PostgreSQL et les stocke en dual :
Parquet sur MinIO + table listening_events dans PostgreSQL.
"""

import json
import os
import tempfile
import time
from datetime import datetime, timedelta
from typing import Any
from uuid import UUID

import boto3
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import redis
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json


DAG_DOC = """
## streaming_events_pipeline

### Rôle
Consomme en micro-batch les événements du simulateur P2P depuis Redis,
les valide, les enrichit avec le catalogue PostgreSQL, puis les stocke
dans MinIO au format Parquet et dans PostgreSQL.

### Sources
- Redis channel `listening_events`
- Redis channel `p2p_network_events`
- Redis LIST fallback : `listening_events`, `p2p_network_events`,
  `listening_events_buffer`, `p2p_network_events_buffer`

### Destinations
- Table `listening_events`
- Fichiers Parquet dans `s3://spotify-parquet/listening_events/date=.../hour=.../`
- Table `dead_letter_events` pour les événements invalides

### Idempotence
Chaque event est identifié par `event_id`. L'insertion PostgreSQL utilise
`ON CONFLICT (id) DO NOTHING` pour éviter les doublons.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=10),
}

POSTGRES_CONN_ID = "spotify_postgres"
REDIS_CHANNELS = ["listening_events", "p2p_network_events"]
BATCH_WINDOW_SEC = 300

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/1")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET_PARQUET = os.getenv("MINIO_BUCKET_PARQUET", "spotify-parquet")


def _parse_timestamp(value: str) -> datetime:
    if not value:
        raise ValueError("timestamp manquant")

    cleaned = str(value).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(cleaned)

    if parsed.tzinfo is not None:
        parsed = parsed.replace(tzinfo=None)

    return parsed


def _is_uuid(value: Any) -> bool:
    try:
        UUID(str(value))
        return True
    except Exception:
        return False


def _insert_dlq(
    payload: dict,
    error_type: str,
    error_message: str,
    original_topic: str | None = None,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    sql = """
        INSERT INTO dead_letter_events (
            original_topic,
            payload,
            error_type,
            error_message
        )
        VALUES (%s, %s, %s, %s)
    """

    hook.run(
        sql,
        parameters=(
            original_topic,
            Json(payload),
            error_type,
            error_message,
        ),
    )


with DAG(
    dag_id="streaming_events_pipeline",
    default_args=DEFAULT_ARGS,
    description="Micro-batch : Redis → validation → enrichissement → MinIO + PostgreSQL",
    schedule_interval="*/5 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "events", "streaming"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_from_redis")
    def consume_from_redis(**context) -> dict:
        """
        Consomme les événements Redis.

        Le simulateur Phase 1 publie en pub/sub. Comme Redis pub/sub n'est
        pas persistant, cette tâche écoute les channels pendant une courte
        fenêtre. Elle tente aussi de lire des Redis LIST si le simulateur
        les alimente.
        """
        client = redis.from_url(REDIS_URL, decode_responses=True)

        result = {
            "listening": [],
            "p2p_network": [],
        }

        list_keys = {
            "listening_events": "listening",
            "p2p_network_events": "p2p_network",
            "listening_events_buffer": "listening",
            "p2p_network_events_buffer": "p2p_network",
        }

        for redis_key, output_key in list_keys.items():
            try:
                while True:
                    payload = client.rpop(redis_key)
                    if payload is None:
                        break
                    result[output_key].append(json.loads(payload))
            except Exception as exc:
                print(f"Warning lecture Redis LIST {redis_key}: {exc}")

        listen_seconds = int(os.getenv("REDIS_LISTEN_SECONDS", "10"))
        pubsub = client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(*REDIS_CHANNELS)

        start = time.time()
        try:
            while time.time() - start < listen_seconds:
                message = pubsub.get_message(timeout=1)
                if not message:
                    continue

                channel = message.get("channel")
                data = message.get("data")

                if not data:
                    continue

                event = json.loads(data)

                if channel == "listening_events":
                    result["listening"].append(event)
                elif channel == "p2p_network_events":
                    result["p2p_network"].append(event)

        finally:
            pubsub.close()

        print(
            f"Events consommés — listening={len(result['listening'])}, "
            f"p2p_network={len(result['p2p_network'])}"
        )

        return result

    @task(task_id="validate_events")
    def validate_events(raw_events: dict, **context) -> dict:
        """
        Valide les événements et envoie les invalides en DLQ.
        """
        valid_listening = []
        valid_p2p = []
        errors = 0

        required_listening = [
            "event_id",
            "user_id",
            "track_id",
            "timestamp",
            "duration_ms",
        ]

        for event in raw_events.get("listening", []):
            try:
                missing = [field for field in required_listening if field not in event]
                if missing:
                    raise ValueError(f"Champs manquants: {missing}")

                if not _is_uuid(event["event_id"]):
                    raise ValueError("event_id invalide")

                if not _is_uuid(event["user_id"]):
                    raise ValueError("user_id invalide")

                if not _is_uuid(event["track_id"]):
                    raise ValueError("track_id invalide")

                parsed_ts = _parse_timestamp(event["timestamp"])
                duration_ms = int(event["duration_ms"])

                if duration_ms <= 0:
                    raise ValueError("duration_ms doit être positif")

                normalized = {
                    **event,
                    "timestamp": parsed_ts.isoformat(),
                    "duration_ms": duration_ms,
                    "completed": bool(event.get("completed", False)),
                    "device_type": event.get("device_type"),
                    "geo_country": event.get("geo_country"),
                    "event_source": event.get("event_source", "p2p"),
                    "source_peer": event.get("source_peer") or event.get("source_peer_id"),
                }

                valid_listening.append(normalized)

            except Exception as exc:
                errors += 1
                _insert_dlq(
                    payload=event,
                    error_type="validation",
                    error_message=str(exc),
                    original_topic="listening_events",
                )

        for event in raw_events.get("p2p_network", []):
            try:
                if "event_id" not in event or "event_type" not in event:
                    raise ValueError("event_id ou event_type manquant")

                if not _is_uuid(event["event_id"]):
                    raise ValueError("event_id invalide")

                if "timestamp" in event:
                    event["timestamp"] = _parse_timestamp(event["timestamp"]).isoformat()

                valid_p2p.append(event)

            except Exception as exc:
                errors += 1
                _insert_dlq(
                    payload=event,
                    error_type="validation",
                    error_message=str(exc),
                    original_topic="p2p_network_events",
                )

        print(
            f"Validation terminée — valid_listening={len(valid_listening)}, "
            f"valid_p2p={len(valid_p2p)}, errors={errors}"
        )

        return {
            "valid_listening": valid_listening,
            "valid_p2p": valid_p2p,
            "errors": errors,
        }

    @task(task_id="enrich_events")
    def enrich_events(validated: dict, **context) -> list:
        """
        Enrichit les événements d'écoute avec le catalogue PostgreSQL.
        """
        listening_events = validated.get("valid_listening", [])

        if not listening_events:
            print("Aucun listening_event valide à enrichir.")
            return []

        track_ids = sorted({event["track_id"] for event in listening_events})

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        placeholders = ",".join(["%s"] * len(track_ids))
        sql = f"""
            SELECT
                id::text,
                title,
                artist_id::text,
                genre
            FROM tracks
            WHERE id::text IN ({placeholders})
        """

        rows = hook.get_records(sql, parameters=track_ids)

        catalog_by_track_id = {
            row[0]: {
                "track_title": row[1],
                "artist_id": row[2],
                "genre": row[3],
            }
            for row in rows
        }

        enriched = []

        for event in listening_events:
            track_id = event["track_id"]
            catalog_data = catalog_by_track_id.get(track_id)

            if not catalog_data:
                _insert_dlq(
                    payload=event,
                    error_type="unknown_track",
                    error_message=f"track_id absent du catalogue PostgreSQL: {track_id}",
                    original_topic="listening_events",
                )
                continue

            enriched.append({
                **event,
                **catalog_data,
            })

        print(f"Enrichissement terminé — events enrichis={len(enriched)}")

        return enriched

    @task(task_id="store_to_parquet")
    def store_to_parquet(enriched_events: list, **context) -> str:
        """
        Sauvegarde les événements enrichis en Parquet sur MinIO.
        """
        if not enriched_events:
            print("Aucun event enrichi à stocker en Parquet.")
            return "no_data"

        df = pd.DataFrame(enriched_events)
        df["timestamp"] = pd.to_datetime(df["timestamp"])
        df["date"] = df["timestamp"].dt.strftime("%Y-%m-%d")
        df["hour"] = df["timestamp"].dt.strftime("%H")

        run_id = context["dag_run"].run_id.replace(":", "_").replace("/", "_")

        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        uploaded_keys = []

        for (date, hour), partition_df in df.groupby(["date", "hour"]):
            key = (
                f"listening_events/date={date}/hour={hour}/"
                f"part-{run_id}.parquet"
            )

            with tempfile.NamedTemporaryFile(suffix=".parquet") as tmp:
                table = pa.Table.from_pandas(partition_df, preserve_index=False)
                pq.write_table(table, tmp.name)
                s3.upload_file(tmp.name, MINIO_BUCKET_PARQUET, key)

            uploaded_keys.append(f"s3://{MINIO_BUCKET_PARQUET}/{key}")

        print("Parquet uploadés:")
        for key in uploaded_keys:
            print(f"- {key}")

        return ",".join(uploaded_keys)

    @task(task_id="upsert_to_postgres")
    def upsert_to_postgres(enriched_events: list, **context) -> dict:
        """
        Insère les événements dans PostgreSQL de façon idempotente.
        """
        if not enriched_events:
            print("Aucun event enrichi à insérer dans PostgreSQL.")
            return {"inserted": 0, "skipped": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()

        sql = """
            INSERT INTO listening_events (
                id,
                user_id,
                track_id,
                source_peer_id,
                timestamp,
                duration_ms,
                device_type,
                geo_country,
                completed,
                event_source
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO NOTHING
        """

        rows = []

        for event in enriched_events:
            rows.append((
                event["event_id"],
                event["user_id"],
                event["track_id"],
                None,
                _parse_timestamp(event["timestamp"]),
                int(event["duration_ms"]),
                event.get("device_type"),
                event.get("geo_country"),
                bool(event.get("completed", False)),
                event.get("event_source", "p2p"),
            ))

        with conn.cursor() as cursor:
            cursor.executemany(sql, rows)

        conn.commit()

        print(f"Insertion PostgreSQL terminée — rows traitées={len(rows)}")

        return {
            "inserted": len(rows),
            "skipped": 0,
        }

    raw = consume_from_redis()
    validated = validate_events(raw)
    enriched = enrich_events(validated)

    store_to_parquet(enriched)
    upsert_to_postgres(enriched)
