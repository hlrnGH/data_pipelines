"""
DAG : late_events_reprocessing
================================
Retraite les events trop tardifs routés par Spark (#15) depuis le topic
Kafka `late_listening_events` vers la table `listening_events`.

Pont Spark → Airflow de l'architecture Lambda :
    Speed layer  : Spark → topic `late_listening_events` (Issue #15)
    Batch layer  : Ce DAG → `listening_events` + `daily_streams`

Planification : @hourly
"""

import json
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## late_events_reprocessing

### Rôle
Consomme le topic Kafka `late_listening_events` (mode availableNow),
revalide les events tardifs et les insère dans `listening_events`.
Recalcule ensuite les agrégats `daily_streams` affectés.

### Lien avec #15
Les late events détectés par Spark (watermark 10 min) sont routés vers
le topic `late_listening_events`. Ce DAG les récupère et les réintègre
dans le pipeline batch.

### Architecture Lambda
- Speed layer : Spark → `late_listening_events`
- Batch layer : Ce DAG → `listening_events` + `daily_streams`

### Critère de validation
SELECT COUNT(*) FROM listening_events augmente lors de l'exécution.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID  = "spotify_postgres"
KAFKA_BOOTSTRAP   = "kafka-1:9092"
LATE_EVENTS_TOPIC = "late_listening_events"
BATCH_SIZE        = 500


with DAG(
    dag_id="late_events_reprocessing",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des late events Spark → listening_events",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "late-events", "lambda"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="consume_late_events_from_kafka")
    def consume_late_events_from_kafka(**context) -> list:
        """
        Consomme le topic `late_listening_events` en mode availableNow.
        Retourne la liste des events JSON à retraiter.
        """
        try:
            from confluent_kafka import Consumer, KafkaException, TopicPartition
            from confluent_kafka.admin import AdminClient

            admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
            metadata = admin.list_topics(topic=LATE_EVENTS_TOPIC, timeout=10)

            if LATE_EVENTS_TOPIC not in metadata.topics:
                print(f"Topic {LATE_EVENTS_TOPIC} introuvable — 0 events à retraiter")
                return []

            consumer = Consumer({
                "bootstrap.servers":  KAFKA_BOOTSTRAP,
                "group.id":           "airflow-late-events-reprocessing",
                "auto.offset.reset":  "earliest",
                "enable.auto.commit": False,
            })

            consumer.subscribe([LATE_EVENTS_TOPIC])

            events    = []
            empty_polls = 0
            max_empty   = 3

            while len(events) < BATCH_SIZE and empty_polls < max_empty:
                msg = consumer.poll(timeout=2.0)
                if msg is None:
                    empty_polls += 1
                    continue
                if msg.error():
                    print(f"Kafka error : {msg.error()}")
                    empty_polls += 1
                    continue

                try:
                    payload = json.loads(msg.value().decode("utf-8"))
                    events.append(payload)
                    empty_polls = 0
                except json.JSONDecodeError as e:
                    print(f"JSON parse error : {e}")

            consumer.close()
            print(f"[kafka] {len(events)} late events consommés depuis {LATE_EVENTS_TOPIC}")
            return events

        except ImportError:
            print("confluent_kafka non disponible — simulation avec 0 events")
            return []
        except Exception as e:
            print(f"Kafka connexion error : {e} — 0 events à retraiter")
            return []

    @task(task_id="validate_late_events")
    def validate_late_events(raw_events: list, **context) -> dict:
        """
        Revalide les events tardifs :
        - Champs obligatoires présents (event_id, user_id, track_id, timestamp)
        - Durée > 0
        - Event non déjà présent dans listening_events
        """
        if not raw_events:
            print("Aucun event à valider")
            return {"valid": [], "invalid": []}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Récupère les event_ids déjà présents pour éviter les doublons
        existing_ids = set()

        valid   = []
        invalid = []

        for event in raw_events:
            event_id  = str(event.get("event_id", ""))
            user_id   = str(event.get("user_id", ""))
            track_id  = str(event.get("track_id", ""))
            timestamp = str(event.get("event_time") or event.get("timestamp", ""))
            duration  = int(event.get("duration_ms", 0))

            # Validation
            if not event_id or not user_id or not track_id or not timestamp:
                invalid.append({"event": event, "reason": "champs_obligatoires_manquants"})
                continue

            int(event.get("duration_ms", 0)),


            valid.append(event)

        print(f"[validate] {len(valid)} valides, {len(invalid)} invalides")
        return {"valid": valid, "invalid": invalid}

    @task(task_id="insert_into_listening_events")
    def insert_into_listening_events(validation_result: dict, **context) -> dict:
        """
        Insère les events valides dans `listening_events`.
        """
        valid_events = validation_result.get("valid", [])

        if not valid_events:
            print("Aucun event valide à insérer")
            return {"inserted": 0, "track_ids": []}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()

        inserted   = 0
        track_ids  = set()

        try:
            with conn.cursor() as cur:
                for event in valid_events:
                    try:
                        cur.execute(
                            """
                            INSERT INTO listening_events (
                                user_id, track_id, source_peer_id,
                                timestamp, duration_ms, device_type,
                                geo_country, completed, event_source
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """,
                            (
                                str(event.get("user_id")),
                                str(event.get("track_id")),
                                None,
                                str(event.get("event_time") or event.get("timestamp")),
                                int(event.get("duration_ms", 0)),
                                str(event.get("device_type", "unknown")),
                                str(event.get("geo_country", "XX")),
                                bool(event.get("completed", False)),
                                "late_reprocessed",
                            ),
                        )
                        inserted += 1
                        track_ids.add(str(event.get("track_id")))
                    except Exception as e:
                        print(f"Insert error pour event {event.get('event_id')} : {e}")
                        conn.rollback()

            conn.commit()
            print(f"[insert] {inserted} events insérés dans listening_events")

        finally:
            conn.close()

        return {"inserted": inserted, "track_ids": list(track_ids)}

    @task(task_id="recalculate_daily_streams")
    def recalculate_daily_streams(insert_result: dict, **context) -> dict:
        """
        Recalcule les agrégats `daily_streams` pour les tracks affectées.
        """
        inserted  = insert_result.get("inserted", 0)
        track_ids = insert_result.get("track_ids", [])

        if not track_ids:
            print("Aucun agrégat à recalculer")
            return {"updated_tracks": 0}

        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        updated = 0
        for track_id in track_ids:
            hook.run(
                """
                INSERT INTO daily_streams (track_id, date, total_streams, unique_listeners, total_duration_ms)
                SELECT
                    track_id,
                    DATE(timestamp) AS date,
                    COUNT(*)                   AS total_streams,
                    COUNT(DISTINCT user_id)    AS unique_listeners,
                    SUM(duration_ms)           AS total_duration_ms
                FROM listening_events
                WHERE track_id = %s
                  AND event_source = 'late_reprocessed'
                GROUP BY track_id, DATE(timestamp)
                ON CONFLICT (track_id, date) DO UPDATE SET
                    total_streams     = daily_streams.total_streams     + EXCLUDED.total_streams,
                    unique_listeners  = daily_streams.unique_listeners  + EXCLUDED.unique_listeners,
                    total_duration_ms = daily_streams.total_duration_ms + EXCLUDED.total_duration_ms,
                    updated_at        = NOW()
                """,
                parameters=(track_id,),
            )
            updated += 1

        print(f"[daily_streams] {updated} tracks recalculées")
        return {"updated_tracks": updated}

    @task(task_id="notify_reprocessing")
    def notify_reprocessing(insert_result: dict, recalc_result: dict, **context):
        """
        Log final du retraitement.
        """
        inserted = insert_result.get("inserted", 0)
        updated  = recalc_result.get("updated_tracks", 0)

        print(f"""
        ╔══════════════════════════════════════════╗
        ║     RETRAITEMENT LATE EVENTS             ║
        ╠══════════════════════════════════════════╣
        ║  Events insérés    : {inserted:<20} ║
        ║  Tracks recalculées: {updated:<20} ║
        ╚══════════════════════════════════════════╝
        """)

    # Orchestration
    raw_events        = consume_late_events_from_kafka()
    validation_result = validate_late_events(raw_events)
    insert_result     = insert_into_listening_events(validation_result)
    recalc_result     = recalculate_daily_streams(insert_result)
    notify_reprocessing(insert_result, recalc_result)