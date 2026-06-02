from datetime import datetime, timedelta
import json
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## dlq_reprocessing_pipeline

### Rôle
Retraite les événements défectueux isolés dans `dead_letter_events`.
Tente de corriger les erreurs et de réinjecter les events valides.

### Sources
- Table `dead_letter_events` où `status = 'pending'`

### Logique de retraitement
1. Récupérer les events `pending` avec `retry_count < 3`
2. Tenter la validation et la correction
3. Si succès → réinjecter dans `listening_events` + `status = 'reprocessed'`
4. Si échec après 3 tentatives → `status = 'abandoned'`

### Test d'injection
```sql
INSERT INTO dead_letter_events (payload, error_type, original_topic)
VALUES ('{"user_id": null, "track_id": "invalid"}', 'missing_fields', 'listening_events');
```
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=20),
}

POSTGRES_CONN_ID = "spotify_postgres"
MAX_RETRIES      = 3
BATCH_SIZE       = 100

log = logging.getLogger(__name__)


with DAG(
    dag_id="dlq_reprocessing_pipeline",
    default_args=DEFAULT_ARGS,
    description="Retraitement horaire des événements Dead Letter Queue",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "dlq", "resilience"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="fetch_pending_dlq")
    def fetch_pending_dlq(**context) -> list:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            sql="""
                SELECT id, payload, error_type, retry_count, original_topic
                FROM dead_letter_events
                WHERE status = 'pending'
                  AND retry_count < %(max_retries)s
                ORDER BY created_at ASC
                LIMIT %(batch_size)s
            """,
            parameters={"max_retries": MAX_RETRIES, "batch_size": BATCH_SIZE},
        )

        events = [
            {
                "id":             row[0],
                "payload":        row[1],
                "error_type":     row[2],
                "retry_count":    row[3],
                "original_topic": row[4],
            }
            for row in rows
        ]

        log.info("%d événements pending trouvés", len(events))
        return events


    @task(task_id="reprocess_events")
    def reprocess_events(pending_events: list, **context) -> dict:
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        reprocessed = []
        failed      = []

        for event in pending_events:
            event_id = event["id"]

            # 1. Parser le payload
            try:
                payload = json.loads(event["payload"]) if isinstance(event["payload"], str) else event["payload"]
            except (json.JSONDecodeError, TypeError) as exc:
                log.warning("Event %s : payload JSON invalide (%s)", event_id, exc)
                failed.append({"id": event_id, "reason": "invalid_json"})
                continue

            # 2 & 3. Valider et corriger selon les règles métier
            corrected, reason = _validate_and_fix(payload, event, hook)

            if corrected is not None:
                reprocessed.append({"id": event_id, "payload": corrected, "original_topic": event["original_topic"]})
            else:
                failed.append({"id": event_id, "reason": reason})

        log.info(
            "Retraitement terminé : %d succès, %d échecs",
            len(reprocessed), len(failed),
        )
        return {"reprocessed": reprocessed, "failed": failed}


    def _validate_and_fix(payload: dict, event: dict, hook: PostgresHook):
        """
        Valide et corrige un payload.
        Retourne (payload_corrigé, None) en cas de succès,
        ou (None, raison) en cas d'échec irrécupérable.
        """
        # Règle 1 : user_id obligatoire, impossible à corriger
        if not payload.get("user_id"):
            return None, "missing_user_id"

        # Règle 2 : timestamp invalide → fallback sur created_at de la DLQ
        if not payload.get("timestamp"):
            log.info("Event %s : timestamp manquant, utilisation de created_at", event["id"])
            row = hook.get_first(
                "SELECT created_at FROM dead_letter_events WHERE id = %s",
                parameters=(event["id"],),
            )
            payload["timestamp"] = row[0].isoformat() if row else datetime.utcnow().isoformat()

        # Règle 3 : track_id doit exister dans la table tracks
        track_id = payload.get("track_id")
        if not track_id:
            return None, "missing_track_id"

        exists = hook.get_first(
            "SELECT 1 FROM tracks WHERE id = %s LIMIT 1",
            parameters=(track_id,),
        )
        if not exists:
            return None, f"unknown_track_id:{track_id}"

        return payload, None


@task(task_id="update_dlq_status")
def update_dlq_status(results: dict, **context) -> dict:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = hook.get_conn()
    cursor = conn.cursor()

    reprocessed = results.get("reprocessed", [])
    failed      = results.get("failed", [])

    # 1. Events corrigés avec succès : insertion dans listening_events + marquage
    for event in reprocessed:
        cursor.execute(
            """
            INSERT INTO listening_events (payload, source_topic, imported_at)
            VALUES (%s, %s, NOW())
            """,
            (
                json.dumps(event["payload"]),
                event.get("original_topic"),
            ),
        )

        cursor.execute(
            """
            UPDATE dead_letter_events
               SET status      = 'reprocessed',
                   resolved_at = NOW()
             WHERE id = %s
            """,
            (event["id"],),
        )

    # 2. Events en échec : incrémenter retry_count, basculer en abandoned si ≥ MAX_RETRIES
    failed_ids = [e["id"] for e in failed]

    if failed_ids:
        cursor.execute(
            """
            UPDATE dead_letter_events
               SET retry_count   = retry_count + 1,
                   last_retry_at = NOW(),
                   status        = CASE
                                     WHEN retry_count + 1 >= %(max_retries)s THEN 'abandoned'
                                     ELSE 'pending'
                                   END
             WHERE id = ANY(%(ids)s)
            """,
            {
                "max_retries": MAX_RETRIES,
                "ids": failed_ids,
            },
        )

    conn.commit()
    cursor.close()

    # 3. Bilan (corrigé pour être cohérent avec DB, pas Python approximé)
    nb_reprocessed = len(reprocessed)
    nb_abandoned   = 0
    nb_pending     = len(failed)

    if failed_ids:
        # recalcul propre depuis DB (plus fiable que logique Python)
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        rows = hook.get_records(
            """
            SELECT status, COUNT(*)
            FROM dead_letter_events
            WHERE id = ANY(%s)
            GROUP BY status
            """,
            parameters=(failed_ids,),
        )

        status_map = {r[0]: r[1] for r in rows}
        nb_abandoned = status_map.get("abandoned", 0)
        nb_pending   = status_map.get("pending", 0)

    log.info(
        "Bilan DLQ : %d retraités, %d abandonnés, %d encore en pending",
        nb_reprocessed, nb_abandoned, nb_pending,
    )

    return {
        "reprocessed": nb_reprocessed,
        "abandoned":   nb_abandoned,
        "pending":     nb_pending,
    }

    # ── Orchestration ─────────────────────────────────────────
    pending = fetch_pending_dlq()
    results = reprocess_events(pending)
    update_dlq_status(results)