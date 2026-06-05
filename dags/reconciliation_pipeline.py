"""
DAG : reconciliation_pipeline
================================
Pont batch/streaming — compare les agrégats batch (daily_streams)
avec les résultats Spark streaming (realtime_top_tracks) pour détecter
les divergences entre les deux couches de l'architecture Lambda.

Architecture Lambda :
    Speed layer  : Spark Structured Streaming → realtime_top_tracks
    Batch layer  : Airflow DAGs               → daily_streams
    Ce DAG       : compare les deux et alerte si divergence > 5%

Schedule : toutes les heures
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

DAG_DOC = """
## reconciliation_pipeline

### Rôle
Compare les agrégats batch (`daily_streams`) avec les résultats Spark
streaming (`realtime_top_tracks`) pour détecter les divergences entre
les deux couches de l'architecture Lambda.

### Architecture Lambda
- **Speed layer** : Spark → `realtime_top_tracks` (données temps réel)
- **Batch layer** : Airflow → `daily_streams` (données agrégées)
- **Ce DAG** : réconciliation entre les deux

### Logique
1. Extraire les top tracks depuis `realtime_top_tracks` (dernière heure)
2. Extraire les top tracks depuis `daily_streams` (aujourd'hui)
3. Calculer le taux de divergence par track
4. Alerter si divergence > 5%
5. Logger le rapport de réconciliation

### Critère de validation
Log du DAGRun montrant les taux de convergence batch/streaming.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           1,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID    = "spotify_postgres"
DIVERGENCE_THRESHOLD = 0.05  # 5%


with DAG(
    dag_id="reconciliation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Pont batch/streaming — réconciliation daily_streams vs realtime_top_tracks",
    schedule_interval="@hourly",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-2", "reconciliation", "lambda"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_streaming_top_tracks")
    def extract_streaming_top_tracks(**context) -> list:
        """
        Extrait les top tracks depuis realtime_top_tracks
        pour la dernière heure.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            """
            SELECT
                track_id,
                SUM(stream_count)    AS streaming_count,
                MAX(unique_listeners) AS streaming_listeners,
                MAX(updated_at)       AS last_updated
            FROM realtime_top_tracks
            WHERE window_start >= NOW() - INTERVAL '1 hour'
            GROUP BY track_id
            ORDER BY streaming_count DESC
            LIMIT 50
            """
        )

        result = [
            {
                "track_id":            str(row[0]),
                "streaming_count":     int(row[1]),
                "streaming_listeners": int(row[2]),
            }
            for row in rows
        ]

        print(f"[streaming] {len(result)} tracks extraits depuis realtime_top_tracks")
        return result

    @task(task_id="extract_batch_top_tracks")
    def extract_batch_top_tracks(**context) -> list:
        """
        Extrait les top tracks depuis daily_streams
        pour aujourd'hui.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        rows = hook.get_records(
            """
            SELECT
                track_id,
                SUM(total_streams)  AS batch_count,
                SUM(unique_listeners) AS batch_listeners
            FROM daily_streams
            WHERE date = CURRENT_DATE
            GROUP BY track_id
            ORDER BY batch_count DESC
            LIMIT 50
            """
        )

        result = [
            {
                "track_id":       str(row[0]),
                "batch_count":    int(row[1]),
                "batch_listeners": int(row[2]),
            }
            for row in rows
        ]

        print(f"[batch] {len(result)} tracks extraits depuis daily_streams")
        return result

    @task(task_id="compute_divergence")
    def compute_divergence(streaming_tracks: list, batch_tracks: list, **context) -> dict:
        """
        Calcule le taux de divergence entre batch et streaming pour chaque track.

        Formule : divergence = |batch_count - streaming_count| / max(batch_count, streaming_count)
        """
        streaming_map = {t["track_id"]: t for t in streaming_tracks}
        batch_map     = {t["track_id"]: t for t in batch_tracks}

        all_track_ids = set(streaming_map.keys()) | set(batch_map.keys())

        divergences  = []
        alerts       = []
        converged    = []

        for track_id in all_track_ids:
            s_count = streaming_map.get(track_id, {}).get("streaming_count", 0)
            b_count = batch_map.get(track_id, {}).get("batch_count", 0)

            if s_count == 0 and b_count == 0:
                continue

            max_count  = max(s_count, b_count)
            divergence = abs(b_count - s_count) / max_count if max_count > 0 else 0

            entry = {
                "track_id":        track_id,
                "streaming_count": s_count,
                "batch_count":     b_count,
                "divergence_pct":  round(divergence * 100, 2),
            }

            divergences.append(entry)

            if divergence > DIVERGENCE_THRESHOLD:
                alerts.append(entry)
            else:
                converged.append(entry)

        divergences.sort(key=lambda x: x["divergence_pct"], reverse=True)

        report = {
            "total_tracks":    len(divergences),
            "converged":       len(converged),
            "alerts":          len(alerts),
            "convergence_rate": round(len(converged) / len(divergences) * 100, 2) if divergences else 100.0,
            "top_divergences": divergences[:10],
            "alert_tracks":    alerts,
        }

        print(f"\n{'='*60}")
        print(f"RAPPORT DE RÉCONCILIATION BATCH/STREAMING")
        print(f"{'='*60}")
        print(f"Tracks analysés    : {report['total_tracks']}")
        print(f"Convergés          : {report['converged']}")
        print(f"En alerte (>5%)    : {report['alerts']}")
        print(f"Taux de convergence: {report['convergence_rate']}%")
        print(f"\nTop 5 divergences :")
        for t in divergences[:5]:
            print(
                f"  {t['track_id'][:8]}... "
                f"streaming={t['streaming_count']} "
                f"batch={t['batch_count']} "
                f"divergence={t['divergence_pct']}%"
            )
        print(f"{'='*60}\n")

        return report

    @task(task_id="store_reconciliation_report")
    def store_reconciliation_report(report: dict, **context) -> dict:
        """
        Stocke le rapport de réconciliation dans PostgreSQL (dead_letter_events)
        et alerte dans les logs si des tracks dépassent le seuil.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        dag_run = context["dag_run"]

        # Stocker les alertes dans dead_letter_events
        alert_tracks = report.get("alert_tracks", [])
        if alert_tracks:
            for track in alert_tracks:
                import json
                hook.run(
                    """
                    INSERT INTO dead_letter_events
                        (original_topic, payload, error_type, error_message)
                    VALUES (%s, %s::jsonb, %s, %s)
                    """,
                    parameters=(
                        "reconciliation_pipeline",
                        json.dumps(track),
                        "divergence_alert",
                        f"Track {track['track_id'][:8]} divergence {track['divergence_pct']}% > {DIVERGENCE_THRESHOLD*100}%",
                    ),
                )
            print(f"⚠️  {len(alert_tracks)} tracks en alerte stockés dans dead_letter_events")
        else:
            print("✅ Aucune alerte — tous les tracks convergent à moins de 5%")

        stats = {
            "dag_run_id":       dag_run.run_id,
            "total_tracks":     report["total_tracks"],
            "converged":        report["converged"],
            "alerts":           report["alerts"],
            "convergence_rate": report["convergence_rate"],
        }

        print(f"\n✅ Rapport stocké — convergence globale : {report['convergence_rate']}%")
        return stats

    @task(task_id="notify_reconciliation")
    def notify_reconciliation(stats: dict, **context):
        """
        Log final du rapport de réconciliation.
        """
        convergence = stats.get("convergence_rate", 0)
        alerts      = stats.get("alerts", 0)

        if convergence >= 95:
            status = "✅ EXCELLENT"
        elif convergence >= 80:
            status = "⚠️  ACCEPTABLE"
        else:
            status = "❌ DIVERGENCE CRITIQUE"

        print(f"""
        ╔══════════════════════════════════════════╗
        ║     RÉCONCILIATION BATCH/STREAMING       ║
        ╠══════════════════════════════════════════╣
        ║  Status         : {status:<22} ║
        ║  Convergence    : {convergence:.1f}%{'':<20} ║
        ║  Tracks total   : {stats.get('total_tracks', 0):<22} ║
        ║  Convergés      : {stats.get('converged', 0):<22} ║
        ║  Alertes (>5%)  : {alerts:<22} ║
        ╚══════════════════════════════════════════╝
        """)

    # Orchestration
    streaming = extract_streaming_top_tracks()
    batch     = extract_batch_top_tracks()
    report    = compute_divergence(streaming, batch)
    stats     = store_reconciliation_report(report)
    notify_reconciliation(stats)