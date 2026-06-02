"""
DAG : aggregation_pipeline
============================
Calcule les agrégats quotidiens après la fin du streaming_events_pipeline.
Dépend de streaming_events_pipeline via ExternalTaskSensor.

Architecture :
    ExternalTaskSensor (attend streaming_events_pipeline)
        → compute_top_tracks()      ← top 50 du jour → daily_streams
        → compute_artist_stats()    ← streams + unique_listeners → artist_stats
        → compute_p2p_metrics()     ← taux cache_hit, latence moyenne
        → update_aggregates()       ← écriture PostgreSQL

TODO :
    [ ] Implémenter compute_top_tracks()
    [ ] Implémenter compute_artist_stats()
    [ ] Implémenter compute_p2p_metrics()
    [ ] Implémenter update_aggregates()
    [ ] Configurer correctement l'ExternalTaskSensor
    [ ] Stratégie incrémentale : calculer uniquement pour la date d'exécution
    [ ] Ajouter doc_md sur ce DAG
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.sensors.external_task import ExternalTaskSensor

DAG_DOC = """
## aggregation_pipeline

### Rôle
Calcule les agrégats quotidiens (top tracks, stats artistes, métriques P2P)
après la fin du streaming_events_pipeline.

### Dépendances
Attend la fin de `streaming_events_pipeline` via ExternalTaskSensor.

### Destinations
- Table `daily_streams` : top 50 tracks par jour
- Table `artist_stats` : streams + unique listeners par artiste par jour

### Stratégie
Incrémentale : calcule uniquement pour `execution_date` (le jour courant).
Idempotente : INSERT ... ON CONFLICT (track_id, date) DO UPDATE SET ...

### TODO
Compléter les 4 tâches marquées NotImplementedError.
"""

DEFAULT_ARGS = {
    "owner":             "spotify-team",
    "depends_on_past":   False,
    "start_date":        datetime(2025, 1, 1),
    "retries":           2,
    "retry_delay":       timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"


with DAG(
    dag_id="aggregation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Agrégats quotidiens : top tracks, stats artistes, métriques P2P",
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "aggregation"],
    doc_md=DAG_DOC,
) as dag:

    wait_for_events = ExternalTaskSensor(
        task_id="wait_for_streaming_events",
        external_dag_id="streaming_events_pipeline",
        external_task_id=None,     # attend la fin du DAGRun complet
        allowed_states=["success"],
        timeout=3600,
        poke_interval=60,
        mode="reschedule",
    )

    @task(task_id="compute_top_tracks")
    def compute_top_tracks(**context) -> list:
        """
        Calcule le top 50 des tracks pour la date d'exécution.
        On cible uniquement les écoutes "completed" (>30s) — comme le vrai Spotify
        qui ne comptabilise un stream que si l'écoute dépasse 30 secondes.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        log = logging.getLogger(__name__)

        # data_interval_start = début de la fenêtre d'exécution
        # Pour un DAG qui tourne à 4h le mardi, data_interval_start = lundi 4h
        # Donc on calcule les stats du JOUR PRÉCÉDENT, ce qui est logique :
        # on agrège la journée complète d'hier une fois qu'elle est terminée.
        execution_date = context["data_interval_start"].date()
        log.info(f"Calcul top tracks pour la date : {execution_date}")

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Une seule requête SQL qui fait tout le travail :
        # - COUNT(*) = nombre total de streams
        # - COUNT(DISTINCT user_id) = auditeurs uniques (pas de doublons)
        # - ARRAY_AGG = liste des pays d'où viennent les écoutes
        # - WHERE completed = TRUE : on ne compte que les vraies écoutes (>30s)
        rows = pg.get_records(
            """
            SELECT
                track_id::text,
                COUNT(*)                        AS total_streams,
                COUNT(DISTINCT user_id)         AS unique_listeners,
                SUM(duration_ms)                AS total_duration_ms,
                ARRAY_AGG(DISTINCT geo_country) AS countries
            FROM listening_events
            WHERE DATE(timestamp) = %s
              AND completed = TRUE
            GROUP BY track_id
            ORDER BY total_streams DESC
            LIMIT 50
            """,
            parameters=(execution_date,),
        )

        results = [
            {
                "track_id":         row[0],
                "total_streams":    row[1],
                "unique_listeners": row[2],
                "total_duration_ms": row[3],
                "countries":        row[4],
            }
            for row in rows
        ]

        log.info(f"Top tracks calculés : {len(results)} tracks pour {execution_date}")
        return results

    @task(task_id="compute_artist_stats")
    def compute_artist_stats(**context) -> list:
        """
        Calcule les statistiques par artiste pour la date d'exécution.
        On fait une jointure entre 3 tables : listening_events → tracks → artists.
        Le "top_track_id" est la track la plus streamée de cet artiste ce jour-là.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        log = logging.getLogger(__name__)
        execution_date = context["data_interval_start"].date()

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Jointure listening_events → tracks → artists
        # DISTINCT ON (t.artist_id) + ORDER BY streams DESC = track la + streamée
        # C'est une technique SQL appelée "top-1 par groupe"
        rows = pg.get_records(
            """
            WITH streams_by_track AS (
                SELECT
                    t.artist_id,
                    le.track_id,
                    COUNT(*)                AS streams,
                    COUNT(DISTINCT le.user_id) AS listeners
                FROM listening_events le
                JOIN tracks t ON t.id = le.track_id::uuid
                WHERE DATE(le.timestamp) = %s
                GROUP BY t.artist_id, le.track_id
            ),
            top_track_per_artist AS (
                SELECT DISTINCT ON (artist_id)
                    artist_id,
                    track_id AS top_track_id
                FROM streams_by_track
                ORDER BY artist_id, streams DESC
            )
            SELECT
                sbt.artist_id::text,
                SUM(sbt.streams)      AS total_streams,
                SUM(sbt.listeners)    AS unique_listeners,
                tt.top_track_id::text
            FROM streams_by_track sbt
            JOIN top_track_per_artist tt ON tt.artist_id = sbt.artist_id
            GROUP BY sbt.artist_id, tt.top_track_id
            ORDER BY total_streams DESC
            """,
            parameters=(execution_date,),
        )

        results = [
            {
                "artist_id":        row[0],
                "total_streams":    row[1],
                "unique_listeners": row[2],
                "top_track_id":     row[3],
            }
            for row in rows
        ]

        log.info(f"Stats artistes calculées : {len(results)} artistes pour {execution_date}")
        return results

    @task(task_id="compute_p2p_metrics")
    def compute_p2p_metrics(**context) -> dict:
        """
        Calcule les métriques du réseau P2P pour la date d'exécution.

        Ces métriques servent à monitorer la santé du réseau P2P :
        - Si le taux de cache_hit est bas → les peers ne gardent pas les tracks en cache
        - Si la latence P2P est haute → le réseau est congestionné
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        log = logging.getLogger(__name__)
        execution_date = context["data_interval_start"].date()

        pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

        # Taux de cache_hit : proportion d'écoutes servies depuis le cache local
        # event_source = 'cache' signifie que le peer avait déjà la track
        row = pg.get_first(
            """
            SELECT
                COUNT(*)                                              AS total_events,
                COUNT(*) FILTER (WHERE event_source = 'cache')       AS cache_hits,
                COUNT(*) FILTER (WHERE event_source = 'p2p')         AS p2p_transfers,
                COUNT(*) FILTER (WHERE event_source = 'direct')      AS direct_streams,
                COUNT(DISTINCT source_peer_id)                        AS active_peers
            FROM listening_events
            WHERE DATE(timestamp) = %s
            """,
            parameters=(execution_date,),
        )

        total = row[0] or 1  # évite la division par zéro si aucun event
        metrics = {
            "date":             str(execution_date),
            "total_events":     row[0],
            "cache_hit_rate":   round((row[1] or 0) / total, 4),  # ex: 0.32 = 32%
            "p2p_rate":         round((row[2] or 0) / total, 4),
            "direct_rate":      round((row[3] or 0) / total, 4),
            "active_peers":     row[4],
        }

        log.info(f"Métriques P2P : cache_hit={metrics['cache_hit_rate']:.1%} | "
                 f"peers_actifs={metrics['active_peers']}")
        return metrics

    @task(task_id="update_aggregates")
    def update_aggregates(top_tracks: list, artist_stats: list, p2p_metrics: dict, **context):
        """
        Écrit les agrégats dans PostgreSQL de façon idempotente.

        "Idempotent" = on peut relancer ce DAG plusieurs fois sans créer de doublons.
        ON CONFLICT ... DO UPDATE garantit ça : si la ligne existe déjà pour
        (track_id, date), on la met à jour au lieu d'en créer une nouvelle.
        C'est crucial pour les reprises après panne.
        """
        import logging
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        log = logging.getLogger(__name__)
        execution_date = context["data_interval_start"].date()

        pg   = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg.get_conn()
        cur  = conn.cursor()

        # ── 1. UPSERT daily_streams ───────────────────────────────────────
        # ON CONFLICT (track_id, date) : si cette track a déjà des stats pour
        # ce jour, on écrase avec les nouvelles valeurs (DO UPDATE SET).
        # C'est différent de DO NOTHING : on veut toujours la valeur la plus fraîche.
        upsert_streams = """
            INSERT INTO daily_streams
                (track_id, date, total_streams, unique_listeners, total_duration_ms, countries, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, NOW())
            ON CONFLICT (track_id, date) DO UPDATE SET
                total_streams     = EXCLUDED.total_streams,
                unique_listeners  = EXCLUDED.unique_listeners,
                total_duration_ms = EXCLUDED.total_duration_ms,
                countries         = EXCLUDED.countries,
                updated_at        = NOW()
        """
        for t in top_tracks:
            cur.execute(upsert_streams, (
                t["track_id"],
                execution_date,
                t["total_streams"],
                t["unique_listeners"],
                t["total_duration_ms"],
                t["countries"],
            ))

        # ── 2. UPSERT artist_stats ────────────────────────────────────────
        upsert_artists = """
            INSERT INTO artist_stats
                (artist_id, date, total_streams, unique_listeners, top_track_id, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (artist_id, date) DO UPDATE SET
                total_streams    = EXCLUDED.total_streams,
                unique_listeners = EXCLUDED.unique_listeners,
                top_track_id     = EXCLUDED.top_track_id,
                updated_at       = NOW()
        """
        for a in artist_stats:
            cur.execute(upsert_artists, (
                a["artist_id"],
                execution_date,
                a["total_streams"],
                a["unique_listeners"],
                a["top_track_id"],
            ))

        conn.commit()
        cur.close()

        # ── 3. Log récapitulatif ──────────────────────────────────────────
        log.info(
            f"Agrégats du {execution_date} écrits en base : "
            f"{len(top_tracks)} tracks, {len(artist_stats)} artistes | "
            f"cache_hit={p2p_metrics.get('cache_hit_rate', 0):.1%}"
        )
        if top_tracks:
            log.info(f"Track #1 du jour : {top_tracks[0]['track_id']} "
                     f"avec {top_tracks[0]['total_streams']} streams")

    # ── Orchestration ─────────────────────────────────────────
    top_tracks   = compute_top_tracks()
    artist_stats = compute_artist_stats()
    p2p_metrics  = compute_p2p_metrics()

    wait_for_events >> [top_tracks, artist_stats, p2p_metrics]
    update_aggregates(top_tracks, artist_stats, p2p_metrics)
