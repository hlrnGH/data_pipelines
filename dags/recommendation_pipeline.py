"""
DAG : recommendation_pipeline
==============================
Calcule les recommandations musicales par collaborative filtering
(user-based cosine similarity) et les stocke dans Redis + PostgreSQL.

Planification : quotidienne à 04:00 UTC (après aggregation_pipeline à 03:00)
Catchup       : désactivé

Architecture :
    ExternalTaskSensor(aggregation_pipeline)
        → build_user_track_matrix()   ← matrice user×track, fenêtre 7 jours
        → compute_recommendations()   ← cosine similarity, top-5 voisins, top-10 reco
        → store_recommendations()     ← Redis setex TTL 24h + PostgreSQL upsert
"""

import json
import logging
from datetime import datetime, timedelta

import numpy as np

from airflow import DAG
from airflow.decorators import task
from airflow.models import DagRun as AirflowDagRun
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.session import provide_session

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

POSTGRES_CONN_ID   = "spotify_postgres"
REDIS_CONN_ID      = "spotify_redis"
WINDOW_DAYS        = 7       # fenêtre glissante pour la matrice
TOP_N_NEIGHBORS    = 5       # nombre de voisins pour le collaborative filtering
TOP_N_RECO         = 10      # nombre de recommandations par utilisateur
REDIS_TTL_SECONDS  = 86400   # 24h

DEFAULT_ARGS = {
    "owner":                     "spotify-team",
    "depends_on_past":           False,
    "start_date":                datetime(2025, 1, 1),
    "email_on_failure":          False,
    "email_on_retry":            False,
    "retries":                   2,
    "retry_delay":               timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout":         timedelta(minutes=60),
}

DAG_DOC = """
## recommendation_pipeline

### Rôle
Calcule des recommandations musicales personnalisées par collaborative filtering
user-based (cosine similarity) sur les écoutes des 7 derniers jours.

### Dépendance
Déclenché après `aggregation_pipeline` via ExternalTaskSensor.

### Algorithme
1. Matrice user×track : valeur = nombre d'écoutes (play_count ou fréquence)
2. Cosine similarity entre utilisateurs → top-5 voisins les plus proches
3. Score de recommandation = somme pondérée des écoutes des voisins
4. Top-10 tracks non encore écoutées par l'utilisateur cible

### Destinations
- Redis : `reco:{user_id}` → JSON list de track_ids, TTL 24h
- PostgreSQL : table `recommendations` (upsert)
"""



# ─────────────────────────────────────────────────────────────
# CALLBACK ON_FAILURE
# ─────────────────────────────────────────────────────────────

def on_failure_callback(context):
    task_instance = context.get("task_instance")
    exception     = context.get("exception")
    logger.error(
        "❌ Échec DAG=%s | Task=%s | Run=%s | Exception=%s",
        task_instance.dag_id,
        task_instance.task_id,
        task_instance.run_id,
        str(exception),
    )


@provide_session
def _latest_aggregation_run_date(logical_date, session=None, **kwargs):
    """
    Pointe sur le dernier run réussi d'aggregation_pipeline.
    Évite le blocage quand les execution_dates ne coïncident pas exactement
    (runs manuels, décalages de schedule, etc.).
    """
    last_run = (
        session.query(AirflowDagRun)
        .filter(AirflowDagRun.dag_id == "aggregation_pipeline")
        .filter(AirflowDagRun.state == "success")
        .order_by(AirflowDagRun.execution_date.desc())
        .first()
    )
    return last_run.execution_date if last_run else logical_date


# ─────────────────────────────────────────────────────────────
# DAG DEFINITION
# ─────────────────────────────────────────────────────────────

with DAG(
    dag_id="recommendation_pipeline",
    default_args=DEFAULT_ARGS,
    description="Recommandations musicales par collaborative filtering",
    schedule_interval="0 5 * * *",
    catchup=False,
    max_active_runs=1,
    tags=["spotify", "phase-1", "recommandation", "ml"],
    doc_md=DAG_DOC,
    on_failure_callback=on_failure_callback,
) as dag:

    # ── Attente de la fin de aggregation_pipeline ─────────────
    wait_for_aggregation = ExternalTaskSensor(
        task_id="wait_for_aggregation_pipeline",
        external_dag_id="aggregation_pipeline",
        external_task_id=None,
        allowed_states=["success"],
        execution_date_fn=_latest_aggregation_run_date,
        timeout=3600,
        poke_interval=10,
        mode="reschedule",
    )

    @task(task_id="build_user_track_matrix")
    def build_user_track_matrix(**context) -> dict:
        """
        Construit la matrice user×track depuis listening_events.

        Fenêtre glissante : WINDOW_DAYS derniers jours par rapport à execution_date.
        Valeur de la matrice : nombre total d'écoutes (play_count agrégé).

        Returns:
            dict {
                "matrix":    {user_id: {track_id: score}},
                "user_ids":  [str, ...],
                "track_ids": [str, ...]
            }
        """
        pg_hook       = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn          = pg_hook.get_conn()
        cursor        = conn.cursor()

        execution_date = context["data_interval_end"]
        window_start   = execution_date - timedelta(days=WINDOW_DAYS)

        logger.info(
            "📊 Construction matrice user×track — fenêtre : %s → %s",
            window_start.date(), execution_date.date(),
        )

        # Agrégation des écoutes par (user_id, track_id) sur la fenêtre
        # Filtre : uniquement les users avec >= 3 tracks distinctes écoutées
        # (profil trop mince = similarité cosinus inexploitable)
        cursor.execute(
            """
            SELECT
                user_id,
                track_id,
                COUNT(*) AS play_count
            FROM listening_events
            WHERE DATE(timestamp) >= %s
              AND DATE(timestamp) <= %s
            GROUP BY user_id, track_id
            HAVING user_id IN (
                SELECT user_id
                FROM listening_events
                WHERE DATE(timestamp) >= %s
                  AND DATE(timestamp) <= %s
                GROUP BY user_id
                HAVING COUNT(DISTINCT track_id) >= 3
            )
            """,
            (window_start.date(), execution_date.date(),
             window_start.date(), execution_date.date()),
        )
        rows = cursor.fetchall()
        cursor.close()
        conn.close()

        if not rows:
            logger.warning("⚠️  Aucune écoute dans la fenêtre — matrice vide.")
            return {"matrix": {}, "user_ids": [], "track_ids": []}

        # Construction du dict user → {track → score}
        matrix    = {}
        track_set = set()
        for user_id, track_id, play_count in rows:
            user_id  = str(user_id)
            track_id = str(track_id)
            matrix.setdefault(user_id, {})[track_id] = float(play_count)
            track_set.add(track_id)

        user_ids  = sorted(matrix.keys())
        track_ids = sorted(track_set)

        logger.info(
            "✅ Matrice construite — %d utilisateurs × %d tracks",
            len(user_ids), len(track_ids),
        )
        return {"matrix": matrix, "user_ids": user_ids, "track_ids": track_ids}

    @task(task_id="compute_recommendations")
    def compute_recommendations(matrix_data: dict) -> dict:
        """
        Calcule les recommandations par cosine similarity user-based.

        Algorithme :
        1. Vectoriser la matrice en numpy (lignes = users, colonnes = tracks)
        2. Cosine similarity entre tous les utilisateurs
        3. Pour chaque user : top-N_NEIGHBORS voisins les plus similaires
        4. Score de reco = somme(similarité × écoutes_voisin) pour les tracks
           non encore écoutées par l'utilisateur cible
        5. Retourner le top-N_RECO par utilisateur

        Returns:
            dict {user_id: [track_id, ...]}  (top-10 par user)
        """
        matrix    = matrix_data["matrix"]
        user_ids  = matrix_data["user_ids"]
        track_ids = matrix_data["track_ids"]

        if not user_ids or not track_ids:
            logger.warning("⚠️  Matrice vide — pas de recommandations calculées.")
            return {}

        # 1. Vectorisation numpy : shape (n_users, n_tracks)
        track_index = {tid: i for i, tid in enumerate(track_ids)}
        n_users     = len(user_ids)
        n_tracks    = len(track_ids)
        mat         = np.zeros((n_users, n_tracks), dtype=np.float32)

        for u_idx, user_id in enumerate(user_ids):
            for track_id, score in matrix[user_id].items():
                t_idx = track_index[track_id]
                mat[u_idx, t_idx] = score

        # 2. Cosine similarity numpy pur : cos(u,v) = (u·v) / (||u|| × ||v||)
        #    Normalisation des lignes puis produit matriciel — équivalent sklearn
        norms      = np.linalg.norm(mat, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        mat_normed = mat / norms
        sim_matrix = mat_normed @ mat_normed.T

        recommendations = {}

        for u_idx, user_id in enumerate(user_ids):
            # 3. Top-N_NEIGHBORS voisins (on exclut l'utilisateur lui-même)
            sim_scores  = sim_matrix[u_idx].copy()
            sim_scores[u_idx] = -1.0            # s'exclure soi-même
            neighbor_idxs = np.argsort(sim_scores)[::-1][:TOP_N_NEIGHBORS]

            # Tracks déjà écoutées par cet utilisateur
            already_heard = set(matrix[user_id].keys())

            # 4. Score de recommandation agrégé depuis les voisins
            reco_scores = np.zeros(n_tracks, dtype=np.float32)
            for n_idx in neighbor_idxs:
                similarity = sim_scores[n_idx]
                if similarity <= 0:
                    continue
                neighbor_id = user_ids[n_idx]
                for track_id, play_count in matrix[neighbor_id].items():
                    if track_id not in already_heard:
                        t_idx = track_index[track_id]
                        reco_scores[t_idx] += similarity * play_count

            # 5. Top-N_RECO tracks par score décroissant
            top_indices = np.argsort(reco_scores)[::-1][:TOP_N_RECO]
            top_tracks  = [
                track_ids[i]
                for i in top_indices
                if reco_scores[i] > 0
            ]

            if top_tracks:
                recommendations[user_id] = top_tracks

        logger.info(
            "✅ Recommandations calculées pour %d utilisateurs / %d actifs",
            len(recommendations), n_users,
        )
        return recommendations

    @task(task_id="store_recommendations")
    def store_recommendations(recommendations: dict, **context) -> dict:
        """
        Stocke les recommandations dans Redis (TTL 24h) et PostgreSQL (upsert).

        Redis  : clé `reco:{user_id}` → JSON list de track_ids, TTL REDIS_TTL_SECONDS
        PG     : table `recommendations`, upsert ON CONFLICT (user_id)

        Returns:
            dict {users_stored: int, users_skipped: int}
        """
        if not recommendations:
            logger.warning("⚠️  Aucune recommandation à stocker.")
            return {"users_stored": 0, "users_skipped": 0}

        # ── Redis ─────────────────────────────────────────────
        import redis
        from airflow.hooks.base import BaseHook

        redis_conn = BaseHook.get_connection(REDIS_CONN_ID)
        r = redis.Redis(
            host=redis_conn.host,
            port=redis_conn.port or 6379,
            password=redis_conn.password or None,
            decode_responses=True,
        )

        users_stored  = 0
        users_skipped = 0

        for user_id, track_ids in recommendations.items():
            if not track_ids:
                users_skipped += 1
                continue
            key   = f"reco:{user_id}"
            value = json.dumps(track_ids)
            # setex : SET + EXpire en une commande atomique
            r.setex(name=key, time=REDIS_TTL_SECONDS, value=value)
            users_stored += 1

        logger.info("💾 Redis — %d clés reco:* écrites (TTL=%ds)", users_stored, REDIS_TTL_SECONDS)

        # ── PostgreSQL ────────────────────────────────────────
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn    = pg_hook.get_conn()
        cursor  = conn.cursor()

        execution_date = context["data_interval_start"]

        # Une ligne par (user_id, track_id) avec le score de recommandation
        reco_rows = []
        for user_id, track_ids in recommendations.items():
            for rank, track_id in enumerate(track_ids):
                # Score décroissant : 1.0 pour le premier, puis décrémente
                score = round(1.0 - rank * (1.0 / TOP_N_RECO), 2)
                reco_rows.append((user_id, track_id, score, execution_date))

        cursor.executemany(
            """
            INSERT INTO recommendations (user_id, track_id, score, generated_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (user_id, track_id)
            DO UPDATE SET
                score        = EXCLUDED.score,
                generated_at = EXCLUDED.generated_at
            """,
            reco_rows,
        )
        conn.commit()
        cursor.close()
        conn.close()

        logger.info("💾 PostgreSQL — %d lignes upsertées dans recommendations", len(reco_rows))

        stats = {"users_stored": users_stored, "users_skipped": users_skipped}

        ti = context["ti"]
        ti.xcom_push(key="users_stored",  value=users_stored)
        ti.xcom_push(key="users_skipped", value=users_skipped)

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]
        print(f"""
        ✅ recommendation_pipeline terminé
        DAGRun          : {dag_run.run_id}
        Users stockés   : {stats.get('users_stored', 0)}
        Users sans reco : {stats.get('users_skipped', 0)}
        Redis TTL       : {REDIS_TTL_SECONDS}s (24h)
        """)

    # ── Orchestration ─────────────────────────────────────────
    matrix_data     = build_user_track_matrix()
    recommendations = compute_recommendations(matrix_data)
    stats           = store_recommendations(recommendations)

    wait_for_aggregation >> matrix_data
    notify_success(stats)