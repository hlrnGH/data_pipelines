"""
Spark Job : streaming_trends_job
==================================
Consomme le topic Kafka `listening_events` et produit en continu
les tendances musicales temps réel.

Outputs :
    - PostgreSQL → table `realtime_top_tracks` (top 10 par fenêtre de 5 min)
    - Redis      → clé `top_tracks:live` (top genres par sliding window)

Lancement :
    spark-submit \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_trends_job.py

TODO :
    [ ] Implémenter la lecture du topic Kafka avec readStream
    [ ] Désérialiser les messages JSON avec le bon schéma
    [ ] Implémenter les fenêtres tumbling de 5 minutes
    [ ] Implémenter les sliding windows pour les genres (15 min / 5 min)
    [ ] Configurer le checkpoint sur MinIO
    [ ] Écrire les résultats dans PostgreSQL et Redis
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, BooleanType, TimestampType
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP  = os.getenv("KAFKA_BOOTSTRAP",  "kafka-1:9092")
KAFKA_TOPIC      = "listening_events"
CHECKPOINT_PATH  = "s3a://spotify-checkpoints/streaming_trends"
POSTGRES_URL     = os.getenv("SPOTIFY_POSTGRES_URL",
                             "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS   = {
    "user":   "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),    False),
    StructField("user_id",     StringType(),    False),
    StructField("track_id",    StringType(),    False),
    StructField("source_peer", StringType(),    True),
    StructField("timestamp",   StringType(),    False),  # ISO 8601 → à caster en Timestamp
    StructField("duration_ms", IntegerType(),   True),
    StructField("device_type", StringType(),    True),
    StructField("geo_country", StringType(),    True),
    StructField("completed",   BooleanType(),   True),
    StructField("event_source",StringType(),    True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    """
    Crée et configure la SparkSession avec les dépendances nécessaires.

    TODO : vérifier que les packages kafka et postgresql sont disponibles
    """
    return (
        SparkSession.builder
        .appName("SPOTIFY-streaming-trends")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint",             "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",           "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",    "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_kafka_stream(spark: SparkSession):
    """
    Lit le topic Kafka `listening_events` en streaming et retourne un
    DataFrame avec des colonnes typées, prêt pour les fenêtres temporelles.

    Étapes :
        1. readStream sur Kafka
        2. value (octets) → string
        3. JSON → colonnes via from_json + LISTENING_EVENT_SCHEMA
        4. timestamp ISO (string) → TimestampType, renommé event_time
    """
    # 1. Lecture brute du topic Kafka en streaming.
    #    Chaque message Kafka a des colonnes techniques : key, value, topic,
    #    partition, offset, timestamp... La donnée utile est dans "value" (octets).
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", KAFKA_TOPIC)
        # earliest = on relit tout depuis le début du topic au 1er démarrage.
        # Ensuite, le checkpoint prend le relais pour reprendre où on s'est arrêté.
        .option("startingOffsets", "earliest")
        .load()
    )

    # 2. La colonne "value" est en octets → on la convertit en texte JSON.
    # 3. On parse ce JSON selon le schéma défini plus haut, ce qui éclate
    #    le JSON en vraies colonnes (track_id, user_id, timestamp...).
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(F.from_json(F.col("json_str"), LISTENING_EVENT_SCHEMA).alias("data"))
        .select("data.*")
    )

    # 4. Le champ timestamp est une string ISO 8601 ("2026-06-02T14:30:00Z").
    #    Spark a besoin d'un vrai type Timestamp pour faire des fenêtres
    #    temporelles. On le caste et on le renomme event_time (convention
    #    Spark Streaming pour la colonne de temps événementiel).
    events = parsed.withColumn(
        "event_time",
        F.to_timestamp(F.col("timestamp"))
    )

    return events


# ─────────────────────────────────────────────────────────────
# AGRÉGATIONS STREAMING
# ─────────────────────────────────────────────────────────────

def compute_top_tracks_tumbling(events_df):
    """
    Top tracks par tumbling window (fenêtre fixe) de 5 minutes.

    Une tumbling window découpe le temps en tranches qui ne se chevauchent
    pas : [10h00-10h05], [10h05-10h10]... Chaque stream est compté dans
    une seule tranche.

    Écriture dans PostgreSQL via foreachBatch : on ne peut pas faire
    df.write.jdbc() directement sur un stream (Spark ne sait pas quand
    "finir" d'écrire un flux infini). foreachBatch découpe le flux en
    micro-batches finis, et sur chaque batch on peut écrire normalement.
    """
    # Watermark MINIMAL (la gestion fine des late events = ticket #15).
    # Le watermark dit à Spark : "attends les events jusqu'à 2 min de retard,
    # au-delà ferme la fenêtre". Sans ça, Spark garderait l'état de toutes
    # les fenêtres en mémoire à l'infini.
    aggregated = (
        events_df
        .withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window(F.col("event_time"), "5 minutes"),
            F.col("track_id"),
        )
        .agg(
            F.count("*").alias("stream_count"),
            F.countDistinct("user_id").alias("unique_listeners"),
        )
        # On aplatit la structure window {start, end} en deux colonnes
        # pour qu'elles rentrent dans les colonnes de la table PostgreSQL.
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("track_id"),
            F.col("stream_count"),
            F.col("unique_listeners"),
        )
    )

    def write_batch_to_postgres(batch_df, batch_id):
        """
        Appelé à chaque micro-batch. batch_df est un DataFrame fini
        (statique) → on peut le traiter normalement.

        Écriture idempotente : la clé primaire de realtime_top_tracks est
        (window_start, track_id). Une même fenêtre peut être réécrite sur
        plusieurs batches (outputMode update) → un simple append violerait
        la contrainte d'unicité. On fait donc un UPSERT via psycopg2 :
        INSERT ... ON CONFLICT (window_start, track_id) DO UPDATE.
        """
        import psycopg2

        # Top 10 du batch courant, trié par nombre de streams
        top10 = (
            batch_df
            .orderBy(F.col("stream_count").desc())
            .limit(10)
            .collect()
        )
        if not top10:
            return

        conn = psycopg2.connect(
            host="postgres", port=5432, dbname="spotify",
            user="spotify", password="spotify",
        )
        cur = conn.cursor()
        upsert = """
            INSERT INTO realtime_top_tracks
                (window_start, window_end, track_id, stream_count, unique_listeners, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (window_start, track_id) DO UPDATE SET
                window_end       = EXCLUDED.window_end,
                stream_count     = EXCLUDED.stream_count,
                unique_listeners = EXCLUDED.unique_listeners,
                updated_at       = NOW()
        """
        for row in top10:
            cur.execute(upsert, (
                row["window_start"],
                row["window_end"],
                row["track_id"],
                row["stream_count"],
                row["unique_listeners"],
            ))
        conn.commit()
        cur.close()
        conn.close()
        print(f"[batch {batch_id}] {len(top10)} lignes upsertées dans realtime_top_tracks")

    query = (
        aggregated.writeStream
        .outputMode("update")          # on met à jour les fenêtres au fil de l'eau
        .foreachBatch(write_batch_to_postgres)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/top_tracks")
        .trigger(processingTime="30 seconds")   # un micro-batch toutes les 30s
        .start()
    )
    return query

def compute_genre_listeners_sliding(events_df, catalog_df):
    """
    Auditeurs uniques par genre en sliding window (15 min glissant toutes les 5 min).

    Sliding window : fenêtre large de 15 min recalculée tous les 5 min. Les
    fenêtres se chevauchent → à tout instant on voit les 15 dernières minutes,
    ce qui donne une tendance lissée (pas de retombée brutale à zéro).

    Jointure stream-static : on joint le flux d'events avec le catalogue
    PostgreSQL (chargé une fois comme DataFrame statique) pour récupérer le
    genre de chaque track. Spark sait faire ce type de jointure flux×statique.
    """
    # Jointure : chaque event reçoit le genre de sa track via track_id.
    # catalog_df vient de PostgreSQL (table tracks) chargé en statique.
    enriched = events_df.join(
        catalog_df,
        events_df["track_id"] == catalog_df["id"],
        how="inner",
    )

    aggregated = (
        enriched
        .withWatermark("event_time", "2 minutes")   # watermark minimal (#15 affinera)
        .groupBy(
            # 2 arguments = sliding : taille de fenêtre puis pas de glissement
            F.window(F.col("event_time"), "15 minutes", "5 minutes"),
            F.col("genre"),
        )
        .agg(
            F.countDistinct("user_id").alias("unique_listeners"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("genre"),
            F.col("unique_listeners"),
        )
    )

    def write_batch_to_redis(batch_df, batch_id):
        """
        Écrit le snapshot des genres dans Redis sous la clé
        genre_listeners:live. On utilise redis-py dans le batch.
        """
        import redis
        import json

        # On ne garde que la fenêtre la plus récente pour le "live"
        rows = (
            batch_df
            .orderBy(F.col("window_end").desc(), F.col("unique_listeners").desc())
            .limit(50)
            .collect()
        )
        if not rows:
            return

        # Connexion Redis (le job Spark tourne dans le réseau Docker → host "redis")
        r = redis.Redis(host="redis", port=6379, db=1, decode_responses=True)
        payload = [
            {
                "genre": row["genre"],
                "unique_listeners": row["unique_listeners"],
                "window_end": str(row["window_end"]),
            }
            for row in rows
        ]
        # SETEX : la clé expire après 5 min (TTL) car c'est du "live",
        # elle est rafraîchie à chaque batch.
        r.setex("genre_listeners:live", 300, json.dumps(payload))
        print(f"[batch {batch_id}] {len(payload)} genres écrits dans Redis (genre_listeners:live)")

    query = (
        aggregated.writeStream
        .outputMode("update")
        .foreachBatch(write_batch_to_redis)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/genre_listeners")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage streaming_trends_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP} → topic : {KAFKA_TOPIC}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")

    # Lecture Kafka
    events_df = read_kafka_stream(spark)

    # Chargement du catalogue comme DataFrame STATIQUE (lu une fois).
    # On ne garde que les colonnes utiles pour la jointure : id + genre.
    catalog_df = (
        spark.read
        .jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
        .select("id", "genre")
    )

    # Agrégations
    query_top_tracks = compute_top_tracks_tumbling(events_df)
    query_genres     = compute_genre_listeners_sliding(events_df, catalog_df)

    # Attendre l'arrêt gracieux
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
