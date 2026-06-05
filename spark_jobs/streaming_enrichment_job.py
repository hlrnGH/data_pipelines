"""
Spark Job : streaming_enrichment_job
======================================
Enrichit les events d'écoute avec les métadonnées du catalogue
(artiste, genre) et les données P2P réseau.

Outputs :
    - Kafka  → topic `enriched_events` (events enrichis JSON)
    - MinIO  → s3a://spotify-parquet/listening_events/ (Parquet, partitionné date/hour)

Lancement :
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/streaming_enrichment_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP   = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
TOPIC_LISTENING   = "listening_events"
TOPIC_P2P         = "p2p_network_events"
TOPIC_ENRICHED    = "enriched_events"
CHECKPOINT_PATH   = "s3a://spotify-checkpoints/streaming_enrichment"
PARQUET_PATH      = "s3a://spotify-parquet/listening_events"
POSTGRES_URL      = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")
POSTGRES_PROPS    = {
    "user": "spotify",
    "password": "spotify",
    "driver": "org.postgresql.Driver",
}

# ─────────────────────────────────────────────────────────────
# SCHÉMAS
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id",    StringType(),  False),
    StructField("user_id",     StringType(),  False),
    StructField("track_id",    StringType(),  False),
    StructField("source_peer", StringType(),  True),
    StructField("timestamp",   StringType(),  False),
    StructField("duration_ms", IntegerType(), True),
    StructField("device_type", StringType(),  True),
    StructField("geo_country", StringType(),  True),
    StructField("completed",   BooleanType(), True),
    StructField("event_source",StringType(),  True),
])

P2P_EVENT_SCHEMA = StructType([
    StructField("event_id",         StringType(),  False),
    StructField("event_type",       StringType(),  True),
    StructField("peer_id",          StringType(),  True),
    StructField("target_peer",      StringType(),  True),
    StructField("track_id",         StringType(),  True),
    StructField("chunk_size_bytes", IntegerType(), True),
    StructField("latency_ms",       IntegerType(), True),
    StructField("timestamp",        StringType(),  False),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SPOTIFY-streaming-enrichment")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",           "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",         "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",         "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def parse_timestamp(df):
    """Convertit le timestamp ISO en TimestampType et crée event_time."""
    return (
        df.withColumn("ts_clean", F.regexp_replace(F.col("timestamp"), "Z$", ""))
        .withColumn("event_time", F.coalesce(
            F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
        ))
        .drop("ts_clean")
    )


def read_listening_stream(spark: SparkSession):
    """Lit le topic listening_events depuis Kafka."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_LISTENING)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json_payload"))
        .withColumn("event", F.from_json(F.col("json_payload"), LISTENING_EVENT_SCHEMA))
        .select(F.col("event.*"))
    )
    return parse_timestamp(parsed)


def read_p2p_stream(spark: SparkSession):
    """Lit le topic p2p_network_events depuis Kafka."""
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_P2P)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed = (
        raw.select(F.col("value").cast("string").alias("json_payload"))
        .withColumn("event", F.from_json(F.col("json_payload"), P2P_EVENT_SCHEMA))
        .select(F.col("event.*"))
    )
    return parse_timestamp(parsed)


# ─────────────────────────────────────────────────────────────
# ENRICHISSEMENT
# ─────────────────────────────────────────────────────────────

def enrich_with_catalog(listening_df, catalog_df):
    """
    Jointure stream-static : enrichit les events avec artiste et genre.
    catalog_df est un DataFrame statique chargé une fois depuis PostgreSQL.
    """
    return listening_df.join(
        catalog_df,
        listening_df["track_id"] == catalog_df["track_id_cat"],
        how="left",
    ).drop("track_id_cat")


def enrich_with_p2p(listening_df, p2p_df):
    listening_wm = listening_df.withWatermark("event_time", "2 minutes")
    p2p_wm = (
        p2p_df
        .withColumnRenamed("track_id",   "p2p_track_id")
        .withColumnRenamed("event_time", "p2p_event_time")
        .withColumnRenamed("event_id",   "p2p_event_id")
        .withWatermark("p2p_event_time", "2 minutes")
    )

    return listening_wm.join(
        p2p_wm,
        (listening_wm["track_id"] == p2p_wm["p2p_track_id"]) &
        (listening_wm["event_time"] >= p2p_wm["p2p_event_time"] - F.expr("INTERVAL 2 MINUTES")) &
        (listening_wm["event_time"] <= p2p_wm["p2p_event_time"] + F.expr("INTERVAL 2 MINUTES")),
        how="left",
    ).drop("p2p_track_id", "p2p_event_time")


# ─────────────────────────────────────────────────────────────
# ÉCRITURE
# ─────────────────────────────────────────────────────────────

def write_to_kafka(enriched_df):
    """
    Sérialise les events enrichis en JSON et publie dans enriched_events.
    Déduplication par event_id + watermark avant écriture.
    """
    deduped = (
        enriched_df
        .withWatermark("event_time", "10 minutes")
        .dropDuplicates(["event_id"])
    )

    output = deduped.select(
        F.col("event_id").alias("key"),
        F.to_json(F.struct(
            F.col("event_id"),
            F.col("user_id"),
            F.col("track_id"),
            F.col("event_time"),
            F.col("duration_ms"),
            F.col("device_type"),
            F.col("geo_country"),
            F.col("completed"),
            F.col("event_source"),
            F.col("artist_name"),
            F.col("genre"),
            F.col("label"),
        )).alias("value")
    )

    query = (
        output.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("topic", TOPIC_ENRICHED)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/kafka")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def write_to_parquet(enriched_df):
    """
    Écrit les events enrichis en Parquet sur MinIO
    partitionné par date et heure.
    """
    with_partition = enriched_df.select(
        F.col("event_id"),
        F.col("user_id"),
        F.col("track_id"),
        F.col("event_time"),
        F.col("duration_ms"),
        F.col("device_type"),
        F.col("geo_country"),
        F.col("completed"),
        F.col("event_source"),
        F.col("artist_name"),
        F.col("genre"),
        F.col("label"),
        F.to_date(F.col("event_time")).alias("date"),
        F.hour(F.col("event_time")).alias("hour"),
    )

    query = (
        with_partition.writeStream
        .format("parquet")
        .option("path", PARQUET_PATH)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/parquet")
        .partitionBy("date", "hour")
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

    print("Démarrage streaming_enrichment_job...")
    print(f"Kafka : {KAFKA_BOOTSTRAP}")
    print(f"Checkpoint : {CHECKPOINT_PATH}")
    print(f"Parquet : {PARQUET_PATH}")

    # Chargement statique du catalogue PostgreSQL
    try:
        catalog_df = (
            spark.read.jdbc(POSTGRES_URL, "tracks", properties=POSTGRES_PROPS)
            .select(
                F.col("id").alias("track_id_cat"),
                F.col("artist_id"),
                F.col("genre"),
            )
            .join(
                spark.read.jdbc(POSTGRES_URL, "artists", properties=POSTGRES_PROPS)
                .select(
                    F.col("id").alias("artist_id_ref"),
                    F.col("name").alias("artist_name"),
                    F.col("label"),
                ),
                F.col("artist_id") == F.col("artist_id_ref"),
                how="left",
            )
            .drop("artist_id", "artist_id_ref")
        )
        print(f"Catalogue chargé : {catalog_df.count()} tracks")
    except Exception as e:
        print(f"Warning: catalogue indisponible : {e}")
        from pyspark.sql.types import StructType, StructField, StringType
        catalog_df = spark.createDataFrame([], StructType([
            StructField("track_id_cat", StringType(), True),
            StructField("artist_name",  StringType(), True),
            StructField("genre",        StringType(), True),
            StructField("label",        StringType(), True),
        ]))

    # Streams Kafka
    listening_df = read_listening_stream(spark)
    p2p_df       = read_p2p_stream(spark)

    # Enrichissement stream-static (catalogue)
    enriched_catalog = enrich_with_catalog(listening_df, catalog_df)
    query_kafka   = write_to_kafka(enriched_catalog)
    query_parquet = write_to_parquet(enriched_catalog)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()