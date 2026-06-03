"""
Spark Job : fraud_detection_job
================================
Détecte des comportements frauduleux en streaming à partir du topic Kafka
`listening_events` et publie des alertes dans `fraud_alerts`.

Version #18 :
    - détection comportement utilisateur basée sur `listening_events`
    - stateful streaming via watermark + fenêtres temporelles par user_id
    - sortie Kafka `fraud_alerts`
    - sortie console pour validation locale

Évolution possible après #17 :
    Si `enriched_events` est disponible, le job pourra être enrichi avec des règles
    liées au catalogue : genre, artiste, album, label, etc.
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    BooleanType,
)


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
INPUT_TOPIC = os.getenv("FRAUD_INPUT_TOPIC", "listening_events")
OUTPUT_TOPIC = os.getenv("FRAUD_OUTPUT_TOPIC", "fraud_alerts")
CHECKPOINT_PATH = os.getenv(
    "FRAUD_CHECKPOINT_PATH",
    "s3a://spotify-checkpoints/fraud_detection",
)

SHORT_PLAY_THRESHOLD_MS = int(os.getenv("SHORT_PLAY_THRESHOLD_MS", "5000"))
MIN_EVENTS_FOR_ALERT = int(os.getenv("MIN_EVENTS_FOR_ALERT", "5"))
MIN_DISTINCT_TRACKS_FOR_ALERT = int(os.getenv("MIN_DISTINCT_TRACKS_FOR_ALERT", "4"))


# ─────────────────────────────────────────────────────────────
# SCHÉMA DES ÉVÉNEMENTS D'ÉCOUTE
# ─────────────────────────────────────────────────────────────

LISTENING_EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), False),
    StructField("user_id", StringType(), False),
    StructField("track_id", StringType(), False),
    StructField("source_peer", StringType(), True),
    StructField("timestamp", StringType(), False),
    StructField("duration_ms", IntegerType(), True),
    StructField("device_type", StringType(), True),
    StructField("geo_country", StringType(), True),
    StructField("completed", BooleanType(), True),
    StructField("event_source", StringType(), True),
])


# ─────────────────────────────────────────────────────────────
# INITIALISATION SPARK
# ─────────────────────────────────────────────────────────────

def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("SPOTIFY-fraud-detection")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        # MinIO / S3A
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


# ─────────────────────────────────────────────────────────────
# LECTURE KAFKA
# ─────────────────────────────────────────────────────────────

def read_listening_events(spark: SparkSession):
    kafka_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    events_df = (
        kafka_df
        .select(
            F.col("key").cast("string").alias("kafka_key"),
            F.col("value").cast("string").alias("json_payload"),
            F.col("topic").alias("kafka_topic"),
            F.col("partition").alias("kafka_partition"),
            F.col("offset").alias("kafka_offset"),
            F.col("timestamp").alias("kafka_timestamp"),
        )
        .withColumn("event", F.from_json(F.col("json_payload"), LISTENING_EVENT_SCHEMA))
        .select(
            "kafka_key",
            "json_payload",
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "kafka_timestamp",
            F.col("event.*"),
        )
        .withColumn(
            "event_time",
            F.to_timestamp(
                F.regexp_replace(F.col("timestamp"), "Z$", ""),
                "yyyy-MM-dd'T'HH:mm:ss.SSSSSS",
            )
        )
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
    )

    return events_df


# ─────────────────────────────────────────────────────────────
# DÉTECTION FRAUDE STATEFUL
# ─────────────────────────────────────────────────────────────

def detect_fraud(events_df):
    """
    Détection stateful basée sur watermark + fenêtres temporelles.

    Règles :
        - short_play_count : nombre d'écoutes < 5 secondes
        - event_count : volume d'événements par user sur 30 secondes
        - distinct_tracks : nombre de tracks différentes par user sur 30 secondes
    """
    windowed_df = (
        events_df
        .withWatermark("event_time", "2 minutes")
        .withColumn(
            "is_short_play",
            F.when(F.col("duration_ms") < SHORT_PLAY_THRESHOLD_MS, F.lit(1)).otherwise(F.lit(0))
        )
        .groupBy(
            F.window("event_time", "30 seconds"),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("is_short_play").alias("short_play_count"),
            F.approx_count_distinct("track_id").alias("distinct_tracks"),
            F.min("event_time").alias("first_event_time"),
            F.max("event_time").alias("last_event_time"),
        )
    )

    scored_df = (
        windowed_df
        .withColumn(
            "fraud_reason",
            F.when(
                F.col("short_play_count") >= 3,
                F.lit("short_play_bot_pattern")
            ).when(
                F.col("event_count") >= MIN_EVENTS_FOR_ALERT,
                F.lit("high_event_velocity")
            ).when(
                F.col("distinct_tracks") >= MIN_DISTINCT_TRACKS_FOR_ALERT,
                F.lit("suspicious_track_hopping")
            )
        )
        .filter(F.col("fraud_reason").isNotNull())
        .withColumn(
            "risk_score",
            F.least(
                F.lit(100),
                (F.col("short_play_count") * 25)
                + (F.col("event_count") * 8)
                + (F.col("distinct_tracks") * 5)
            )
        )
        .withColumn("alert_id", F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end", F.col("window.end"))
        .select(
            "alert_id",
            "user_id",
            "fraud_reason",
            "risk_score",
            "event_count",
            "short_play_count",
            "distinct_tracks",
            "first_event_time",
            "last_event_time",
            "window_start",
            "window_end",
            "alert_timestamp",
        )
    )

    return scored_df


def prepare_alerts_for_kafka(alerts_df):
    return (
        alerts_df
        .select(
            F.col("user_id").cast("string").alias("key"),
            F.to_json(
                F.struct(
                    "alert_id",
                    "user_id",
                    "fraud_reason",
                    "risk_score",
                    "event_count",
                    "short_play_count",
                    "distinct_tracks",
                    "first_event_time",
                    "last_event_time",
                    "window_start",
                    "window_end",
                    "alert_timestamp",
                )
            ).alias("value")
        )
    )


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage fraud_detection_job...")
    print(f"Kafka input  : {KAFKA_BOOTSTRAP} → topic : {INPUT_TOPIC}")
    print(f"Kafka output : {KAFKA_BOOTSTRAP} → topic : {OUTPUT_TOPIC}")
    print(f"Checkpoint   : {CHECKPOINT_PATH}")

    events_df = read_listening_events(spark)
    alerts_df = detect_fraud(events_df)
    kafka_alerts_df = prepare_alerts_for_kafka(alerts_df)

    console_query = (
        alerts_df
        .writeStream
        .format("console")
        .outputMode("update")
        .option("truncate", "false")
        .option("numRows", 20)
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/console")
        .start()
    )

    def write_alerts_to_kafka(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        (
            batch_df
            .select(
                F.col("key").cast("string"),
                F.col("value").cast("string"),
            )
            .write
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
            .option("topic", OUTPUT_TOPIC)
            .save()
        )

    kafka_query = (
        kafka_alerts_df
        .writeStream
        .foreachBatch(write_alerts_to_kafka)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/kafka")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
