"""
Spark Job : fraud_detection_job
================================
Détecte les comportements frauduleux en temps réel depuis le topic
Kafka `listening_events` et publie des alertes.

Règles de détection :
    Règle 1 : > 10 écoutes en 10 minutes pour un même user_id
    Règle 2 : durée moyenne < 5 secondes → pattern bot
    Règle 3 : score de suspicion calculé par fenêtre

Outputs :
    - Kafka         → topic `fraud_alerts`
    - PostgreSQL    → table `fraud_detections`
    - PostgreSQL    → table `dead_letter_events` (réutilisation Phase 1)

Lancement :
    spark-submit \\
        --master spark://spark-master:7077 \\
        --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,\\
                   org.postgresql:postgresql:42.7.1 \\
        spark_jobs/fraud_detection_job.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka-1:9092")
INPUT_TOPIC     = "listening_events"
OUTPUT_TOPIC    = "fraud_alerts"
CHECKPOINT_PATH = "s3a://spotify-checkpoints/fraud_detection"
POSTGRES_URL    = os.getenv("SPOTIFY_POSTGRES_URL", "jdbc:postgresql://postgres:5432/spotify")

SHORT_PLAY_MS   = 5000
HIGH_VOLUME     = 10
MIN_SCORE_ALERT = 30

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


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder.appName("SPOTIFY-fraud-detection")
        .config("spark.sql.shuffle.partitions", "6")
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.hadoop.fs.s3a.endpoint",          "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key",        "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def read_listening_stream(spark: SparkSession):
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", INPUT_TOPIC)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )
    return (
        raw.select(F.col("value").cast("string").alias("json_payload"))
        .withColumn("event", F.from_json(F.col("json_payload"), LISTENING_EVENT_SCHEMA))
        .select(F.col("event.*"))
        .withColumn("ts_clean", F.regexp_replace(F.col("timestamp"), "Z$", ""))
        .withColumn("event_time", F.coalesce(
            F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
            F.to_timestamp(F.col("ts_clean"), "yyyy-MM-dd'T'HH:mm:ss"),
        ))
        .drop("ts_clean")
        .filter(F.col("user_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
    )


def detect_fraud(events_df):
    windowed = (
        events_df
        .withWatermark("event_time", "10 minutes")
        .withColumn("is_short", F.when(
            F.col("duration_ms") < SHORT_PLAY_MS, F.lit(1)
        ).otherwise(F.lit(0)))
        .groupBy(
            F.window("event_time", "10 minutes"),
            F.col("user_id"),
        )
        .agg(
            F.count("*").alias("event_count"),
            F.sum("is_short").alias("short_play_count"),
            F.avg("duration_ms").alias("avg_duration_ms"),
            F.approx_count_distinct("track_id").alias("distinct_tracks"),
        )
        .withColumn("window_start", F.col("window.start"))
        .withColumn("window_end",   F.col("window.end"))
        .drop("window")
    )

    return (
        windowed
        .withColumn("score_velocity",   F.when(F.col("event_count") > HIGH_VOLUME, F.lit(40)).otherwise(F.lit(0)))
        .withColumn("score_short_play", F.when(F.col("short_play_count") >= 3, F.lit(40)).otherwise(F.lit(0)))
        .withColumn("score_hopping",    F.when(F.col("distinct_tracks") >= 5, F.lit(20)).otherwise(F.lit(0)))
        .withColumn("suspicion_score",  F.col("score_velocity") + F.col("score_short_play") + F.col("score_hopping"))
        .withColumn("fraud_reason", F.when(
            F.col("score_velocity") > 0, F.lit("high_event_velocity")
        ).when(
            F.col("score_short_play") > 0, F.lit("short_play_bot_pattern")
        ).when(
            F.col("score_hopping") > 0, F.lit("suspicious_track_hopping")
        ).otherwise(F.lit("unknown")))
        .filter(F.col("suspicion_score") >= MIN_SCORE_ALERT)
        .withColumn("alert_id",        F.expr("uuid()"))
        .withColumn("alert_timestamp", F.current_timestamp())
        .select(
            "alert_id", "user_id", "fraud_reason", "suspicion_score",
            "event_count", "short_play_count", "distinct_tracks",
            "avg_duration_ms", "window_start", "window_end", "alert_timestamp",
        )
    )


def write_alerts(alerts_df):
    def process_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        rows = batch_df.collect()
        print(f"[batch {batch_id}] {len(rows)} alertes fraude détectées")

        # 1. Kafka → fraud_alerts
        try:
            kafka_df = batch_df.select(
                F.col("user_id").alias("key"),
                F.to_json(F.struct(
                    "alert_id", "user_id", "fraud_reason", "suspicion_score",
                    "event_count", "short_play_count", "distinct_tracks",
                    "avg_duration_ms", "window_start", "window_end", "alert_timestamp",
                )).alias("value")
            )
            (
                kafka_df.write.format("kafka")
                .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
                .option("topic", OUTPUT_TOPIC)
                .save()
            )
            print(f"[batch {batch_id}] {len(rows)} alertes → Kafka fraud_alerts ✓")
        except Exception as e:
            print(f"[batch {batch_id}] Kafka error : {e}")

        # 2. PostgreSQL → fraud_detections
        try:
            import psycopg2
            from psycopg2.extras import execute_values

            conn = psycopg2.connect(
                host="postgres", port=5432,
                dbname="spotify", user="spotify", password="spotify",
            )
            try:
                records = [
                    (
                        str(r["alert_id"]),
                        str(r["user_id"]),
                        str(r["fraud_reason"]),
                        float(r["suspicion_score"]),
                        str(r["window_start"]),
                        str(r["window_end"]),
                    )
                    for r in rows
                ]
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO fraud_detections
                            (id, user_id, fraud_type, suspicion_score,
                             window_start, window_end, detected_at)
                        VALUES %s
                        ON CONFLICT DO NOTHING
                        """,
                        records,
                        template="(%s, %s, %s, %s, %s, %s, NOW())",
                    )
                conn.commit()
                print(f"[batch {batch_id}] {len(rows)} alertes → PostgreSQL fraud_detections ✓")
            except Exception as e:
                conn.rollback()
                print(f"[batch {batch_id}] PostgreSQL fraud error : {e}")
            finally:
                conn.close()
        except Exception as e:
            print(f"[batch {batch_id}] PostgreSQL connexion error : {e}")

        # 3. PostgreSQL → dead_letter_events (réutilisation Phase 1)
        try:
            import psycopg2
            from psycopg2.extras import Json, execute_values

            conn = psycopg2.connect(
                host="postgres", port=5432,
                dbname="spotify", user="spotify", password="spotify",
            )
            try:
                dlq_records = [
                    (
                        "fraud_detection",
                        Json({
                            "user_id":         str(r["user_id"]),
                            "fraud_reason":    str(r["fraud_reason"]),
                            "suspicion_score": float(r["suspicion_score"]),
                            "event_count":     int(r["event_count"]),
                        }),
                        "fraud_alert",
                        f"Suspicion score: {r['suspicion_score']} - {r['fraud_reason']}",
                    )
                    for r in rows
                ]
                with conn.cursor() as cur:
                    execute_values(
                        cur,
                        """
                        INSERT INTO dead_letter_events
                            (original_topic, payload, error_type, error_message)
                        VALUES %s
                        """,
                        dlq_records,
                        template="(%s, %s, %s, %s)",
                    )
                conn.commit()
                print(f"[batch {batch_id}] {len(rows)} alertes → DLQ dead_letter_events ✓")
            except Exception as e:
                conn.rollback()
                print(f"[batch {batch_id}] DLQ error : {e}")
            finally:
                conn.close()
        except Exception as e:
            print(f"[batch {batch_id}] DLQ connexion error : {e}")

    query = (
        alerts_df.writeStream
        .foreachBatch(process_batch)
        .outputMode("update")
        .option("checkpointLocation", f"{CHECKPOINT_PATH}/main")
        .trigger(processingTime="30 seconds")
        .start()
    )
    return query


def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print("Démarrage fraud_detection_job...")
    print(f"Kafka input  : {KAFKA_BOOTSTRAP} → {INPUT_TOPIC}")
    print(f"Kafka output : {KAFKA_BOOTSTRAP} → {OUTPUT_TOPIC}")
    print(f"Checkpoint   : {CHECKPOINT_PATH}")
    print(f"Seuils       : HIGH_VOLUME={HIGH_VOLUME}, SHORT_PLAY_MS={SHORT_PLAY_MS}")

    events_df = read_listening_stream(spark)
    alerts_df = detect_fraud(events_df)
    query     = write_alerts(alerts_df)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()