"""
DAG : catalog_ingestion_pipeline
=================================
Ingère le catalogue musical depuis les fichiers JSON des labels
stockés dans MinIO et les charge dans PostgreSQL.

Pipeline :
    MinIO labels-raw/*.json
        → extract_from_minio()
        → validate_schema()
        → transform_catalog()
        → load_to_postgres()
        → notify_success()
"""

import json
import os
from datetime import datetime, timedelta
from typing import Any

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import Json


DAG_DOC = """
## catalog_ingestion_pipeline

### Rôle
Ingère les métadonnées musicales depuis les fichiers JSON de 3 labels
stockés dans MinIO.

### Sources
- `s3://labels-raw/sunset_records.json`
- `s3://labels-raw/nightwave_music.json`
- `s3://labels-raw/urban_pulse.json`

### Destinations
- Table `artists`
- Table `albums`
- Table `tracks`
- Table `dead_letter_events` pour les entrées invalides

### Idempotence
Le pipeline est idempotent :
- `artists` : upsert via contrainte unique `(name, label)`
- `albums` : upsert via clé primaire `id`
- `tracks` : upsert via clé primaire `id`

Relancer le DAG plusieurs fois ne crée pas de doublons.
"""

DEFAULT_ARGS = {
    "owner": "spotify-team",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "execution_timeout": timedelta(minutes=30),
}

POSTGRES_CONN_ID = "spotify_postgres"

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = os.getenv("MINIO_BUCKET_LABELS", "labels-raw")

LABEL_FILES = [
    "sunset_records.json",
    "nightwave_music.json",
    "urban_pulse.json",
]


def _insert_dlq(
    payload: dict,
    error_type: str,
    error_message: str,
    original_topic: str | None = None,
) -> None:
    hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    hook.run(
        """
        INSERT INTO dead_letter_events (
            original_topic,
            payload,
            error_type,
            error_message
        )
        VALUES (%s, %s, %s, %s)
        """,
        parameters=(
            original_topic,
            Json(payload),
            error_type,
            error_message,
        ),
    )


def _has_required_fields(item: dict, required_fields: list[str]) -> tuple[bool, list[str]]:
    missing = [field for field in required_fields if field not in item or item[field] in (None, "")]
    return len(missing) == 0, missing


with DAG(
    dag_id="catalog_ingestion_pipeline",
    default_args=DEFAULT_ARGS,
    description="Ingestion quotidienne du catalogue musical depuis MinIO vers PostgreSQL",
    schedule_interval="0 2 * * *",
    catchup=True,
    max_active_runs=1,
    tags=["spotify", "phase-1", "ingestion", "catalogue"],
    doc_md=DAG_DOC,
) as dag:

    @task(task_id="extract_from_minio")
    def extract_from_minio(**context) -> list[dict]:
        """
        Télécharge les fichiers JSON des labels depuis MinIO.
        """
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_ACCESS_KEY,
            aws_secret_access_key=MINIO_SECRET_KEY,
        )

        catalogs = []

        for filename in LABEL_FILES:
            try:
                obj = s3.get_object(Bucket=MINIO_BUCKET, Key=filename)
                content = obj["Body"].read().decode("utf-8")
                catalog = json.loads(content)
                catalog["_source_file"] = filename
                catalogs.append(catalog)

                print(
                    f"Catalogue extrait : {filename} "
                    f"artists={len(catalog.get('artists', []))}, "
                    f"albums={len(catalog.get('albums', []))}, "
                    f"tracks={len(catalog.get('tracks', []))}"
                )

            except Exception as exc:
                _insert_dlq(
                    payload={"file": filename},
                    error_type="minio_extract_error",
                    error_message=str(exc),
                    original_topic="catalog_ingestion",
                )
                print(f"Warning : impossible de lire {filename}: {exc}")

        return catalogs

    @task(task_id="validate_schema")
    def validate_schema(raw_catalogs: list[dict]) -> dict:
        """
        Valide le schéma des artistes, albums et tracks.
        Les entrées invalides partent en DLQ.
        """
        valid = {
            "artists": [],
            "albums": [],
            "tracks": [],
        }

        errors_count = 0

        required_artist = ["id", "name", "label"]
        required_album = ["id", "artist_id", "title"]
        required_track = ["id", "artist_id", "title", "duration_ms"]

        for catalog in raw_catalogs:
            source_file = catalog.get("_source_file", "unknown")

            for artist in catalog.get("artists", []):
                ok, missing = _has_required_fields(artist, required_artist)
                if ok:
                    valid["artists"].append(artist)
                else:
                    errors_count += 1
                    _insert_dlq(
                        payload=artist,
                        error_type="schema_validation",
                        error_message=f"artist champs manquants: {missing}",
                        original_topic=source_file,
                    )

            for album in catalog.get("albums", []):
                ok, missing = _has_required_fields(album, required_album)
                if ok:
                    valid["albums"].append(album)
                else:
                    errors_count += 1
                    _insert_dlq(
                        payload=album,
                        error_type="schema_validation",
                        error_message=f"album champs manquants: {missing}",
                        original_topic=source_file,
                    )

            for track in catalog.get("tracks", []):
                ok, missing = _has_required_fields(track, required_track)
                if ok:
                    valid["tracks"].append(track)
                else:
                    errors_count += 1
                    _insert_dlq(
                        payload=track,
                        error_type="schema_validation",
                        error_message=f"track champs manquants: {missing}",
                        original_topic=source_file,
                    )

        print(
            "Validation catalogue terminée — "
            f"artists={len(valid['artists'])}, "
            f"albums={len(valid['albums'])}, "
            f"tracks={len(valid['tracks'])}, "
            f"errors={errors_count}"
        )

        return {
            "valid": valid,
            "errors_count": errors_count,
        }

    @task(task_id="transform_catalog")
    def transform_catalog(validated: dict) -> dict:
        """
        Normalise et prépare les données pour insertion PostgreSQL.
        """
        valid = validated["valid"]

        artists_by_id = {}
        albums_by_id = {}
        tracks_by_id = {}

        for artist in valid.get("artists", []):
            artist_id = artist["id"]

            name = str(artist["name"]).strip()
            label = str(artist.get("label") or "").strip()
            genres = artist.get("genres") or []

            if isinstance(genres, str):
                genres = [genres]

            artists_by_id[artist_id] = {
                "id": artist_id,
                "name": name,
                "country": artist.get("country"),
                "label": label,
                "genres": [str(g).strip() for g in genres if g],
                "monthly_listeners": int(artist.get("monthly_listeners") or 0),
            }

        for album in valid.get("albums", []):
            album_id = album["id"]
            artist_id = album["artist_id"]

            if artist_id not in artists_by_id:
                _insert_dlq(
                    payload=album,
                    error_type="unknown_artist",
                    error_message=f"artist_id absent des artistes valides: {artist_id}",
                    original_topic="catalog_ingestion",
                )
                continue

            albums_by_id[album_id] = {
                "id": album_id,
                "artist_id": artist_id,
                "title": str(album["title"]).strip(),
                "release_year": album.get("release_year"),
                "total_tracks": album.get("total_tracks"),
            }

        for track in valid.get("tracks", []):
            track_id = track["id"]
            artist_id = track["artist_id"]
            album_id = track.get("album_id")

            if artist_id not in artists_by_id:
                _insert_dlq(
                    payload=track,
                    error_type="unknown_artist",
                    error_message=f"artist_id absent des artistes valides: {artist_id}",
                    original_topic="catalog_ingestion",
                )
                continue

            if album_id and album_id not in albums_by_id:
                _insert_dlq(
                    payload=track,
                    error_type="unknown_album",
                    error_message=f"album_id absent des albums valides: {album_id}",
                    original_topic="catalog_ingestion",
                )
                continue

            duration_ms = int(track["duration_ms"])
            if duration_ms <= 0 or duration_ms > 3_600_000:
                _insert_dlq(
                    payload=track,
                    error_type="invalid_duration",
                    error_message=f"duration_ms invalide: {duration_ms}",
                    original_topic="catalog_ingestion",
                )
                continue

            tracks_by_id[track_id] = {
                "id": track_id,
                "album_id": album_id,
                "artist_id": artist_id,
                "title": str(track["title"]).strip(),
                "duration_ms": duration_ms,
                "genre": track.get("genre"),
                "bpm": track.get("bpm"),
                "explicit": bool(track.get("explicit", False)),
                "audio_file_path": track.get("audio_file_path"),
            }

        transformed = {
            "artists": list(artists_by_id.values()),
            "albums": list(albums_by_id.values()),
            "tracks": list(tracks_by_id.values()),
            "errors_count": validated.get("errors_count", 0),
        }

        print(
            "Transformation terminée — "
            f"artists={len(transformed['artists'])}, "
            f"albums={len(transformed['albums'])}, "
            f"tracks={len(transformed['tracks'])}"
        )

        return transformed

    @task(task_id="load_to_postgres")
    def load_to_postgres(transformed: dict, **context) -> dict:
        """
        Charge le catalogue dans PostgreSQL avec upsert idempotent.
        """
        hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = hook.get_conn()

        artists = transformed.get("artists", [])
        albums = transformed.get("albums", [])
        tracks = transformed.get("tracks", [])

        artist_sql = """
            INSERT INTO artists (
                id,
                name,
                country,
                label,
                genres,
                monthly_listeners
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (name, label) DO UPDATE SET
                country = EXCLUDED.country,
                genres = EXCLUDED.genres,
                monthly_listeners = EXCLUDED.monthly_listeners,
                updated_at = NOW()
        """

        album_sql = """
            INSERT INTO albums (
                id,
                artist_id,
                title,
                release_year,
                total_tracks
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                artist_id = EXCLUDED.artist_id,
                title = EXCLUDED.title,
                release_year = EXCLUDED.release_year,
                total_tracks = EXCLUDED.total_tracks
        """

        track_sql = """
            INSERT INTO tracks (
                id,
                album_id,
                artist_id,
                title,
                duration_ms,
                genre,
                bpm,
                explicit,
                audio_file_path
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (id) DO UPDATE SET
                album_id = EXCLUDED.album_id,
                artist_id = EXCLUDED.artist_id,
                title = EXCLUDED.title,
                duration_ms = EXCLUDED.duration_ms,
                genre = EXCLUDED.genre,
                bpm = EXCLUDED.bpm,
                explicit = EXCLUDED.explicit,
                audio_file_path = EXCLUDED.audio_file_path,
                updated_at = NOW()
        """

        artist_rows = [
            (
                artist["id"],
                artist["name"],
                artist.get("country"),
                artist.get("label"),
                artist.get("genres"),
                artist.get("monthly_listeners", 0),
            )
            for artist in artists
        ]

        album_rows = [
            (
                album["id"],
                album["artist_id"],
                album["title"],
                album.get("release_year"),
                album.get("total_tracks"),
            )
            for album in albums
        ]

        track_rows = [
            (
                track["id"],
                track.get("album_id"),
                track["artist_id"],
                track["title"],
                track["duration_ms"],
                track.get("genre"),
                track.get("bpm"),
                track.get("explicit", False),
                track.get("audio_file_path"),
            )
            for track in tracks
        ]

        with conn.cursor() as cursor:
            cursor.executemany(artist_sql, artist_rows)
            cursor.executemany(album_sql, album_rows)
            cursor.executemany(track_sql, track_rows)

        conn.commit()

        stats = {
            "artists_inserted": len(artist_rows),
            "albums_inserted": len(album_rows),
            "tracks_inserted": len(track_rows),
            "errors_count": transformed.get("errors_count", 0),
        }

        context["ti"].xcom_push(key="tracks_inserted", value=stats["tracks_inserted"])
        context["ti"].xcom_push(key="errors_count", value=stats["errors_count"])

        print(f"Chargement PostgreSQL terminé : {stats}")

        return stats

    @task(task_id="notify_success")
    def notify_success(stats: dict, **context):
        dag_run = context["dag_run"]

        print(f"""
        ✅ catalog_ingestion_pipeline terminé
        DAGRun : {dag_run.run_id}
        Tracks traitées   : {stats.get('tracks_inserted', 0)}
        Albums traités    : {stats.get('albums_inserted', 0)}
        Artists traités   : {stats.get('artists_inserted', 0)}
        Erreurs DLQ       : {stats.get('errors_count', 0)}
        """)

    raw = extract_from_minio()
    validated = validate_schema(raw)
    transformed = transform_catalog(validated)
    stats = load_to_postgres(transformed)
    notify_success(stats)
