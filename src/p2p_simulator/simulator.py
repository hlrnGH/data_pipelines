"""
SPOTIFY — Simulateur P2P
========================
Ce simulateur génère des événements réalistes d'un réseau peer-to-peer
de streaming musical. Il publie dans Redis pub/sub (Phase 1) et dans
Kafka (Phase 2, après décommentage).

Usage :
    python -m src.p2p_simulator.simulator --peers 10 --rate 5
    python -m src.p2p_simulator.simulator --mode fraud --peers 5
    python -m src.p2p_simulator.simulator --mode late_events

TODO Phase 1 :  Compléter _generate_listening_event() et _publish_to_redis()
TODO Phase 2 :  Activer _publish_to_kafka() et le mode fraude
"""

import argparse
import json
import logging
import os
import random
import signal
import time
import uuid
from datetime import datetime, timedelta
from typing import Optional

import redis

# Phase 2 — décommenter quand Kafka est prêt
from confluent_kafka import Producer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s"
)
logger = logging.getLogger("p2p_simulator")


# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:19092")  # hors Docker → listener EXTERNAL de kafka-1

# Connexion PostgreSQL pour charger les vrais track_id du catalogue.
# Le simulateur tourne hors Docker → on passe par localhost:5432.
POSTGRES_DSN = os.getenv(
    "SIMULATOR_POSTGRES_DSN",
    "host=localhost port=5432 dbname=spotify user=spotify password=spotify",
)

TOPICS = {
    "listening":   "listening_events",
    "p2p_network": "p2p_network_events",
}

DEVICE_TYPES = ["mobile", "desktop", "smart_speaker", "web", "tv"]
GEO_COUNTRIES = ["FR", "DE", "US", "GB", "ES", "IT", "BR", "JP", "KR", "AU"]
EVENT_SOURCES = ["p2p", "p2p", "p2p", "direct", "cache"]  # pondéré : 60% P2P


# ─────────────────────────────────────────────────────────────
# DONNÉES SIMULÉES
# ─────────────────────────────────────────────────────────────

# Fallback : si PostgreSQL est vide ou injoignable, on garde des UUID aléatoires
# pour que le simulateur fonctionne quand même (mais les events partiront en DLQ
# côté #6 faute de matcher le catalogue). En usage normal, _load_catalog()
# remplace cette liste par les vrais track_id de la base.
SAMPLE_TRACKS = [
    {"id": str(uuid.uuid4()), "title": f"Track {i}", "duration_ms": random.randint(120000, 300000)}
    for i in range(50)
]


def _load_catalog_from_postgres() -> list:
    """
    Charge les vrais track_id depuis PostgreSQL.

    Pourquoi : le DAG #6 enrichit chaque event en joignant son track_id avec
    la table `tracks`. Si le simulateur invente des UUID aléatoires, aucun ne
    matche → tout part en DLQ (unknown_track). En lisant les vrais IDs ici,
    les events référencent des tracks qui existent vraiment.

    Retourne une liste de dicts {id, title, duration_ms}.
    Si la connexion échoue ou si la base est vide, retourne [] (le caller
    gardera alors les SAMPLE_TRACKS aléatoires en fallback).
    """
    try:
        import psycopg2
    except ImportError:
        logger.warning("psycopg2 absent — catalogue non chargé, fallback UUID aléatoires")
        return []

    try:
        conn = psycopg2.connect(POSTGRES_DSN)
        with conn.cursor() as cur:
            # On limite à 1000 tracks : largement assez pour simuler des écoutes
            cur.execute("SELECT id::text, title, duration_ms FROM tracks LIMIT 1000")
            rows = cur.fetchall()
        conn.close()

        catalog = [
            {"id": row[0], "title": row[1], "duration_ms": int(row[2])}
            for row in rows
        ]
        return catalog

    except Exception as e:
        logger.warning(f"Lecture catalogue PostgreSQL impossible ({e}) — fallback UUID aléatoires")
        return []

SAMPLE_USERS = [str(uuid.uuid4()) for _ in range(200)]
SAMPLE_PEERS = [str(uuid.uuid4()) for _ in range(20)]


# ─────────────────────────────────────────────────────────────
# SIMULATEUR PRINCIPAL
# ─────────────────────────────────────────────────────────────

class P2PSimulator:
    """
    Simulateur du réseau P2P SPOTIFY.

    Génère deux types d'événements :
    - listening_events   : un utilisateur écoute un morceau via un peer
    - p2p_network_events : connexion/déconnexion/transfert entre peers
    """

    def __init__(
        self,
        n_peers: int = 10,
        events_per_second: float = 5.0,
        mode: str = "normal",
    ):
        self.n_peers = n_peers
        self.events_per_second = events_per_second
        self.mode = mode
        self.running = True
        self.event_count = 0

        # Charge les vrais track_id du catalogue PostgreSQL.
        # Si la base est vide/injoignable, on retombe sur les SAMPLE_TRACKS aléatoires.
        catalog = _load_catalog_from_postgres()
        if catalog:
            self.tracks = catalog
            logger.info(f"Catalogue chargé depuis PostgreSQL : {len(catalog)} tracks réels")
        else:
            self.tracks = SAMPLE_TRACKS
            logger.warning("Catalogue PostgreSQL vide — utilisation de track_id aléatoires "
                           "(les events partiront en DLQ côté #6)")

        # Connexion Redis
        self.redis = redis.from_url(REDIS_URL, decode_responses=True)

        # Phase 2 — Kafka producer
        self.kafka_producer = Producer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "acks": "all",
            "enable.idempotence": True,
        })

        # Peers actifs simulés
        self.active_peers = [str(uuid.uuid4()) for _ in range(n_peers)]

        signal.signal(signal.SIGTERM, self._shutdown)
        signal.signal(signal.SIGINT, self._shutdown)

        logger.info(f"Simulateur démarré | mode={mode} | peers={n_peers} | rate={events_per_second} evt/s")

    def run(self):
        """Boucle principale : génère et publie des événements en continu."""
        interval = 1.0 / self.events_per_second

        while self.running:
            try:
                # Alterner listening et réseau P2P (80% / 20%)
                if random.random() < 0.8:
                    event = self._generate_listening_event()
                    self._publish_event("listening", event)
                else:
                    event = self._generate_p2p_network_event()
                    self._publish_event("p2p_network", event)

                self.event_count += 1

                if self.event_count % 100 == 0:
                    logger.info(f"Événements publiés : {self.event_count}")

                time.sleep(interval)

            except Exception as e:
                logger.error(f"Erreur lors de la génération d'événement : {e}")
                time.sleep(1)

    # ── Génération d'événements ──────────────────────────────

    def _generate_listening_event(self) -> dict:
        """
        Génère un événement d'écoute.

        TODO : compléter ce squelette pour générer un événement réaliste.
        Champs attendus :
            - event_id     : UUID unique
            - user_id      : UUID utilisateur (depuis SAMPLE_USERS)
            - track_id     : UUID du morceau (depuis SAMPLE_TRACKS)
            - source_peer  : UUID du peer qui sert le morceau
            - timestamp    : ISO 8601 (datetime.utcnow())
            - duration_ms  : durée écoutée (entre 30 000 et track.duration_ms)
            - device_type  : depuis DEVICE_TYPES
            - geo_country  : depuis GEO_COUNTRIES
            - completed    : bool (True si duration_ms > 30s)
            - event_source : depuis EVENT_SOURCES

        En mode "fraud" (Phase 2) :
            - 30% des events : duration_ms < 5000 (écoute trop courte = bot)
            - 10% : même user_id sur 20 tracks en <10 secondes

        En mode "late_events" (Phase 2) :
            - timestamp décalé de -5 à -30 minutes dans le passé
        """
        track = random.choice(self.tracks)

        # On tire une durée d'écoute réaliste :
        # minimum 30 secondes, maximum = durée totale du morceau
        duration_ms = random.randint(30_000, track["duration_ms"])

        # Un stream est « completed » si l'utilisateur a écouté au moins 30s
        # C'est le seuil utilisé par Spotify pour comptabiliser un stream payant
        completed = duration_ms >= 30_000

        event = {
            "event_id":     str(uuid.uuid4()),
            "user_id":      random.choice(SAMPLE_USERS),
            "track_id":     track["id"],
            "source_peer":  random.choice(self.active_peers),
            "timestamp":    datetime.utcnow().isoformat() + "Z",
            "duration_ms":  duration_ms,
            "device_type":  random.choice(DEVICE_TYPES),
            "geo_country":  random.choice(GEO_COUNTRIES),
            "completed":    completed,
            "event_source": random.choice(EVENT_SOURCES),
        }

        # Mode fraud (Phase 2) — décommenter
        # if self.mode == "fraud" and random.random() < 0.3:
        #     event["duration_ms"] = random.randint(100, 4999)
        #     event["completed"] = False

        # Mode late_events (Phase 2) — décommenter
        if self.mode == "late_events" and random.random() < 0.4:
            delay_minutes = random.randint(5, 30)
            ts = datetime.utcnow() - timedelta(minutes=delay_minutes)
            event["timestamp"] = ts.isoformat() + "Z"

        return event

    def _generate_p2p_network_event(self) -> dict:
        """
        Génère un événement réseau P2P.

        TODO : compléter pour générer des événements de type :
            - peer_connect    : un peer rejoint le réseau
            - peer_disconnect : un peer quitte le réseau
            - chunk_transfer  : transfert d'un chunk audio entre peers
            - cache_hit       : le morceau était en cache local
            - cache_miss      : téléchargement depuis un autre peer nécessaire
        """
        event_type = random.choice([
            "peer_connect", "peer_disconnect",
            "chunk_transfer", "cache_hit", "cache_miss"
        ])

        peer_id = random.choice(self.active_peers)
        base = {
            "event_id":   str(uuid.uuid4()),
            "event_type": event_type,
            "peer_id":    peer_id,
            "timestamp":  datetime.utcnow().isoformat() + "Z",
        }

        if event_type == "peer_connect":
            # Un nouveau peer rejoint le réseau P2P
            # On simule son IP et le nombre de tracks qu'il partage
            extra = {
                "remote_ip":     f"10.{random.randint(0,255)}.{random.randint(0,255)}.{random.randint(1,254)}",
                "shared_tracks": random.randint(50, 500),
            }

        elif event_type == "peer_disconnect":
            # Un peer quitte le réseau
            # session_duration_s = combien de temps il est resté connecté
            extra = {
                "session_duration_s": random.randint(30, 7200),
                "bytes_uploaded":     random.randint(0, 50_000_000),
            }

        elif event_type == "chunk_transfer":
            # Transfert d'un morceau (ou d'un fragment) entre deux peers
            # target_peer ≠ source_peer pour que le transfert ait du sens
            other_peers = [p for p in self.active_peers if p != peer_id]
            target = random.choice(other_peers) if other_peers else peer_id
            extra = {
                "target_peer_id":  target,
                "track_id":        random.choice(self.tracks)["id"],
                "chunk_size_bytes": random.randint(32_768, 262_144),  # 32 KB → 256 KB
                "transfer_ms":     random.randint(10, 500),
            }

        elif event_type == "cache_hit":
            # Le morceau demandé était déjà dans le cache local du peer
            # response_time_ms très faible (lecture locale)
            extra = {
                "track_id":        random.choice(self.tracks)["id"],
                "response_time_ms": random.randint(1, 20),
            }

        else:  # cache_miss
            # Morceau absent du cache → doit être téléchargé depuis un autre peer
            other_peers = [p for p in self.active_peers if p != peer_id]
            fallback = random.choice(other_peers) if other_peers else peer_id
            extra = {
                "track_id":        random.choice(self.tracks)["id"],
                "fallback_peer_id": fallback,
                "response_time_ms": random.randint(50, 800),
            }

        event = {**base, **extra}
        return event

    # ── Publication ──────────────────────────────────────────

    def _publish_event(self, topic_key: str, event: dict):
        """Publie un événement dans Redis et (Phase 2) dans Kafka."""
        payload = json.dumps(event)
        channel = TOPICS[topic_key]

        self._publish_to_redis(channel, payload)
        # Phase 2 — décommenter
        self._publish_to_kafka(channel, event.get("user_id", ""), payload)

    def _publish_to_redis(self, channel: str, payload: str):
        """
        Publie le payload dans Redis de DEUX façons complémentaires :

        1. PUB/SUB (self.redis.publish) : diffusion temps réel. Les abonnés
           connectés au moment de la publication reçoivent l'event. Mais rien
           n'est gardé : un abonné absent rate l'event (= radio en direct).

        2. LIST (self.redis.lpush) : persistance. L'event est empilé dans une
           liste Redis et y reste jusqu'à ce qu'un consommateur le dépile.
           Le DAG batch streaming_events_pipeline (#6) lit cette liste avec
           rpop — il récupère donc TOUS les events accumulés depuis son
           dernier passage, même ceux publiés avant son démarrage.
           lpush + rpop = file FIFO (premier entré, premier sorti).

        On garde un plafond (LTRIM) pour éviter que la liste grossisse à
        l'infini si aucun DAG ne la consomme.
        """
        try:
            self.redis.publish(channel, payload)          # 1. temps réel
            self.redis.lpush(channel, payload)             # 2. persistance pour le batch
            self.redis.ltrim(channel, 0, 99_999)           # garde au max 100 000 events
        except redis.RedisError as e:
            # On ne plante pas le simulateur si Redis est momentanément
            # indisponible : on log et on continue. Les events sont perdus
            # mais le simulateur reste vivant.
            logger.warning(f"Redis indisponible, événement ignoré [{channel}] : {e}")

    def _publish_to_kafka(self, topic: str, key: str, payload: str):
        """
        Publie le payload dans Kafka avec garanties de durabilité.
        - acks=all + idempotence : exactly-once sur le producteur
        - key : user_id pour le partitionnement (tous les events
        d'un même utilisateur → même partition)
        """
        def delivery_report(err, msg):
            if err is not None:
                logger.warning(f"Kafka delivery failed [{topic}] : {err}")
            else:
                logger.debug(f"Kafka delivery OK [{msg.topic()}] partition={msg.partition()}")

        try:
            self.kafka_producer.produce(
                topic,
                key=key.encode("utf-8") if key else None,
                value=payload.encode("utf-8"),
                callback=delivery_report,
            )
            self.kafka_producer.poll(0)
        except Exception as e:
            logger.warning(f"Kafka indisponible, événement ignoré [{topic}] : {e}")



    def _shutdown(self, signum, frame):
        logger.info(f"Arrêt du simulateur (signal {signum}) — {self.event_count} événements publiés")
        self.running = False


# ─────────────────────────────────────────────────────────────
# POINT D'ENTRÉE
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="SPOTIFY P2P Simulator")
    parser.add_argument("--peers",  type=int,   default=10,     help="Nombre de peers simulés")
    parser.add_argument("--rate",   type=float, default=5.0,    help="Événements par seconde")
    parser.add_argument("--mode",   type=str,   default="normal",
                        choices=["normal", "fraud", "late_events", "chaos"],
                        help="Mode de simulation")
    args = parser.parse_args()

    simulator = P2PSimulator(
        n_peers=args.peers,
        events_per_second=args.rate,
        mode=args.mode,
    )
    simulator.run()


if __name__ == "__main__":
    main()
