# RUNBOOK SPOTIFY — Procédures incidents

> Ce document doit être complété par votre groupe au fur et à mesure de la semaine.
> Un bon runbook = ce dont vous auriez eu besoin pendant la panne.


---

## Incidents Phase 1 — Airflow / Batch

### INC-01 — DAG bloqué en "running" depuis > 30 minutes

**Symptômes :** Une tâche reste en état `running` dans l'UI Airflow.

**Diagnostic :**
```bash
# Voir les logs de la tâche
docker compose logs airflow-worker -f

# Lister les tâches actives
docker exec airflow-scheduler airflow tasks states-for-dag-run <dag_id> <run_id>
```

**Résolution :**
```bash
# Marquer la tâche comme failed manuellement
docker exec airflow-scheduler airflow tasks clear <dag_id> -t <task_id> --yes

# Ou tuer le worker et le relancer
docker compose restart airflow-worker
```

**Cause probable :** `aggregation_pipeline` utilise un `ExternalTaskSensor` qui attend que `streaming_events_pipeline` ait tourné avec succès. Si `streaming_events` n'est pas activé ou n'a pas encore de run réussi, le sensor attend indéfiniment. **Solution : toujours activer les DAGs dans l'ordre suivant :** `catalog_ingestion` → `streaming_events` → `aggregation` → `recommendation` → `dlq_reprocessing`.

---

### INC-02 — PostgreSQL : `too many connections`

**Symptômes :** Les tâches Airflow échouent avec `FATAL: too many connections`.

**Diagnostic :**
```sql
SELECT count(*), state FROM pg_stat_activity GROUP BY state;
SELECT max_conn FROM pg_settings WHERE name='max_connections';
```

**Résolution :**
```bash
# Augmenter max_connections dans docker-compose
# PostgreSQL environment: POSTGRES_MAX_CONNECTIONS: 200

# Court terme : killer les connexions idle
# SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE state='idle';
```

**Prévention :** Configurer les **Airflow Pools** (Admin → Pools dans l'UI) pour limiter la concurrence des tâches qui touchent PostgreSQL. Exemple : créer un pool `postgres_pool` avec 5 slots et assigner les tâches `load_to_postgres` à ce pool. Cela évite que plusieurs DAGs ouvrent trop de connexions simultanées.

---

### INC-03 — MinIO inaccessible depuis Airflow

**Symptômes :** Les tâches de lecture/écriture Parquet échouent avec `Connection refused`.

**Diagnostic :**
```bash
docker compose ps minio
curl http://localhost:9000/minio/health/live
```

**Résolution :**
```bash
docker compose restart minio
# Attendre 10s puis relancer le DAGRun
```

---

### INC-07 — Docker mount échoue au démarrage

**Symptômes :** `error while creating mount source path: operation not permitted`

**Cause :** Le projet est dans un dossier dont le chemin contient des espaces (ex: `Desktop/Data Pipelines Production/`). Docker sur Mac ne supporte pas les espaces dans les chemins de mount.

**Résolution :**
```bash
docker compose down
mv ~/Desktop/Data\ Pipelines\ Production/cours_hetic/data_pipelines ~/data_pipelines
cd ~/data_pipelines
docker compose up -d
```

**Prévention :** Toujours travailler dans un dossier sans espace dans le chemin.

---

### INC-08 — Simulateur P2P : les events n'arrivent jamais dans PostgreSQL

**Symptômes :** Le simulateur tourne et publie des events, mais `SELECT COUNT(*) FROM listening_events` reste à 0 après le run de `streaming_events_pipeline`.

**Diagnostic :**
```bash
# Vérifier si la liste Redis se remplit
docker exec -it data_pipelines-redis-1 redis-cli -n 1 LLEN listening_events
```

**Cause :** Le simulateur publiait uniquement en Redis **pub/sub** (`redis.publish`). Le pub/sub ne conserve rien : si aucun consommateur n'écoute au moment exact de la publication, l'event est perdu. Or `streaming_events_pipeline` est un batch qui ne lit que quelques secondes toutes les 5 min → il ratait la quasi-totalité des events.

**Résolution :** Ajouter une publication dans une **liste Redis** persistante en plus du pub/sub :
```python
self.redis.publish(channel, payload)   # temps réel
self.redis.lpush(channel, payload)     # persistance (lu par le DAG via rpop)
self.redis.ltrim(channel, 0, 99_999)   # plafond anti-débordement
```

**Prévention :** Pour qu'un batch consomme des events, toujours les stocker dans une structure persistante (liste Redis, Kafka), jamais en pub/sub seul.

---

### INC-09 — Spark ne résout pas le hostname PostgreSQL

**Symptômes :** `java.net.UnknownHostException: postgres` au démarrage du job Spark.

**Cause :** Le conteneur Spark (bitnamilegacy) ne résout pas les hostnames Docker
même s'il est sur le même réseau que PostgreSQL.

**Résolution :**
Ajouter `extra_hosts` dans `docker-compose.yml` pour les services Spark :
```yaml
spark-master:
  extra_hosts:
    - "postgres:172.19.0.5"

spark-worker-1:
  extra_hosts:
    - "postgres:172.19.0.5"
```
Attention : l'IP peut changer après `docker compose down/up`.
Vérifier avec : `docker inspect data_pipelines-postgres-1 | grep IPAddress`

**Prévention :** Utiliser un réseau Docker explicite avec des IPs statiques
pour les services critiques.
---

### INC-10 — Nouveau DAGRun bloqué en "queued" indéfiniment

**Symptômes :** Un run reste en `queued`, ne démarre jamais, et bloque tous les runs suivants.

**Diagnostic :**
```bash
docker exec data_pipelines-airflow-scheduler-1 airflow dags list-runs -d <dag_id>
# Chercher un run "queued" avec une execution_date dans le futur
```

**Cause :** Un run avait été déclenché avec une `execution_date` dans le futur (ex : demain). Airflow ne l'exécute jamais mais il occupe le slot `max_active_runs=1` du DAG → tous les autres runs attendent derrière.

**Résolution :** Supprimer le run fantôme via l'UI (vue Grid → clic sur le run → Delete) ou en CLI selon la version d'Airflow. Le slot se libère et les runs en attente démarrent.

**Prévention :** Ne jamais déclencher un run avec `--exec-date` dans le futur.

---

### INC-11 — `daily_streams` vide alors que `listening_events` est plein

**Symptômes :** `aggregation_pipeline` passe `success`, mais `SELECT * FROM daily_streams` ne retourne rien.

**Cause :** Un DAG Airflow agrège par défaut la période `data_interval_start`, qui correspond au **jour précédent** l'`execution_date` (un run du 3 juin agrège le 2 juin). Si les events sont datés d'aujourd'hui mais que le run cible la veille, la requête `WHERE DATE(timestamp) = <veille>` ne trouve rien. **Ce n'est pas un bug** — c'est le comportement standard d'Airflow.

**Diagnostic :**
```bash
# Comparer la date des events avec la date ciblée par le run
docker exec -it data_pipelines-postgres-1 psql -U spotify -d spotify -c "SELECT DATE(timestamp), COUNT(*) FROM listening_events GROUP BY DATE(timestamp);"
```

**Résolution :** Déclencher le run avec une `execution_date` telle que `data_interval_start` tombe sur la date des events (un run daté de J+1 agrège J).

---

## Incidents Phase 2 — Kafka / Spark

### INC-04 — Consumer lag Kafka qui explose

**Symptômes :** Kafka UI → consumer group `spark-streaming-trends` → lag > 10 000

**Diagnostic :**
```bash
# Vérifier le throughput Spark
docker logs spark-master -f | grep "Batch Duration"

# Vérifier les ressources
docker stats spark-worker-1
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-05 — Job Spark crash avec OutOfMemory

**Symptômes :** `java.lang.OutOfMemoryError: GC overhead limit exceeded`

**Diagnostic :**
```bash
docker logs spark-master -f | grep -i "error\|exception\|oom"
```

**Résolution :**
```bash
# Augmenter la mémoire du worker dans docker-compose
# SPARK_WORKER_MEMORY: 4G

# Réduire le state store : ajouter un TTL sur flatMapGroupsWithState
# GroupState.setTimeoutDuration("1 hour")
```

---

### INC-06 — Spark ne reprend pas depuis le checkpoint

**Symptômes :** Après redémarrage, le job repart de zéro au lieu du checkpoint.

**Diagnostic :**
```bash
# Vérifier que le checkpoint est sur MinIO
docker exec minio mc ls local/spotify-checkpoints/streaming_trends/

# Vérifier les logs Spark au démarrage
docker logs spark-master | grep "checkpoint"
```

**Résolution :**
→ À compléter par votre groupe

---

### INC-12 — `realtime_top_tracks` reste vide alors que le job Spark tourne

**Symptômes :** Le job Spark tourne sans erreur, mais `SELECT * FROM realtime_top_tracks` ne retourne rien.

**Diagnostic :**
```bash
# 1. Le topic reçoit-il des events ? (Kafka UI localhost:8090 → topic listening_events)
# 2. event_time est-il bien parsé (non NULL) ? Regarder la colonne dans la sortie console du job.
# 3. Le catalogue est-il peuplé ? (sinon le simulateur tourne en track_id aléatoires)
docker exec data_pipelines-postgres-1 psql -U spotify -d spotify -c "SELECT count(*) FROM tracks;"
```

**Cause :** Le simulateur produit `datetime.utcnow().isoformat() + "Z"`, qui **omet les microsecondes** quand elles valent 0. Un pattern de parsing fixe (`.SSSSSS`) échoue sur les timestamps sans fraction → `event_time` NULL → aucune fenêtre ne se forme.

**Résolution :** Parser avec un `coalesce` tolérant de deux formats (avec et sans microsecondes), côté Spark — ne jamais imposer un format au simulateur (consommé par plusieurs jobs) :
```python
F.coalesce(
    F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss.SSSSSS"),
    F.to_timestamp(col, "yyyy-MM-dd'T'HH:mm:ss"),
)
```

**Prévention :** Côté consommateur, toujours parser les timestamps ISO de façon tolérante aux fractions de seconde variables.

---

### INC-13 — Crash `IntegrityError` / violation de clé primaire à l'écriture Postgres

**Symptômes :** Le foreachBatch crashe au 2ᵉ batch : `duplicate key value violates unique constraint` sur `realtime_top_tracks_pkey`.

**Cause :** Avec `outputMode("update")`, Spark réémet une même fenêtre à chaque batch tant qu'elle évolue. Un `write.mode("append")` tente alors de réinsérer une PK `(window_start, track_id)` déjà présente.

**Résolution :** Remplacer l'append par un **UPSERT** dans le foreachBatch (psycopg2, installé sur spark-master) :
```sql
INSERT INTO realtime_top_tracks (...) VALUES %s
ON CONFLICT (window_start, track_id) DO UPDATE SET
    stream_count = EXCLUDED.stream_count, ...
```

**Prévention :** En `outputMode("update")`, toute écriture vers une table à PK doit être idempotente (UPSERT), jamais un append simple.

---

### INC-14 — Job Spark instable / checkpoints qui se corrompent entre queries

**Symptômes :** Comportement non déterministe avec plusieurs queries en parallèle ; une query refuse de démarrer ou rejoue des offsets.

**Cause :** Plusieurs `writeStream` partageaient un `checkpointLocation` imbriqué (une query sur la racine, les autres sur des sous-dossiers de cette racine). Chaque query doit avoir un répertoire de checkpoint **dédié et non imbriqué**.

**Résolution :** Un sous-dossier frère par query :
.../streaming_trends/console
.../streaming_trends/top_tracks
.../streaming_trends/genre_listeners

**Prévention :** Un checkpointLocation unique par query, jamais imbriqué dans celui d'une autre.

---

## Chaos Engineering — Résultats

> Compléter pendant l'issue #25 (vendredi)

### Scénario 1 : Arrêt d'un broker Kafka

**Commande :** `docker compose stop kafka-2`

**Comportement observé :** ...

**Recovery automatique :** oui / non — détails : ...

**Temps de recovery :** ...

---

### Scénario 2 : Kill du driver Spark

**Commande :** `docker compose kill spark-master`

**Comportement observé :** ...

**Recovery depuis checkpoint :** oui / non — détails : ...

**Doublons introduits :** 0 / N — vérification : ...

---

### Scénario 3 : Coupure PostgreSQL

**Commande :** `docker compose stop postgres` (2 minutes) → `docker compose start postgres`

**Comportement observé (Airflow) :** ...

**Comportement observé (Spark) :** ...

**Données perdues :** oui / non — détails : ...