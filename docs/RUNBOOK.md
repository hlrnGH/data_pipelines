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