# TP 3 — L'entrepôt résilient (HDFS & Spark)

## Contexte

Migration du stockage des journaux de commandes d'une seule machine vers un
cluster HDFS distribué et tolérant aux pannes, interrogé directement par Spark
(pas de fichier transitant par le disque local d'une seule machine).

## Architecture

- **HDFS** : 1 namenode + 3 datanodes (images `bde2020/hadoop-*`), réplication = 3
- **Spark standalone** : 1 master + 2 workers (images `bde2020/spark-*`), même
  réseau Docker (`tp3net`) que le cluster HDFS
- Communication exclusivement via `hdfs://namenode:8020/...`

Schéma :
```
[host] --docker cp--> [namenode] --hdfs put--> HDFS (3 datanodes, repl=3)
                                                    ^
[spark-master] ----spark-submit---- lit/écrit ------|
[spark-worker-1/2]
```

## Étape 1 — Lancement du cluster HDFS + Spark

```bash
docker compose up -d
```

Vérifications :
- UI namenode : http://localhost:9870
- UI Spark master : http://localhost:8080

```bash
docker ps
```
| CONTAINER ID | IMAGE | COMMAND | CREATED | STATUS | PORTS | NAMES |
|---|---|---|---|---|---|---|
| 111f62bf50e4 | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | "/entrypoint.sh /run…" | About a minute ago | Up About a minute (healthy) | 9864/tcp | project2-datanode3-1 |
| 007b6a844e59 | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | "/entrypoint.sh /run…" | About a minute ago | Up About a minute (healthy) | 9864/tcp | project2-datanode1-1 |
| 056ab10425c4 | bde2020/hadoop-datanode:2.0.0-hadoop3.2.1-java8 | "/entrypoint.sh /run…" | About a minute ago | Up About a minute (healthy) | 9864/tcp | project2-datanode2-1 |
| c6ee06e45a5e | bde2020/spark-worker:3.3.0-hadoop3.3 | "/bin/bash /worker.sh" | About a minute ago | Up About a minute | 8081/tcp | project2-spark-worker-1-1 |
| eb72dff7a955 | bde2020/spark-worker:3.3.0-hadoop3.3 | "/bin/bash /worker.sh" | About a minute ago | Up About a minute | 8081/tcp | project2-spark-worker-2-1 |
| c6fc61c4422d | bde2020/spark-master:3.3.0-hadoop3.3 | "/bin/bash /master.sh" | About a minute ago | Up About a minute | 0.0.0.0:7077->7077/tcp, [::]:7077->7077/tcp, 0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp | spark-master |
| f1fcd1a6a284 | bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8 | "/entrypoint.sh /run…" | About a minute ago | Up About a minute (healthy) | 0.0.0.0:8020->8020/tcp, [::]:8020->8020/tcp, 0.0.0.0:9870->9870/tcp, [::]:9870->9870/tcp | namenode |

## Étape 2 — Génération et chargement des données

```bash
uv run python generate_commandes.py

docker cp commandes_2026-06-12.csv namenode:/tmp/
docker cp commandes_2026-06-13.csv namenode:/tmp/
docker cp commandes_2026-06-14.csv namenode:/tmp/

docker exec namenode hdfs dfs -mkdir -p /data/commandes
docker exec namenode hdfs dfs -put /tmp/commandes_2026-06-12.csv /data/commandes/
docker exec namenode hdfs dfs -put /tmp/commandes_2026-06-13.csv /data/commandes/
docker exec namenode hdfs dfs -put /tmp/commandes_2026-06-14.csv /data/commandes/
```

```bash
docker exec namenode hdfs dfs -ls /data/commandes
```

Found 3 items
-rw-r--r--   3 root supergroup      76353 2026-07-08 12:57 /data/commandes/commandes_2026-06-12.csv
-rw-r--r--   3 root supergroup      76582 2026-07-08 12:57 /data/commandes/commandes_2026-06-13.csv
-rw-r--r--   3 root supergroup      76371 2026-07-08 12:57 /data/commandes/commandes_2026-06-14.csv

## Étape 3 — Preuve de la réplication (avant panne)

```bash
docker exec namenode hdfs fsck /data/commandes -files -blocks -locations
```

```
Status: HEALTHY
 Number of data-nodes:  3
 Number of racks:               1
 Total dirs:                    1
 Total symlinks:                0
```

## Étape 4 — Job Spark : lecture, typage, agrégation

```bash
docker cp job.py spark-master:/tmp/job.py
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /tmp/job.py
```

Le job :
- lit les 3 CSV en un seul DataFrame (`hdfs://namenode:8020/data/commandes/*.csv`)
- caste `quantite` en int et `prix_unitaire` en double
- calcule par `entrepot` et `date` : CA total, nombre de commandes, panier moyen
- écrit le résultat en Parquet dans `hdfs://namenode:8020/data/agg_entrepot_jour`

```
+-------------+----------+----------------------+------------+------------------+
|entrepot     |date      |chiffre_affaires_total|nb_commandes|panier_moyen      |
+-------------+----------+----------------------+------------+------------------+
|Entrepot Est |2026-06-12|69854.31999999996     |195         |358.22728205128186|
|Entrepot Nord|2026-06-12|165671.70999999993    |493         |336.0480933062879 |
|Entrepot Sud |2026-06-12|97317.95              |312         |311.9165064102564 |
|Entrepot Est |2026-06-13|62843.82000000001     |213         |295.04140845070424|
|Entrepot Nord|2026-06-13|196025.71999999983    |515         |380.63246601941717|
|Entrepot Sud |2026-06-13|100965.93000000002    |272         |371.1982720588236 |
|Entrepot Est |2026-06-14|65872.82000000004     |189         |348.53343915343936|
|Entrepot Nord|2026-06-14|188460.4900000001     |501         |376.1686427145711 |
|Entrepot Sud |2026-06-14|112708.41999999998    |310         |363.5755483870967 |
+-------------+----------+----------------------+------------+------------------+
```

## Étape 5 — Vérification du résultat Parquet

```bash
docker exec namenode hdfs dfs -ls /data/agg_entrepot_jour
```

```
Found 2 items
-rw-r--r--   3 root supergroup          0 2026-07-08 13:08 /data/agg_entrepot_jour/_SUCCESS
-rw-r--r--   3 root supergroup       1906 2026-07-08 13:08 /data/agg_entrepot_jour/part-00000-6fa7f161-a330-46fb-b51c-5ddd885fb274-c000.snappy.parquet
```

**Analyse** : le fichier `_SUCCESS` confirme que l'écriture s'est terminée sans
erreur. Un seul fichier `part-00000...snappy.parquet` (compression Snappy, le
défaut Spark) suffit puisque le résultat agrégé ne compte que 9 lignes au
maximum (3 entrepôts × 3 jours) — tout tient dans une seule partition.

## Étape 6 — Test de résilience (panne d'un datanode)

Protocole :
1. Lancement d'une version instrumentée du job (`job_resilience_test.py`)
   forçant une lecture (`df.count()`) puis une pause de 20 secondes avant
   l'agrégation/écriture, pour avoir le temps d'agir manuellement.
2. Pendant la pause, arrêt d'un datanode :
   ```bash
   docker stop project2-datanode2-1
   ```
3. Observation des logs du job et de l'état HDFS pendant la panne, puis après
   redémarrage du datanode.

### Log du job pendant la panne

```bash
docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /tmp/job_resilience_test.py
```

```
Nombre de lignes lues : 3000
>>> PAUSE 20s — va couper un datanode maintenant (docker stop datanodeX) <<<
=== Job terminé avec succès malgré la panne ===
+-------------+----------+----------------------+------------+------------------+
|entrepot     |date      |chiffre_affaires_total|nb_commandes|panier_moyen      |
+-------------+----------+----------------------+------------+------------------+
|Entrepot Est |2026-06-13|62843.82000000001     |213         |295.04140845070424|
|Entrepot Sud |2026-06-13|100965.93000000002    |272         |371.1982720588236 |
|Entrepot Nord|2026-06-13|196025.71999999983    |515         |380.63246601941717|
|Entrepot Sud |2026-06-12|97317.95              |312         |311.9165064102564 |
|Entrepot Nord|2026-06-12|165671.70999999993    |493         |336.0480933062879 |
|Entrepot Est |2026-06-12|69854.31999999996     |195         |358.22728205128186|
|Entrepot Nord|2026-06-14|188460.4900000001     |501         |376.1686427145711 |
|Entrepot Est |2026-06-14|65872.82000000004     |189         |348.53343915343936|
|Entrepot Sud |2026-06-14|112708.41999999998    |310         |363.5755483870967 |
+-------------+----------+----------------------+------------+------------------+
```

**Résultat observé** : le job **réussit malgré l'arrêt du datanode**. Aucune
erreur, écriture Parquet complète, résultat cohérent (9 lignes = 3 entrepôts ×
3 jours).

### `fsck` exécuté juste après l'arrêt du datanode (toujours arrêté)

```bash
docker exec namenode hdfs fsck /data/commandes -files -blocks -locations
```

```
/data/commandes/commandes_2026-06-12.csv ... Live_repl=3 [datanode1, datanode2, datanode3]
/data/commandes/commandes_2026-06-13.csv ... Live_repl=3 [datanode1, datanode2, datanode3]
/data/commandes/commandes_2026-06-14.csv ... Live_repl=3 [datanode1, datanode2, datanode3]
Status: HEALTHY
Under-replicated blocks: 0 (0.0 %)
```

**Point important — pourquoi le fsck affiche encore `Live_repl=3` alors que le
datanode est arrêté** : HDFS possède deux mécanismes de tolérance aux pannes,
à des échelles de temps différentes.


### `hdfs dfsadmin -report` — preuve explicite de la détection de panne côté NameNode

```bash
docker exec namenode hdfs dfsadmin -report
```

```
Configured Capacity: 3243303530496 (2.95 TB)
...
Replicated Blocks:
        Under replicated blocks: 0
        Blocks with corrupt replicas: 0
        Missing blocks: 0
-------------------------------------------------
Live datanodes (2):
Name: 172.19.0.6:9866 (project2-datanode3-1.project2_tp3net) ... Last contact: Wed Jul 08 13:47:07 UTC 2026
Name: 172.19.0.8:9866 (project2-datanode1-1.project2_tp3net) ... Last contact: Wed Jul 08 13:47:07 UTC 2026

Dead datanodes (1):
Name: 172.19.0.7:9866 (172.19.0.7)
Hostname: 056ab10425c4
Decommission Status : Normal
Last contact: Wed Jul 08 13:29:41 UTC 2026
Last Block Report: Wed Jul 08 13:28:23 UTC 2026
Num of Blocks: 4
```

**Restart du dataNote**

```bash
    docker start project2-datanode2-1
    sleep 15
    docker exec namenode hdfs dfsadmin -report
```

```
-------------------------------------------------
Live datanodes (3):
Name: 172.19.0.6:9866 (project2-datanode3-1.project2_tp3net) ... Last contact: Wed Jul 08 14:03:25 UTC 2026
Name: 172.19.0.7:9866 (project2-datanode2-1.project2_tp3net) ... Last contact: Wed Jul 08 14:03:26 UTC 2026
Name: 172.19.0.8:9866 (project2-datanode1-1.project2_tp3net) ... Last contact: Wed Jul 08 14:03:25 UTC 2026
```

Analyse : après redémarrage du conteneur, datanode2 (172.19.0.7) se
ré-enregistre auprès du NameNode et repasse en Live datanodes (3), avec un
Last contact à jour. Le cluster est revenu à son état nominal — la boucle
complète est ainsi démontrée : panne détectée (Dead datanodes (1)) →
service maintenu malgré la panne (job Spark réussi) → guérison détectée
après redémarrage (Live datanodes (3)).

