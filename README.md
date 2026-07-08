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