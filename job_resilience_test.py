import time
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg

spark = SparkSession.builder.appName("tp3-resilience-test").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.csv(
    "hdfs://namenode:8020/data/commandes/*.csv",
    header=True,
    inferSchema=False,
)

df = (
    df.withColumn("quantite", col("quantite").cast("int"))
      .withColumn("prix_unitaire", col("prix_unitaire").cast("double"))
      .withColumn("ca", col("quantite") * col("prix_unitaire"))
)

# Force la lecture réelle des blocs HDFS maintenant (sinon Spark est lazy et
# ne va lire les fichiers qu'au moment de l'action .count()/.write())
print(f"Nombre de lignes lues : {df.count()}")

print(">>> PAUSE 20s — va couper un datanode maintenant (docker stop datanodeX) <<<")
time.sleep(20)

agg = df.groupBy("entrepot", "date").agg(
    _sum("ca").alias("chiffre_affaires_total"),
    count("id_commande").alias("nb_commandes"),
    avg("ca").alias("panier_moyen"),
)

agg.write.mode("overwrite").parquet("hdfs://namenode:8020/data/agg_entrepot_jour_test")

print("=== Job terminé avec succès malgré la panne ===")
agg.show(20, truncate=False)

spark.stop()