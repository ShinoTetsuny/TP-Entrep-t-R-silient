from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, count, avg

spark = SparkSession.builder.appName("tp3-entrepot").getOrCreate()

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

agg = df.groupBy("entrepot", "date").agg(
    _sum("ca").alias("chiffre_affaires_total"),
    count("id_commande").alias("nb_commandes"),
    avg("ca").alias("panier_moyen"),
)

agg.write.mode("overwrite").parquet("hdfs://namenode:8020/data/agg_entrepot_jour")

print("=== Aperçu du résultat agrégé ===")
agg.orderBy("date", "entrepot").show(20, truncate=False)

spark.stop()