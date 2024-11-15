import re
import sys
import os
import time
from typing import Tuple
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, sum as spark_sum, size, hash
from pyspark.sql import functions as F

def parse_neighbors(line: str) -> Tuple[str, str]:
    """Parse une ligne pour extraire une paire d'URLs."""
    parts = re.split(r'\s+', line)
    return parts[0], parts[2]

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("Usage: pagerank <file> <iterations> <bucket> <output> <timer_path> <output_execution> <num_workers> <use_partition>",
              file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    main_file_name = os.path.basename(sys.argv[0])[:-3]
    input_file = sys.argv[1]
    iteration_arg = int(sys.argv[2])
    bucket = sys.argv[3]
    output = sys.argv[4]
    timer_path = sys.argv[5]
    output_execution = sys.argv[6]
    num_workers = int(sys.argv[7])
    use_partition = sys.argv[8].lower() == 'url-partionner'
    partition_path = sys.argv[8].lower()

    # Initialiser SparkSession
    spark = SparkSession.builder.appName("PythonPageRankDF").getOrCreate()

    # Charger les données en DataFrame et parser les voisins
    lines = spark.read.text("gs://"+bucket+"/"+input_file)
    links = lines.rdd.map(lambda row: parse_neighbors(row.value)).toDF(["src", "dst"])

    # Grouper par source pour créer une liste des liens sortants
    links = links.groupBy("src").agg(F.collect_list("dst").alias("links"))

    # --- Partitionnement basé sur le hash (si activé) ---
    if use_partition:
        # Appliquer une fonction de hashage à 'src' pour obtenir un partitionnement
        links = links.withColumn("partition_id", hash("src") % num_workers)
        
        # Repartitionner le DataFrame en fonction de la partition_id
        links = links.repartition(num_workers, "partition_id").drop("partition_id")
    else:
        # Si le partitionnement est désactivé, on garde le DataFrame tel quel
        links = links.cache()

    # Initialiser les rangs avec une valeur de 1.0 pour chaque URL
    ranks = links.select("src").withColumn("rank", lit(1.0))

    # Démarrer le timer avant les itérations de PageRank
    start_time = time.time()

    # Effectuer les itérations de PageRank
    for iteration in range(iteration_arg):
        # Calcul des contributions de chaque lien
        contribs = links.alias("l").join(ranks.alias("r"), col("l.src") == col("r.src")) \
            .select(
                col("l.src").alias("src"),
                explode(col("l.links")).alias("dst"),
                (col("r.rank") / size(col("l.links"))).alias("contrib")
            )

        # Repartitionner les contributions avant l'agrégation pour éviter les shuffles si partitionnement activé
        if use_partition:
            contribs = contribs.repartition(num_workers, "dst")

        # Calcul des nouveaux rangs par agrégation des contributions
        ranks = contribs.groupBy("dst").agg(spark_sum("contrib").alias("rank"))
        
        # Appliquer le facteur de décroissance de PageRank
        ranks = ranks.withColumn("rank", col("rank") * 0.85 + 0.15)

        # Renommer `dst` en `src` pour l'itération suivante
        ranks = ranks.withColumnRenamed("dst", "src")

    # Arrêter le timer après le calcul
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Sauvegarder les résultats finaux dans GCS avec coalesce(1)
    output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/"+output
    ranks.write.mode("overwrite").csv(output_path)

    # Sauvegarder le temps d'exécution dans le répertoire timer avec coalesce(1)
    timer_output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/timer/"+timer_path
    spark.createDataFrame([(elapsed_time,)], ["elapsed_time"]).write.mode("overwrite").csv(timer_output_path)

    spark.stop()