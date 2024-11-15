import re
import sys
import time
import os
from operator import add
from typing import Iterable, Tuple
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf, RDD
from pyspark.rdd import portable_hash

num_workers = 1
use_custom_partitioner = True

def computeContribs(urls: Iterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls: str) -> Tuple[str, str]:
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[2]

def urlPartitioner(url: str) -> int:
    global num_workers
    """Custom partitioner based on the hash of the URL's first characters."""
    return portable_hash(url) % num_workers  # % cluster nodes

if __name__ == "__main__":
    if len(sys.argv) != 9:
        print("Usage: pagerank <file> <iterations> <bucket> <output> <timer path> <output execution> <num_workers> <use custom partitioner>", file=sys.stderr)
        sys.exit(-1)

    print("WARN: This is a naive implementation of PageRank and is given as an example!\n" +
          "Please refer to PageRank implementation provided by graphx",
          file=sys.stderr)

    main_file_name = os.path.basename(sys.argv[0])[:-3]
    input_file = sys.argv[1]
    iteration_arg = sys.argv[2]
    bucket = sys.argv[3]
    output = sys.argv[4]
    timer_path = sys.argv[5]
    output_execution = sys.argv[6]
    num_workers = int(sys.argv[7])
    use_custom_partitioner = sys.argv[8].lower() == 'url-partionner'
    partition_path = sys.argv[8].lower()

    # Configuration Spark avec mémoire et exécution spéculative
    spark = SparkSession.builder \
        .appName("OptimizedPythonPageRank") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memoryOverhead", "1g") \
        .config("spark.speculation", "true") \
        .getOrCreate()
    
    sc = spark.sparkContext

    # Chargement et parsing du fichier d'entrée
    lines = sc.textFile("gs://"+bucket+"/"+input_file)
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey()

    # Application de la partition personnalisée si nécessaire
    num_partitions = num_workers  # Ajustement en fonction des ressources
    if use_custom_partitioner:
        links = links.partitionBy(num_partitions, urlPartitioner).cache()
    else:
        links = links.repartition(num_partitions).cache()

    # Initialisation des rangs avec une valeur de 1.0
    ranks = links.mapValues(lambda _: 1.0)

    # Démarrer le timer avant le calcul de PageRank
    start_time = time.time()

    # Boucle d'itération pour calculer le PageRank
    for iteration in range(int(iteration_arg)):
        contribs = links.join(ranks).flatMap(
            lambda url_urls_rank: computeContribs(url_urls_rank[1][0], url_urls_rank[1][1])
        )

        # Utilisation de repartition pour équilibrer les données
        contribs = contribs.repartition(num_partitions)
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

    # Arrêter le timer après la fin du calcul
    end_time = time.time()
    elapsed_time = end_time - start_time

    # Sauvegarde des résultats dans GCS avec coalesce pour réduire le nombre de fichiers
    output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/"+output
    ranks.coalesce(1).saveAsTextFile(output_path)

    # Sauvegarde du temps d'exécution dans GCS avec coalesce également
    timer_output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/timer/"+timer_path
    sc.parallelize([elapsed_time]).coalesce(1).saveAsTextFile(timer_output_path)

    spark.stop()