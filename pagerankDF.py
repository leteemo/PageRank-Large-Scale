import re
import sys
import os
import time
from typing import Tuple, Iterable
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, lit, sum as spark_sum, size, hash
from pyspark.sql import functions as F
from operator import add
from pyspark.resultiterable import ResultIterable



def computeContribs(urls: Iterable[str], rank: float) -> Iterable[Tuple[str, float]]:
    """Calculates URL contributions to the rank of other URLs."""
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

def parseNeighbors(urls: str) -> Tuple[str, str]:
    """Parses a urls pair string into urls pair."""
    parts = re.split(r'\s+', urls)
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

    # Démarrer le timer avant les itérations de PageRank
    start_time = time.time()


    spark = SparkSession\
        .builder\
        .appName("PythonPageRank")\
        .getOrCreate()

    # Loads in input file. It should be in format of:
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     URL         neighbor URL
    #     ...
    lines = spark.read.text("gs://"+bucket+"/"+input_file).rdd.map(lambda r: r[0])

    # Loads all URLs from input file and initialize their neighbors.
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Loads all URLs with other URL(s) link to from input file and initialize ranks of them to one.
    ranks = links.map(lambda url_neighbors: (url_neighbors[0], 1.0))

    if use_partition:
        links = links.partitionBy(None).cache()
        ranks = ranks.partitionBy(None).cache()

    # Calculates and updates URL ranks continuously using PageRank algorithm.
    for iteration in range(int(sys.argv[2])):
        # Calculates URL contributions to the rank of other URLs.
        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]  # type: ignore[arg-type]
        ))

        # Re-calculates URL ranks based on neighbor contributions.
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)



    # Arrêter le timer après le calcul
    end_time = time.time()
    elapsed_time = end_time - start_time
    print("time", elapsed_time)

    # Sauvegarder les résultats finaux dans GCS avec coalesce(1)
    output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/"+output

    ranks_output = ranks.map(lambda x: (x[0], x[1])).toDF(["URL", "Rank"])
    ranks_output.write.mode("overwrite").csv(output_path)

    # Sauvegarder le temps d'exécution dans le répertoire timer avec coalesce(1)
    timer_output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/timer/"+timer_path
    spark.createDataFrame([(elapsed_time,)], ["elapsed_time"]).write.mode("overwrite").csv(timer_output_path)

    spark.stop()
