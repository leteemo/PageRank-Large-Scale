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
    use_partition = sys.argv[8].lower() == 'url-partionner'
    partition_path = sys.argv[8].lower()


    # Initialize the spark context.
    spark = SparkSession.builder.appName("PythonPageRank").getOrCreate()

    sc = spark.sparkContext

    # Load input file as RDD and parse neighbors.
    lines = spark.read.text("gs://"+bucket+"/"+input_file).rdd.map(lambda r: r[0])
    links = lines.map(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()

    # Initialize ranks with a rank of 1.0 for each URL.
    ranks = links.mapValues(lambda _: 1.0)

    if use_partition:
        links = links.partitionBy(None).cache()
        ranks = ranks.partitionBy(None).cache()

    # Start timer
    total_start_time = time.time()

    # Perform PageRank iterations with precise timing.
    for iteration in range(int(iteration_arg)):
        iteration_start_time = time.time()

        contribs = links.join(ranks).flatMap(lambda url_urls_rank: computeContribs(
            url_urls_rank[1][0], url_urls_rank[1][1]
        ))
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)

        ranks.take(1)

    # End timer after all iterations
    total_end_time = time.time()
    total_execution_time = total_end_time - total_start_time
    print("time:", total_execution_time)

    # Sauvegarde des résultats dans GCS avec coalesce pour réduire le nombre de fichiers
    output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/"+output
    ranks.saveAsTextFile(output_path)

    # Sauvegarde du temps d'exécution dans GCS avec coalesce également
    timer_output_path = "gs://"+bucket+"/"+main_file_name+"/"+output_execution+"/"+partition_path+"/timer/"+timer_path
    sc.parallelize([total_execution_time]).saveAsTextFile(timer_output_path)

    spark.stop()
