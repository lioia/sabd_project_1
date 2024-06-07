import time
from typing import List, Tuple

from pyspark.sql import SparkSession

from spark.df import df_preprocess, query_1_df, query_2_df
from spark.rdd import query_1_rdd, query_2_rdd, rdd_preprocess
from spark.utils import load_dataset


def analysis_filtered(filename: str) -> List[Tuple[str, str, int, int, float, float]]:
    # filename,api,query,worker,load_time,exec_time
    performances: List[Tuple[str, str, int, int, float, float]] = []
    # create Spark session
    format = filename.split(".")[-1]
    spark = (
        SparkSession.Builder()
        .appName(f"sabd_{format}")
        .config("spark.logConf", "true")
        .getOrCreate()
    )
    # remove logging
    spark.sparkContext.setLogLevel("OFF")
    # get number of workers
    conf_worker = spark.sparkContext.getConf().get("spark.cores.max")
    if conf_worker is None:
        raise ValueError("spark.cores.max was not set")
    # convert number of workers into number
    worker = int(conf_worker)

    # load dataset based on format
    df = load_dataset(spark, filename).cache()
    # start timer
    start = time.time()
    # action on dataset loading (so the first query is not influenced by this)
    df.head()
    # dataset loading time
    load_time = time.time() - start

    # DataFrame
    start = time.time()
    q1 = query_1_df(df)
    q1.collect()
    performances.append((filename, "df", 1, worker, load_time, time.time() - start))
    start = time.time()
    q2_1, q2_2 = query_2_df(df)
    q2_1.collect()
    q2_2.collect()
    performances.append((filename, "df", 2, worker, load_time, time.time() - start))

    # RDD
    start = time.time()
    q1 = query_1_rdd(df.rdd)
    q1.collect()
    performances.append((filename, "rdd", 1, worker, load_time, time.time() - start))
    start = time.time()
    q2_1, q2_2 = query_2_rdd(df.rdd)
    q2_1.collect()
    q2_2.collect()
    performances.append((filename, "rdd", 2, worker, load_time, time.time() - start))

    spark.stop()
    return performances


def analysis_not_filtered() -> List[Tuple[str, str, str, int, float, float]]:
    performances = []
    spark = (
        SparkSession.Builder()
        .appName(f"sabd_{format}")
        .config("spark.logConf", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("OFF")
    conf_worker = spark.sparkContext.getConf().get("spark.cores.max")
    if conf_worker is None:
        raise ValueError("spark.cores.max was not set")
    worker = int(conf_worker)

    # load complete dataset
    filename = "dataset.csv"
    df = load_dataset(spark, filename).cache()
    start = time.time()
    # action on dataset loading (so the first query is not influenced by this)
    df.head()
    load_time = time.time() - start

    # DataFrame
    start = time.time()
    q1 = query_1_df(df_preprocess(df))
    q1.collect()
    performances.append((filename, "df", 1, worker, load_time, time.time() - start))
    start = time.time()
    q2_1, q2_2 = query_2_df(df_preprocess(df))
    q2_1.collect()
    q2_2.collect()
    performances.append((filename, "df", 2, worker, load_time, time.time() - start))

    # # RDD
    start = time.time()
    q1 = query_1_rdd(rdd_preprocess(df))
    q1.collect()
    performances.append((filename, "rdd", 1, worker, load_time, time.time() - start))
    start = time.time()
    q2_1, q2_2 = query_2_rdd(rdd_preprocess(df))
    q2_1.collect()
    q2_2.collect()
    performances.append((filename, "rdd", 2, worker, load_time, time.time() - start))

    return performances
