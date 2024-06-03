import os
import time
from typing import Tuple

from pyspark.sql import SparkSession

from rdd import query_1_rdd, query_2_rdd, rdd_preprocess
from df import df_preprocess, query_1_df, query_2_df
from utils import (
    check_results_1,
    check_results_2_1,
    check_results_2_2,
    save_to_hdfs,
    load_dataset,
    api_query_map,
    save_to_mongo,
)


def run_spark_save(location: str):
    # creating Spark session
    if location == "hdfs":
        spark = SparkSession.Builder().appName("sabd_save").getOrCreate()
    elif location == "mongo":
        username = os.environ.get("MONGO_USERNAME")
        password = os.environ.get("MONGO_PASSWORD")
        if username is None or password is None:
            raise KeyError("Environment Variables for Mongo not set")
        uri = f"mongodb://{username}:{password}@mongo:27017/"
        spark = (
            # create new session builder
            SparkSession.Builder()
            # set session name
            .appName("sabd_save")
            # config mongo
            .config("spark.mongodb.write.connection.uri", uri)
            # create session
            .getOrCreate()
        )
    else:
        raise ValueError(f"Invalid location {location}, expected: hdfs or mongo")
    # load dataset
    df = load_dataset(spark, "filtered.parquet")
    # running queries
    q1 = query_1_df(df)[0]
    q2_1, q2_2 = query_2_df(df)

    if location == "hdfs":
        # save to HDFS
        save_to_hdfs(q1, "/results/query_1/")
        save_to_hdfs(q2_1, "/results/query_2_1")
        save_to_hdfs(q2_2, "/results/query_2_2")
    else:  # location == "mongo"
        save_to_mongo(q1, "query_1")
        save_to_mongo(q2_1, "query_2_1")
        save_to_mongo(q2_2, "query_2_2")
    # stop Spark
    spark.stop()


def run_spark_analysis():
    # create analysis list
    performance = []

    # variables
    apis = ["rdd", "df"]
    filenames = ["dataset.csv", "filtered.csv", "filtered.avro", "filtered.parquet"]
    queries = [1, 2]
    # iterating through all the possible combinations
    for filename in filenames:
        for api in apis:
            for query in queries:
                print(f"Analysis: {api},{filename},{query}")
                worker, delta = __run_spark_analysis(filename, api, query)
                performance.append((api, filename, query, worker, delta))
    print("api,filename,query,worker,delta")
    for api, filename, query, worker, delta in performance:
        print(f"{api},{filename},{query},{worker},{delta}")


# helper function to run the Spark query for a specific combination
def __run_spark_analysis(filename: str, api: str, query: int) -> Tuple[int, float]:
    # create Spark session
    format = filename.split(".")[-1]
    spark = SparkSession.Builder().appName(f"sabd_{format}_{api}_{query}").getOrCreate()
    conf_worker = spark.sparkContext.getConf().get("spark.cores.max")
    if conf_worker is None:
        raise ValueError("spark.cores.max was not set")
    worker = int(conf_worker)

    # start timer
    start_time = time.time()

    # load dataset based on format
    df = load_dataset(spark, filename)
    rdd = df.rdd
    if filename == "dataset.csv":
        if api == "df":
            df = df_preprocess(df)
        elif api == "rdd":
            rdd = rdd_preprocess(df)

    # run query based on API
    dfs = api_query_map[api][query](df if api == "df" else rdd)
    for res in dfs:
        res.show()
    # stop timer
    end_time = time.time()
    # calculated execution time
    delta = end_time - start_time
    # stop Spark session
    spark.stop()
    return worker, delta


def run_spark_check():
    # create Spark session
    spark = SparkSession.Builder().appName("sabd_check").getOrCreate()
    # read dataset.csv
    df = spark.read.csv(
        "hdfs://master:54310/data/dataset.csv",
        inferSchema=True,
    ).cache()
    # RDD pre-process
    filtered_rdd = rdd_preprocess(df)
    # run RDD queries
    q1_rdd = query_1_rdd(filtered_rdd)
    q2_1_rdd, q2_2_rdd = query_2_rdd(filtered_rdd)

    # DF pre-process
    filtered_df = df_preprocess(df)
    # run DF queries
    q1_df = query_1_df(filtered_df)
    q2_1_df, q2_2_df = query_2_df(filtered_df)

    # check  results
    check_results_1(q1_rdd, q1_df)
    check_results_2_1(q2_1_rdd, q2_1_df)
    check_results_2_2(q2_2_rdd, q2_2_df)
    # stop Spark
    spark.stop()
