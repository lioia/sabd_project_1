import time

from pyspark.sql import SparkSession

from nifi import run_nifi_flow
from rdd import query_1_rdd, query_2_rdd, rdd_preprocess
from df import df_preprocess, query_1_df, query_2_df
from spark.utils import load_dataset, api_query_map
from utils import check_results_1, check_results_2_1, check_results_2_2, write_to_hdfs


# TODO: accept another paramter to specify where to save the file (HDFS or NoSQL data store)
def run_spark_save(nifi: bool, format: str):
    spark = SparkSession.Builder().appName("sabd_save").getOrCreate()
    if nifi or format in ["avro", "parquet"]:
        # run nifi if required and load appropriate file
        run_nifi_flow()
        rdd = load_dataset(spark, format).rdd
    else:
        # read complete dataset and preprocess
        df = spark.read.csv("hdfs://master:54310/data/dataset.csv", inferSchema=True)
        rdd = rdd_preprocess(df)
    # execute queries
    q1 = query_1_rdd(rdd)
    q2_1, q2_2 = query_2_rdd(rdd)

    # write to HDFS
    write_to_hdfs(q1, "/results/query_1/")
    write_to_hdfs(q2_1, "/results/query_2_1")
    write_to_hdfs(q2_2, "/results/query_2_2")


def run_spark_analysis():
    analysis_file = open("/results/analysis.csv", "w+")
    analysis_file.write("api,format,query,workers,time")
    apis = ["rdd", "df"]
    formats = ["csv", "avro", "parquet"]
    queries = [1, 2]
    workers = range(1, 9)
    run_nifi_flow()
    for format in formats:
        for api in apis:
            for query in queries:
                for worker in workers:
                    elapsed_time = __run_spark_analysis(format, api, query, worker)
                    analysis_file.write(
                        f"{api},{format},{query},{worker},{elapsed_time}"
                    )


def __run_spark_analysis(format: str, api: str, query: int, worker: int) -> float:
    # create Spark session
    spark = SparkSession.Builder().appName(f"sabd_{format}_{api}_{query}").getOrCreate()
    # set number of workers
    spark.conf.set("spark.executor.instances", worker)
    # load dataset based on format
    df = load_dataset(spark, format)
    # start timer
    start_time = time.time()
    # execute query based on api
    api_query_map[api][query](df if api == "df" else df.rdd)
    # stop timer
    end_time = time.time()
    # calculated execution time
    elapsed_time = end_time - start_time
    # stop Spark session
    spark.stop()
    return elapsed_time


def run_spark_check():
    spark = SparkSession.Builder().appName("sabd_check").getOrCreate()
    df = spark.read.csv(
        "hdfs://master:54310/data/dataset.csv", inferSchema=True
    ).cache()
    filtered_rdd = rdd_preprocess(df)
    q1_rdd = query_1_rdd(filtered_rdd)
    q2_1_rdd, q2_2_rdd = query_2_rdd(filtered_rdd)

    filtered_df = df_preprocess(df)
    q1_df = query_1_df(filtered_df)
    q2_1_df, q2_2_df = query_2_df(filtered_df)

    check_results_1(q1_rdd, q1_df)
    check_results_2_1(q2_1_rdd, q2_1_df)
    check_results_2_2(q2_2_rdd, q2_2_df)
    spark.stop()
