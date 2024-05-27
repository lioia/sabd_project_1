import time

from pyspark.sql import SparkSession

from rdd import query_1_rdd, query_2_rdd, rdd_preprocess
from df import df_preprocess, query_1_df, query_2_df
from utils import load_dataset, api_query_map
from utils import (
    check_results_1,
    check_results_2_1,
    check_results_2_2,
    write_to_hdfs,
)


# TODO: choose API and format based on analysis results
# TODO: choose where to save based on cli args
def run_spark_save():
    # creating Spark session
    spark = SparkSession.Builder().appName("sabd_save").getOrCreate()
    # load dataset
    rdd = load_dataset(spark, "filtered.parquet").rdd
    # running queries
    q1 = query_1_rdd(rdd)
    q2_1, q2_2 = query_2_rdd(rdd)

    # save to HDFS
    write_to_hdfs(q1, "/results/query_1/")
    write_to_hdfs(q2_1, "/results/query_2_1")
    write_to_hdfs(q2_2, "/results/query_2_2")
    # stop Spark
    spark.stop()


def run_spark_analysis(worker: int):
    # create performance output file
    analysis_file = open("/results/analysis.csv", "w+")
    # write header
    analysis_file.write("api,filename,query,workers,time")

    # variables
    apis = ["rdd", "df"]
    filenames = ["dataset.csv", "filtered.csv", "filtered.avro", "filtered.parquet"]
    queries = [1, 2]
    # iterating through all the possible combinations
    for filename in filenames:
        for api in apis:
            for query in queries:
                delta = __run_spark_analysis(filename, api, query)
                analysis_file.write(f"{api},{filename},{query},{worker},{delta}")


# helper function to run the Spark query for a specific combination
def __run_spark_analysis(filename: str, api: str, query: int) -> float:
    # create Spark session
    format = filename.split(".")[-1]
    spark = SparkSession.Builder().appName(f"sabd_{format}_{api}_{query}").getOrCreate()

    # start timer
    start_time = time.time()

    # load dataset based on format
    df = load_dataset(spark, filename)
    rdd = df.rdd
    if filename == "dataset.csv":
        # TODO: preprocessing
        if api == "df":
            df = df_preprocess(df)
        elif api == "rdd":
            rdd = rdd_preprocess(df)

    # run query based on API
    api_query_map[api][query](df if api == "df" else rdd)
    # stop timer
    end_time = time.time()
    # calculated execution time
    delta = end_time - start_time
    # stop Spark session
    spark.stop()
    return delta


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
