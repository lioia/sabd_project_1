import os

from pyspark.sql import SparkSession

from rdd import query_1_rdd, query_2_rdd, rdd_preprocess
from df import df_preprocess, query_1_df, query_2_df
from spark.analysis import analysis_not_filtered, analysis_filtered
from utils import (
    check_results_1,
    check_results_2_1,
    check_results_2_2,
    save_to_hdfs,
    load_dataset,
    save_to_mongo,
)


def run_spark_save(location: str):
    # creating Spark session
    if location == "hdfs":
        # standard session
        spark = SparkSession.Builder().appName("sabd_save").getOrCreate()
    elif location == "mongo":
        # get username and password from env vars
        username = os.environ.get("MONGO_USERNAME")
        password = os.environ.get("MONGO_PASSWORD")
        if username is None or password is None:
            raise KeyError("Environment Variables for Mongo not set")
        # mongo connection uri
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
    # run queries
    q1 = query_1_df(df)
    q2_1, q2_2 = query_2_df(df)

    if location == "hdfs":
        # save to HDFS
        save_to_hdfs(q1, "/results/query_1/")
        save_to_hdfs(q2_1, "/results/query_2_1")
        save_to_hdfs(q2_2, "/results/query_2_2")
    else:  # location == "mongo"; save to mongo
        save_to_mongo(q1, "query_1")
        save_to_mongo(q2_1, "query_2_1")
        save_to_mongo(q2_2, "query_2_2")
    # stop Spark
    spark.stop()


def run_spark_analysis():
    # create performances list
    performances = []

    # testing every filtered dataset
    filenames = ["filtered.csv", "filtered.avro", "filtered.parquet"]
    for filename in filenames:
        p = analysis_filtered(filename)
        performances.extend(p)
    # testing original dataset
    p = analysis_not_filtered()
    performances.extend(p)

    # Print Results
    print("filename,api,query,worker,load_time,exec_time")
    for p in performances:
        print(f"{p[0]},{p[1]},{p[2]},{p[3]},{p[4]},{p[5]}")


def run_spark_check():
    # create Spark session
    spark = SparkSession.Builder().appName("sabd_check").getOrCreate()
    # read dataset.csv
    df = spark.read.csv(
        "hdfs://master:54310/data/dataset.csv",
        inferSchema=True,
        header=True,
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
