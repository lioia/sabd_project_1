from datetime import datetime
from typing import Callable, Dict

from pyspark.sql import DataFrame, SparkSession


def load_dataset(spark: SparkSession, filename: str) -> DataFrame:
    # assign each file format into the corresponding reading function
    format_map: Dict[str, Callable[..., DataFrame]] = {
        "csv": spark.read.option("inferSchema", True).option("header", True).csv,
        "parquet": spark.read.parquet,
        "avro": spark.read.format("avro").load,
    }
    # get the file format
    format = filename.split(".")[-1]
    # call the read function for the file format
    return format_map[format](f"hdfs://master:54310/data/{filename}")


def save_to_hdfs(df: DataFrame, file: str):
    (
        # write CSV file
        df.write.format("csv")
        # overwrite if it already exists
        .mode("overwrite")
        # include header
        .option("header", True)
        # save to HDFS
        .save(f"hdfs://master:54310{file}")
    )


def save_to_mongo(df: DataFrame, collection: str, mode="overwrite"):
    (
        # write to mongo
        df.write.format("mongodb")
        # overwrite or append
        .mode(mode)
        # to database
        .option("database", "spark")
        # to collection
        .option("collection", collection)
        .save()
    )


def check_results_1(df1: DataFrame, df2: DataFrame):
    # get results from RDD
    output_rdd = df1.collect()
    # get results from DataFrame
    output_df = df2.collect()
    # check size
    if len(output_rdd) != len(output_df):
        print(f"Check 1 failed: different size ({len(output_rdd)} vs {len(output_df)})")
        return
    # iterate through all the results
    for i in range(len(output_rdd)):
        row_rdd = output_rdd[i]  # get RDD row
        row_df = output_df[i]  # get DataFrame Row
        # check each value
        if (
            row_rdd["date"] != datetime.strftime(row_df["date"], "%Y-%m-%d")
            or row_rdd["vault_id"] != row_df["vault_id"]
            or row_rdd["count"] != int(row_df["count"])
        ):
            print(f"Check 1 failed at {i}: {row_rdd} vs {row_df}")
            return
    print("Check 1 success")


def check_results_2_1(df1: DataFrame, df2: DataFrame):
    output_rdd = df1.collect()
    output_df = df2.collect()
    if len(output_rdd) != len(output_df):
        print(
            f"Check 2 Ranking 1 failed: different size ({len(output_rdd)} vs {len(output_df)})"
        )
        return
    for i in range(len(output_rdd)):
        row_rdd = output_rdd[i]
        row_df = output_df[i]
        if row_rdd["model"] != row_df["model"] or row_rdd["failures_count"] != int(
            row_df["failures_count"]
        ):
            print(f"Check 2 Ranking 1 failed at {i}: {row_rdd} vs {row_df}")
            return
    print("Check 2 Ranking 1 success")


def check_results_2_2(df1: DataFrame, df2: DataFrame):
    output_rdd = df1.collect()
    output_df = df2.collect()
    if len(output_rdd) != len(output_df):
        print(
            f"Check 2 Ranking 2 failed: different size ({len(output_rdd)} vs {len(output_df)})"
        )
        return
    for i in range(len(output_rdd)):
        row_rdd = output_rdd[i]
        row_df = output_df[i]
        models_1 = row_rdd["list_of_models"].split(",")
        models_2 = row_df["list_of_models"].split(",")
        if (
            row_rdd["vault_id"] != row_df["vault_id"]
            or row_rdd["failures_count"] != row_df["failures_count"]
            or set(models_1) != set(models_2)
        ):
            print(f"Check 2 Ranking 2 failed at {i}: {row_rdd} vs {row_df}")
            return
    print("Check 2 Ranking 2 success")
