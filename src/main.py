from typing import Tuple, Iterable
from pyspark import RDD
from pyspark.sql import SparkSession
from operator import add


def add_header(header: str, index: int, itr: Iterable):
    if index == 0:
        yield header
    yield from itr


# Tuple:
# - [0]: date
# - [1]: serial_number
# - [2]: model
# - [3]: failure
# - [4]: vault_id


def query_1(rdd: RDD[Tuple[str, str, str, int, str]]):
    output_rdd = (
        # mapping into ((date, vault_id), failures)
        rdd.map(lambda x: ((x[0], x[4]), x[3]))
        # reducing by key (date, vault_id)
        .reduceByKey(add)
        # filtering based on requested values
        .filter(lambda x: x[1] == 4 or x[1] == 3 or x[1] == 2)
        # sorting by value, then by date, then by vault_id
        .sortBy(lambda x: (-x[1], x[0][0], x[0][1]))  # type: ignore[type-var]
    )
    # add header to the data and save it to HDFS
    header = "# DD-MM-YYYY,vault id,count"
    output_rdd.mapPartitionsWithIndex(
        lambda i, itr: add_header(header, i, itr)
    ).saveAsTextFile("hdfs://hdsf_master/results/query_1.csv")


def query_2(spark: SparkSession, rdd: RDD[Tuple[str, str, str, int, str]]):
    ranking_1 = (
        # mapping into (model, failure)
        rdd.map(lambda x: (x[2], x[3]))
        # summing all the failures for the specific model
        .reduceByKey(add)
        # sorting by the failure count
        .sortBy(lambda x: -x[1])  # type: ignore[type-var]
    )

    # Ranking 2
    vault_failures = (
        # mapping into (vault_id, failures)
        rdd.map(lambda x: (x[4], x[3]))
        # summing all the failures in the vault
        .reduceByKey(add)
        # sorting by the number of failures
        .sortBy(lambda x: -x[1])  # type: ignore[type-var]
    )
    vault_model_failures = (
        # filtering the models without a failure
        rdd.filter(lambda x: x[3] > 0)
        # mapping into (vault_id, model)
        .map(lambda x: (x[4], x[2]))
        # grouping into (vault_id, list_of_models)
        .groupByKey()
        # transform Spark Iterable into an actual list
        .mapValues(list)
    )

    header_ranking_1 = "# model,failures_count"
    output_ranking_1 = (
        # mapping into the same structure as the other ranking
        ranking_1.map(lambda x: (x[0], x[1], None))
        # adding header
        .mapPartitionsWithIndex(lambda i, itr: add_header(header_ranking_1, i, itr))
        # taking the first 10 (+1 header)
        .take(11)
    )

    header_ranking_2 = "# vault_id,failures_count,list_of_models"
    output_ranking_2 = (
        # joining the tww RDDs into (vault_id, (failures_count, list_of_models))
        vault_failures.join(vault_model_failures)
        # flatten into (vault_id, failures_count, list_of_models)
        .map(lambda x: (x[0], x[1][0], x[1][1]))
        # adding header
        .mapPartitionsWithIndex(lambda i, itr: add_header(header_ranking_2, i, itr))
        # taking the first 10 (+1 header)
        .take(11)
    )
    # merge the two rankings and save into HDFS
    spark.sparkContext.parallelize([output_ranking_1, output_ranking_2]).saveAsTextFile(
        "hdfs://hdsf_master/results/query_2.csv"
    )
    pass


def main():
    spark = SparkSession.Builder().appName("SABDProject1").getOrCreate()
    rdd = (
        # read from HDFS
        spark.read.csv("hdfs://hdfs_master/data/dataset.csv")
        # filter all the headers (every ~60k events there is a header)
        .rdd.filter(lambda x: x[4].isdecimal())
        # mapping into (date, serial_number, model, failure, vault_id)
        # date is truncated into the format YYYY-MM-DD
        .map(lambda x: (x[0][:10], x[1], x[2], int(x[3]), x[4]))
        # caching as it is required by the two queries
        .cache()
    )
    # TODO: calculate execution time for both queries
    query_1(rdd)
    query_2(spark, rdd)
    spark.stop()


if __name__ == "__main__":
    main()
