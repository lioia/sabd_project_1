from typing import Tuple, Iterable
from pyspark import RDD
from pyspark.sql import SparkSession
from operator import add


def add_header(header: str, index: int, itr: Iterable[Tuple[Tuple[str, str], int]]):
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
    # mapping into ((date, vault_id), failures) and reducing by (date, vault_id)
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
    # TODO: save into hdfs (currently printing just to check if it works)
    # TODO: execution time
    header = "DD-MM-YYYY,vault id,count"
    # add header to the data and save it to HDFS
    output_rdd.mapPartitionsWithIndex(
        lambda i, itr: add_header(header, i, itr)
    ).saveAsTextFile("hdfs://hdsf_master/results/query_1.csv")


def main():
    spark = SparkSession.Builder().appName("SABDProject1").getOrCreate()
    rdd = (
        spark.read.csv("hdfs://hdfs_master/data/dataset.csv")
        .rdd.filter(lambda x: x[4].isdecimal())
        .map(lambda x: (x[0][:10], x[1], x[2], int(x[3]), x[4]))
        .cache()
    )
    query_1(rdd)
    spark.stop()


if __name__ == "__main__":
    main()
