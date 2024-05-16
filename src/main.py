from typing import Tuple
from pyspark import RDD
from pyspark.sql import Row, SparkSession
from operator import add


# Tuple:
# - [0]: date
# - [1]: serial_number
# - [2]: model
# - [3]: failure
# - [4]: vault_id
def map_filter_row(row: Row) -> Tuple:
    split = row.__str__().split(",")
    date = split[0][:10]
    serial_number = split[1]
    model = split[2]
    failure = int(split[3])
    vault_id = split[4]
    return (date, serial_number, model, failure, vault_id)


def query_1(rdd: RDD):
    # mapping into ((date, vault_id), failures) and reducing by (date, vault_id)
    reduced_rdd = rdd.map(lambda x: ((x[0], x[4]), x[3])).reduceByKey(add).cache()
    # filtering based on failures
    failures_4 = reduced_rdd.filter(lambda x: x[1] == 4).collect()
    failures_3 = reduced_rdd.filter(lambda x: x[1] == 3).collect()
    failures_2 = reduced_rdd.filter(lambda x: x[1] == 2).collect()
    # TODO: save into hdfs (currently printing just to check if it works)
    # TODO: execution time
    print("Failure 4")
    for key, value in failures_4:
        print(f"{key[0]},{key[1]},{value}")
    print("Failure 3")
    for key, value in failures_3:
        print(f"{key[0]},{key[1]},{value}")
    print("Failure 2")
    for key, value in failures_2:
        print(f"{key[0]},{key[1]},{value}")


def main():
    spark = SparkSession.Builder().appName("SABDProject1").getOrCreate()
    rdd = spark.read.text("hdfs://hdsf_master/data/dataset.csv").rdd
    header = rdd.first()  # getting header line

    # filtering header and selecting the important values
    # caching the result as it will be used for the two queries
    # (by caching, it is not recalculated each time)
    filtered_rdd = rdd.filter(lambda x: x != header).map(map_filter_row).cache()
    query_1(filtered_rdd)
    spark.stop()


if __name__ == "__main__":
    main()
