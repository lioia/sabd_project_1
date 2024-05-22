from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date

from rdd import query_1_rdd, query_2_rdd
from df import query_1_df, query_2_df
from utils import check_results_1, check_results_2_1, check_results_2_2, write_to_hdfs


def main():
    spark = SparkSession.Builder().appName("SABDProject1").getOrCreate()
    df = spark.read.csv("hdfs://master:54310/data/dataset.csv", header=True)
    # TODO: calculate execution time for both queries

    # RDD API
    rdd = (
        # filter all the headers (every ~60k events there is a header)
        df.rdd.filter(lambda x: x[4].isdecimal())
        # mapping into (date, serial_number, model, failure, vault_id)
        # date is truncated into the format YYYY-MM-DD
        .map(lambda x: (x[0][:10], x[1], x[2], int(x[3]), x[4]))
        # caching as it is required by the two queries
        .cache()
    )
    q1_rdd_df = query_1_rdd(rdd)
    q2_1_rdd_df, q2_2_rdd_df = query_2_rdd(rdd)

    # Write results to HDFS
    write_to_hdfs(q1_rdd_df, "/results/query_1/")
    write_to_hdfs(q2_1_rdd_df, "/results/query_2_1")
    write_to_hdfs(q2_2_rdd_df, "/results/query_2_2")

    # DataFrame API
    df = (
        # select only the necessary columns
        df.select(["date", "serial_number", "model", "failure", "vault_id"])
        # create new column date_no_time to map date column (remove time)
        .withColumn("date_no_time", to_date(df["date"]))
        # drop date column
        .drop("date")
        # rename date_no_time to date
        .withColumnRenamed("date_no_time", "date")
        .cache()
    )
    q1_df = query_1_df(df)
    q2_1_df, q2_2_df = query_2_df(df)

    # Result check
    check_results_1(q1_rdd_df, q1_df)  # query 1
    check_results_2_1(q2_1_rdd_df, q2_1_df)  # query 2 ranking 1
    check_results_2_2(q2_2_rdd_df, q2_2_df)  # query 2 ranking 2

    # Stop Spark
    spark.stop()


if __name__ == "__main__":
    main()
