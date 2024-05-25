from typing import Callable, Dict
from pyspark.sql import DataFrame, SparkSession


from rdd import query_1_rdd, query_2_rdd
from df import query_1_df, query_2_df


def load_dataset(spark: SparkSession, format: str) -> DataFrame:
    format_map: Dict[str, Callable[..., DataFrame]] = {
        "csv": spark.read.option("inferSchema", True).csv,
        "parquet": spark.read.parquet,
        "avro": spark.read.format("avro").load,
    }
    return format_map[format](f"hdfs://master:54310/filtered/dataset.{format}")


api_query_map: Dict[str, Dict[int, Callable]] = {
    "rdd": {
        1: query_1_rdd,
        2: query_2_rdd,
    },
    "df": {
        1: query_1_df,
        2: query_2_df,
    },
}
