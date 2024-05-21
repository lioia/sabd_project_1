from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, file: str):
    df.write.format("csv").option("header", True).save(f"hdfs://master:54310{file}")
