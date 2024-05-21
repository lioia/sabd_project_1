from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, file: str):
    df.write.format("csv").option("header", True).save(f"hdfs://master:54310{file}")


def check_results(df1: DataFrame, df2: DataFrame):
    return df1.schema == df2.schema and df1.collect() == df2.collect()
