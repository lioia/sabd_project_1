from datetime import datetime

from pyspark.sql import DataFrame


def write_to_hdfs(df: DataFrame, file: str):
    df.write.format("csv").option("header", True).save(f"hdfs://master:54310{file}")


def check_results_1(df1: DataFrame, df2: DataFrame):
    output_rdd = df1.collect()
    output_df = df2.collect()
    if len(output_rdd) != len(output_df):
        print(f"Check 1 failed: different size ({len(output_rdd)} vs {len(output_df)})")
        return
    for i in range(len(output_rdd)):
        row_rdd = output_rdd[i]
        row_df = output_df[i]
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