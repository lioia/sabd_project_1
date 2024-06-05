from typing import Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_set, concat_ws, sum, to_date


def df_preprocess(df: DataFrame) -> DataFrame:
    return (
        # select only the necessary columns
        df.select(["date", "serial_number", "model", "failure", "vault_id"])
        # remove time from date column (format YYYY-mm-DD)
        .withColumn("date", to_date(df["date"]))
    )


def query_1_df(df: DataFrame) -> DataFrame:
    df = (
        df.drop("serial_number", "model")
        # group by key (date, vault_id)
        .groupBy("date", "vault_id")
        # reduce failures
        .agg(sum("failure").alias("count"))
    )
    return (
        # filter based on number of failures
        df.filter(df["count"].isin(4, 3, 2))
        # sort with descending failures and ascending key
        .orderBy(
            ["count", "date", "vault_id"],
            ascending=[False, True, True],
        )
    )


def query_2_df(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    df_ranking_1 = (
        # select only the necessary columns
        df.drop("date", "serial_number", "vault_id")
        # group by model (key)
        .groupBy("model")
        # reduce by key
        .agg(sum("failure").alias("failures_count"))
        # order with decreasing failures_count
        .orderBy(["failures_count", "model"], ascending=[False, True])
        # limit to 10 (ranking)
        .limit(10)
    )

    vault_failures = (
        # select only the necessary columns
        df.drop("date", "serial_number", "model")
        # group by vault_id (key)
        .groupBy("vault_id")
        # reduce by key
        .agg(sum("failure").alias("failures_count"))
        # limit to 10 (ranking)
        .limit(10)
    )
    vault_models = (
        # select all the necessary columns
        df.drop("date", "serial_number")
        # filter models with failure
        .filter(df["failure"] > 0)
        # group by vault_id (key)
        .groupBy("vault_id")
        # reduce model to set
        .agg(collect_set("model"))
    )
    df_ranking_2 = (
        # join the two dataframes (based only on the vault_id in the first df)
        vault_failures.join(vault_models, "vault_id", how="left")
        # order by decreasing failures_count and increasing vault_id
        .orderBy(["failures_count", "vault_id"], ascending=[False, True])
    )

    df_ranking_2 = (
        # concatenate set to a string into list_of_models column
        df_ranking_2.withColumn(
            "list_of_models", concat_ws(",", df_ranking_2["collect_set(model)"])
        )
        # drop redundant collect_set(model) column
        .drop("collect_set(model)")
        # limit to 10 (ranking)
        .limit(10)
    )

    return df_ranking_1, df_ranking_2
