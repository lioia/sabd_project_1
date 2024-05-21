from typing import Tuple
from pyspark.sql import DataFrame
from pyspark.sql.functions import collect_set, concat_ws, desc, sum


def query_1_df(df: DataFrame) -> DataFrame:
    return (
        # group by key (date, vault_id)
        df.groupBy(["date_no_time", "vault_id"])
        # reduce failures
        .agg(sum("failure"))
        # rename sum(failure) to count
        .withColumnRenamed("sum(failure)", "count")
        # filter based on number of failures
        .filter((df["count"] == 4) | (df["count"] == 3) | (df["count"] == 2))
        # sort with descending failures and ascending key
        .orderBy(
            ["count", "date", "vault_id"],
            ascending=[False, True, True],
        )
        # retarget to single partition
        .coalesce(1)
    )


def query_2_df(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    df_ranking_1 = (
        # select only the necessary columns
        df.select(["model", "failure"])
        # group by model (key)
        .groupBy("model")
        # reduce by key
        .agg(sum("failure"))
        # rename to failures_count
        .withColumnRenamed("sum(failure)", "failures_count")
        # order with decreasing failures_count
        .orderBy(desc("failures_count"))
        # retarget to single partition
        .coalesce(1)
        # limit to 10 (ranking)
        .limit(10)
    )

    vault_failures = (
        # select only the necessary columns
        df.select(["vault_id", "failure"])
        # group by vault_id (key)
        .groupBy("vault_id")
        # reduce by key
        .agg(sum("failure"))
        # rename to failures_count
        .withColumnRenamed("sum(failure)", "failures_count")
        # order with decreasing failures_count
        .orderBy(desc("failures_count"))
        # retarget to single partition
        .coalesce(1)
        # limit to 10 (ranking)
        .limit(10)
    )
    vault_models = (
        # filter models with failure
        df.filter(df["failure"] > 0)
        # select necessary columns
        .select(["vault_id", "model"])
        # group by vault_id (key)
        .groupBy("vault_id")
        # reduce model to set
        .agg(collect_set("model"))
    )
    vault_models = (
        vault_models
        # create new column list_of_models concatenating the set of models
        .withColumn(
            "list_of_models",
            concat_ws(",", vault_models["collect_set(model)"]),
        )
        # remove collect_set(model) column
        .drop("collect_set(model)")
    )
    df_ranking_2 = (
        # join the two dataframes (based only on the vault_id in the first df)
        vault_failures.join(vault_models, ["vault_id"], how="left")
        # order by decreasing failures_count and increasing vault_id
        .orderBy(["failures_count", "vault_id"], ascending=[False, True])
    )

    return df_ranking_1, df_ranking_2
