from typing import Tuple
from pyspark import RDD
from operator import add

from pyspark.sql import DataFrame


# Tuple:
# - [0]: date
# - [1]: serial_number
# - [2]: model
# - [3]: failure
# - [4]: vault_id


def query_1_rdd(rdd: RDD[Tuple[str, str, str, int, str]]):
    output_rdd = (
        # mapping into ((date, vault_id), failures)
        rdd.map(lambda x: ((x[0], x[4]), x[3]))
        # reducing by key (date, vault_id)
        .reduceByKey(add)
        # filtering based on requested values
        .filter(lambda x: x[1] == 4 or x[1] == 3 or x[1] == 2)
        # flattening key
        .map(lambda x: (x[0][0], x[0][1], x[1]))
        # sorting by value, then by date, then by vault_id
        .sortBy(lambda x: (-x[2], x[0], x[1]))  # type: ignore[type-var]
    )
    # convert to DataFrame to save as CSV
    return output_rdd.toDF(["date", "vault_id", "count"]).coalesce(1)


def query_2_rdd(
    rdd: RDD[Tuple[str, str, str, int, str]],
) -> Tuple[DataFrame, DataFrame]:
    ranking_1 = (
        # mapping into (model, failure)
        rdd.map(lambda x: (x[2], x[3]))
        # summing all the failures for the specific model
        .reduceByKey(add)
        # sorting by the failure count
        .sortBy(lambda x: (-x[1], x[0]))  # type: ignore[type-var]
    )
    df_ranking_1 = ranking_1.toDF(["model", "failures_count"]).coalesce(1).limit(10)

    # Ranking 2
    vault_failures = (
        # mapping into (vault_id, failures)
        rdd.map(lambda x: (x[4], x[3]))
        # summing all the failures in the vault
        .reduceByKey(add)
    )
    vault_models = (
        # filtering the models without a failure
        rdd.filter(lambda x: x[3] > 0)
        # mapping into (vault_id, model)
        .map(lambda x: (x[4], x[2]))
        # grouping into (vault_id, list_of_models)
        .groupByKey()
        .mapValues(set)
    )

    ranking_2 = (
        vault_failures.join(vault_models)
        .map(lambda x: (x[0], int(x[1][0]), ",".join(x[1][1])))
        .sortBy(lambda x: (-x[1], x[0]))  # type: ignore[type-var]
    )
    df_ranking_2 = (
        ranking_2.toDF(["vault_id", "failures_count", "list_of_models"])
        .coalesce(1)
        .limit(10)
    )

    return df_ranking_1, df_ranking_2
