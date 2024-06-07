from typing import Tuple
from operator import add

from pyspark import RDD
from pyspark.sql import DataFrame


# Tuple:
# - [0]: date
# - [1]: serial_number
# - [2]: model
# - [3]: failure
# - [4]: vault_id


def rdd_preprocess(df: DataFrame) -> RDD[Tuple[str, str, str, int, str]]:
    return (
        # filter all the headers (every ~600k events there is a header)
        df.rdd.filter(lambda x: str(x[4]).isdecimal())
        # mapping into (date, serial_number, model, failure, vault_id)
        # date is truncated into the format YYYY-MM-DD
        .map(lambda x: (str(x[0])[:10], x[1], x[2], int(x[3]), x[4]))
    )


def query_1_rdd(rdd: RDD[Tuple[str, str, str, int, str]]) -> DataFrame:
    output_rdd = (
        # mapping into ((date, vault_id), failures)
        rdd.map(lambda x: ((x[0], x[4]), x[3]))
        # reducing by key (date, vault_id)
        .reduceByKey(add)
        # filtering based on requested values
        .filter(lambda x: x[1] in {2, 3, 4})
        # flattening key
        .map(lambda x: (x[0][0], x[0][1], x[1]))
        # sorting by value, then by date, then by vault_id
        .sortBy(lambda x: (-x[2], x[0], x[1]))  # type: ignore[type-var]
    )
    # convert to DataFrame to save as CSV
    return output_rdd.toDF(["date", "vault_id", "count"])


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
    df_ranking_1 = (
        # converting to DataFrame to assign names to the columns
        ranking_1.toDF(["model", "failures_count"])
        # limit to 10 results
        .limit(10)
    )

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
        # mapping models into a set (remove repetitions)
        .mapValues(set)
    )

    ranking_2 = (
        # join the two RDDs
        vault_failures.join(vault_models)
        # mapping into (vault_id, failures_count, list_of_models)
        # where list_of_models is obtained by joining the set of models
        .map(lambda x: (x[0], int(x[1][0]), ",".join(x[1][1])))
        # sort by decreasing failures count and increasing vault ids
        .sortBy(lambda x: (-x[1], x[0]))  # type: ignore[type-var]
    )
    df_ranking_2 = (
        # convert to DataFrame to assign names to the columns
        ranking_2.toDF(["vault_id", "failures_count", "list_of_models"])
        # limit results to 10
        .limit(10)
    )

    return df_ranking_1, df_ranking_2
