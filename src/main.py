import argparse

from spark.spark import run_spark_analysis, run_spark_check, run_spark_save

# TODO: mongo/redis


def main():
    parser = argparse.ArgumentParser(description="SABD Project 1")
    subparsers = parser.add_subparsers(title="sub-commands", dest="command")
    save_parser = subparsers.add_parser(
        "save",
        help="Execute pipeline for saving to HDFS (running all query using RDD API)",
    )
    save_parser.add_argument(
        "--format",
        type=str,
        choices=["csv", "parquet", "avro"],
        default="csv",
        help="Choose format (parquet and avro will run NiFi Flow)",
    )
    subparsers.add_parser(
        "analysis",
        help="Execute pipeline for analysis",
    )
    subparsers.add_parser(
        "check",
        help="Execute pipeline to check different API implementations",
    )
    args = parser.parse_args()
    if args.command == "save":
        run_spark_save()
    elif args.command == "analysis":
        run_spark_analysis()
    elif args.command == "check":
        run_spark_check()


if __name__ == "__main__":
    main()
