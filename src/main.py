import argparse

from spark.spark import run_spark_save, run_spark_check, run_spark_analysis
# TODO: mongo/redis


def main():
    parser = argparse.ArgumentParser(description="SABD Project 1")
    subparsers = parser.add_subparsers(title="sub-commands", dest="command")
    subparsers.add_parser(
        "save",
        help="Execute pipeline for saving to HDFS (running all query using RDD API)",
    )
    # TODO: save_parser output argument
    analysis_parser = subparsers.add_parser(
        "analysis",
        help="Execute pipeline for analysis",
    )
    analysis_parser.add_argument(
        "workers",
        type=int,
        default=3,
        help="Number of spark executors",
    )
    subparsers.add_parser(
        "check",
        help="Execute pipeline to check different API implementations",
    )
    args = parser.parse_args()
    if args.command == "save":
        run_spark_save()
    elif args.command == "analysis":
        run_spark_analysis(args.workers)
    elif args.command == "check":
        run_spark_check()


if __name__ == "__main__":
    main()
