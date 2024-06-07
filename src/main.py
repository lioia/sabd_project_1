import argparse

from spark.spark import run_spark_save, run_spark_check, run_spark_analysis


def main():
    # create argument parser
    parser = argparse.ArgumentParser(description="SABD Project 1")
    # add parser for commands
    subparsers = parser.add_subparsers(title="sub-commands", dest="command")
    # save command
    save_parser = subparsers.add_parser(
        "save",
        help="Execute pipeline for saving to HDFS (running all query using RDD API)",
    )
    save_parser.add_argument(
        "location",
        type=str,
        choices=["hdfs", "mongo"],
        help="Where to save the output",
    )
    # analysis command
    subparsers.add_parser(
        "analysis",
        help="Execute pipeline for analysis",
    )
    # check command
    subparsers.add_parser(
        "check",
        help="Execute pipeline to check different API implementations",
    )
    # parse args
    args = parser.parse_args()
    # run spark job based on argument
    if args.command == "save":
        run_spark_save(args.location)
    elif args.command == "analysis":
        run_spark_analysis()
    elif args.command == "check":
        run_spark_check()


if __name__ == "__main__":
    main()
