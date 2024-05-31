#!/bin/bash

# $1 command
# $2 additional arguments: output (hdfs or mongo, only if $1 is save)

cmd=${1:-save} # first argument of Spark application
additional_arg="" # additional argument of Spark application
workers=(3) # executing application just one time with 3 workers

if [[ "$cmd" != "save" && "$cmd" != "analysis" && "$cmd" != "check" ]]; then
  echo "Error: command '$cmd' is not valid"
  exit 1
fi

if [[ "$cmd" == "save" && "$2" == "hdfs" ]]; then
  rm Results/query_1.csv
  rm Results/query_2.csv
fi

if [[ "$cmd" == "analysis" ]]; then
  # forces application to run multiple times
  workers=(1 2 3 4 5 6 7 8) 
fi

echo "Launching Application"
for w in "${workers[@]}"; do
  mongo_conf=""
  if [[ "$cmd" == "analysis" ]]; then
    echo "Executing $cmd with $w workers"
    additional_arg="$w"
  elif [[ "$cmd" == "save" ]]; then
    echo "Running $cmd with $w workers"
    additional_arg=${2:-hdfs}
    if [[ "$additional_arg" != "hdfs" && "$additional_arg" != "mongo" ]]; then
      echo "Invalid location $additional_arg, using hdfs as default"
      additional_arg="hdfs"
    fi
    if [[ "$additional_arg" == "mongo" ]]; then
      mongo_conf="--conf spark.driver.extraJavaOptions=\"-Divy.cache.dir=/tmp -Divy.home=/tmp\" \
      --packages org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 "
    fi
  fi
  docker exec spark-master sh -c \
    "/opt/spark/bin/spark-submit \
      --master spark://spark-master:7077 \
      --py-files /app/main.py,/app/spark/spark.py,/app/spark/rdd.py,/app/spark/df.py,/app/spark/utils.py \
      $mongo_conf \
      --num-executors $w \
      /app/main.py $cmd $additional_arg"
done

if [[ "$cmd" == "save" && "$additional_arg" == "hdfs" ]]; then
  echo "HDFS: copy results into local fs"
  docker exec namenode sh -c \
    "hdfs dfs -get /results/query_1/*.csv /app/results/query_1.csv"
  docker exec namenode sh -c \
    "hdfs dfs -getmerge /results/query_2_1/*.csv /results/query_2_2/*.csv /app/results/query_2.csv"
fi
