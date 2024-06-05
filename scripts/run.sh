#!/bin/bash

# $1 command
# $2 additional arguments: output (hdfs or mongo, only if $1 is save)

cmd=${1:-save} # first argument of Spark application
additional_arg="" # additional argument of Spark application
w=2

if [[ "$cmd" != "save" && "$cmd" != "analysis" && "$cmd" != "check" ]]; then
  echo "Error: command '$cmd' is not valid"
  exit 1
fi

if [[ "$cmd" == "save" && "$2" == "hdfs" ]]; then
  rm Results/query_1.csv
  rm Results/query_2.csv
fi

if [[ "$cmd" == "save" ]]; then
  additional_arg=${2:-hdfs}
  # set default location to hdfs
  if [[ "$additional_arg" != "hdfs" && "$additional_arg" != "mongo" ]]; then 
    echo "Invalid location $additional_arg, using hdfs as default"
    additional_arg="hdfs"
  fi
fi

if [[ "$cmd" == "analysis" ]]; then
  w=${2:-2}
fi

echo "Executing $cmd with $w workers"

docker exec spark-master sh -c \
  "/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --py-files /app/main.py,/app/spark/spark.py,/app/spark/rdd.py,/app/spark/df.py,/app/spark/utils.py \
    --conf \"spark.cores.max=$w\" \
    --conf \"spark.executor.cores=1\" \
    --conf spark.driver.extraJavaOptions=\"-Divy.cache.dir=/tmp -Divy.home=/tmp\" \
    --packages org.apache.spark:spark-avro_2.12:3.5.1,org.mongodb.spark:mongo-spark-connector_2.12:10.3.0 \
    /app/main.py $cmd $additional_arg"

if [[ "$cmd" == "save" && "$additional_arg" == "hdfs" ]]; then
  echo "HDFS: copy results into local fs"
  docker exec namenode sh -c \
    "hdfs dfs -get /results/query_1/*.csv /app/results/query_1.csv"
  docker exec namenode sh -c \
    "hdfs dfs -getmerge /results/query_2_1/*.csv /results/query_2_2/*.csv /app/results/query_2.csv"
fi
