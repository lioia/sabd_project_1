#!/bin/bash

echo "Cleaning results"
rm ./Results/*.csv

echo "Starting Docker Compose"
docker compose up -d

echo "HDFS: mesg ttyname failed fix"
docker exec hdfs-namenode sh -c \
  "echo '#!/bin/sh' > /usr/bin/mesg && chmod 755 /usr/bin/mesg"

echo "HDFS: formatting"
docker exec hdfs-namenode sh -c \
  "hdfs namenode -format"

echo "HDFS: starting"
docker exec hdfs-namenode sh -c \
  "/usr/local/hadoop/sbin/start-dfs.sh"

echo "HDFS: creating folders"
docker exec hdfs-namenode sh -c \
  "hdfs dfs -mkdir /data"
docker exec hdfs-namenode sh -c \
  "hdfs dfs -mkdir /results && hdfs dfs -chown spark /results"

echo "HDFS: copy dataset"
docker exec hdfs-namenode sh -c \
  "hdfs dfs -put /app/data/dataset.csv /data/dataset.csv"

echo "Spark: launching master"
docker exec spark-master sh -c \
  "/opt/spark/sbin/start-master.sh"

echo "Spark: launching worker 1"
docker exec spark-worker-1 sh -c \
  "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Spark: launching worker 2"
docker exec spark-worker-2 sh -c \
  "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Spark: launching worker 3"
docker exec spark-worker-3 sh -c \
  "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Launching Application"
docker exec spark-master sh -c \
  "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /app/main.py"

echo "HDFS: copy results into local fs"
docker exec hdfs-namenode sh -c \
  "hdfs dfs -get /results/query_1/*.csv /app/results/query_1.csv"
docker exec hdfs-namenode sh -c \
  "hdfs dfs -getmerge /results/query_2_1/*.csv /results/query_2_2/*.csv /app/results/query_2.csv"
