#!/bin/bash

# $1 command

cmd=${1:-save}

if [[ "$cmd" != "save" && "$cmd" != "analysis" && "$cmd" != "check" ]]; then
  echo "Error: command '$cmd' is not valid"
  exit 1
fi

echo "Cleaning results"
rm ./Results/*.csv

echo "Starting Docker Compose"
docker compose -d

echo "HDFS: mesg ttyname failed fix"
docker exec namenode sh -c \
  "echo '#!/bin/sh' > /usr/bin/mesg && chmod 755 /usr/bin/mesg"

echo "HDFS: formatting"
docker exec namenode sh -c \
  "hdfs namenode -format"

echo "HDFS: starting"
docker exec namenode sh -c \
  "/usr/local/hadoop/sbin/start-dfs.sh"

echo "HDFS: creating folders"
docker exec namenode sh -c \
  "hdfs dfs -mkdir /data"
docker exec namenode sh -c \
  "hdfs dfs -mkdir /filtered"
docker exec namenode sh -c \
  "hdfs dfs -mkdir /results"

echo "HDFS: chown folders"
docker exec namenode sh -c \
  "hdfs dfs -chown nifi /data"
docker exec namenode sh -c \
  "hdfs dfs -chown nifi /filtered"
docker exec namenode sh -c \
  "hdfs dfs -chown spark /results"

echo "HDFS: copy dataset"
docker exec namenode sh -c \
  "hdfs dfs -put /app/data/dataset.csv /data/dataset.csv"

echo "NiFi: running flow"
python -m venv .venv > /dev/null
source .venv/bin/activate > /dev/null
pip install -r requirements.txt > /dev/null
python3 src/nifi/nifi.py

echo "Spark: launching master"
docker exec spark-master sh -c \
  "/opt/spark/sbin/start-master.sh"

echo "Spark: launching worker"
docker exec spark-worker sh -c \
  "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Launching Application"
docker exec spark-master sh -c \
  "/opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    /app/main.py $cmd"

echo "HDFS: copy results into local fs"
docker exec namenode sh -c \
  "hdfs dfs -get /results/query_1/*.csv /app/results/query_1.csv"
docker exec namenode sh -c \
  "hdfs dfs -getmerge /results/query_2_1/*.csv /results/query_2_2/*.csv /app/results/query_2.csv"
