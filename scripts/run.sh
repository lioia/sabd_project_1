#!/bin/bash

echo "Starting Docker Compose"
docker compose up -d

echo "HDFS: mesg ttyname failed fix"
docker exec hdfs-namenode sh -c \
  "echo '#!/bin/sh' > /usr/bin/mesg && chmod 755 /usr/bin/mesg"

echo "HDFS: formatting"
docker exec hdfs-namenode sh -c "hdfs namenode -format"

echo "HDFS: starting"
docker exec hdfs-namenode sh -c "/usr/local/hadoop/sbin/start-dfs.sh"

echo "HDFS: copy dataset"
docker exec hdfs-namenode sh -c "hdfs dfs -put /data/dataset.csv /dataset.csv"

echo "Spark: launching master"
docker exec spark-master sh -c "/opt/spark/sbin/start-master.sh"

echo "Spark: launching worker 1"
docker exec spark-worker-1 sh -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Spark: launching worker 2"
docker exec spark-worker-2 sh -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

echo "Spark: launching worker 3"
docker exec spark-worker-3 sh -c "/opt/spark/sbin/start-worker.sh spark://spark-master:7077"

# echo "Launching Application"
# docker exec spark-master sh -c \
#   "/opt/spark/bin/spark-submit --master spark://spark-master:7077 /data/sabd/main.py"
