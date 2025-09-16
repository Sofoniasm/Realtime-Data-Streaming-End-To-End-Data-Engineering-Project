#!/usr/bin/env bash
set -e
MASTER_ID=$(docker ps -qf "name=spark-master")
if [ -z "$MASTER_ID" ]; then
  echo "spark-master container not found"
  exit 1
fi

docker exec -i "$MASTER_ID" spark-submit /opt/spark/app/streaming_job.py
