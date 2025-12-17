#!/bin/bash

# connect to spark master and execute commands inside the container
docker exec -it spark-master bash -c "
mkdir -p /opt/spark/.ivy2_cache

/opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
    --conf spark.jars.ivy=/opt/spark/.ivy2_cache \
    /opt/spark-apps/spark_stream.py
"