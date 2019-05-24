#!/usr/bin/env bash

SPARK_APPLICATION_JAR="Project1-1.0-SNAPSHOT.jar"
SPARK_APPLICATION_JAR_LOCATION=/${SPARK_APPLICATION_JAR}


docker-compose -f Spark/docker-compose.yml up -d

docker cp Spark/${SPARK_APPLICATION_JAR_LOCATION} spark-master:${SPARK_APPLICATION_JAR_LOCATION}
docker cp Spark/${SPARK_APPLICATION_JAR_LOCATION} spark-worker-1:${SPARK_APPLICATION_JAR_LOCATION}
docker cp Spark/${SPARK_APPLICATION_JAR_LOCATION} spark-worker-2:/${SPARK_APPLICATION_JAR_LOCATION}
docker cp Spark/${SPARK_APPLICATION_JAR_LOCATION} spark-worker-3:/${SPARK_APPLICATION_JAR_LOCATION}

docker exec spark-master sh -c "spark/bin/spark-submit --master spark://spark-master:7077 --packages org.apache.spark:spark-avro_2.12:2.4.3 ${SPARK_APPLICATION_JAR_LOCATION}"
