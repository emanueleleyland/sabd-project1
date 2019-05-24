#!/usr/bin/env bash
./NiFi/stop-nifi.sh
./HBase/stop-hbase.sh
./stop-spark.sh
./HDFS/stop-hdfs.sh
