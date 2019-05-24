#!/bin/bash
docker network create --driver bridge --subnet 172.19.0.0/16 hdnet
docker run -t -i -p 9864:9864 -p 9866:9866 -d --network=hdnet --name=slave1 effeerre/hadoop
docker run -t -i -p 9863:9864 -p 9865:9866 -d --network=hdnet --name=slave2 effeerre/hadoop
docker run -t -i -p 9862:9864 -p 9867:9866 -d --network=hdnet --name=slave3 effeerre/hadoop
docker run -t -i -p 9870:9870 -d -p 54310:54310 --network=hdnet --name=master effeerre/hadoop


docker cp HDFS/build/config/hdfs-site.xml master:/usr/local/hadoop/etc/hadoop/hdfs-site.xml
docker cp HDFS/build/config/core-site.xml master:/usr/local/hadoop/etc/hadoop/core-site.xml


docker exec master /bin/bash -c "hdfs namenode -format -force; \$HADOOP_HOME/sbin/start-dfs.sh"

