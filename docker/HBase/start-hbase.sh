#!/usr/bin/env bash

#-p 8080:8080
docker run -ti --name=hbase-docker -h hbase-docker -d --network=hdnet -p 2181:2181  -p 8085:8085 \
    -p 9090:9090 -p 9095:9095 -p 16000:16000 -p 16010:16010 -p 16201:16201 -p 16301:16301 harisekhon/hbase:1.4

docker exec -u root hbase-docker sh -c "/hbase/bin/stop-hbase.sh"

docker cp HBase/build/conf/hbase-site.xml hbase-docker:/hbase/conf/hbase-site.xml

docker cp HBase/build/conf/create_tables.sh hbase-docker:/create_tables.sh

docker exec hbase-docker sh -c "/hbase/bin/start-hbase.sh"
sleep 10
docker exec hbase-docker sh -c "hbase shell create_tables.sh"
