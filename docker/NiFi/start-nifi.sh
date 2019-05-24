#!/bin/bash

NIFI_DEPLOY_JAR_VERSION=1.1.32
NIFI_DEPLOY_JAR=nifi-deploy-config-${NIFI_DEPLOY_JAR_VERSION}.jar
TEMPLATE=template7.0.xml
NIFI_PORT=8080

CITY_ATTR=filter_city_attributes.groovy
DESCR=filter_description.groovy
HUMIDITY=filter_humidity.groovy
PRESSURE=filter_pressure.groovy
TEMPERATURE=filter_temperature.groovy
BLANK_SPACES=skip_blank_spaces_for_avro.groovy
LISTEN_FOR_METRICS=metrics_collector.groovy
COMPUTE_METRICS=compute_metrics.groovy


docker run -it -p ${NIFI_PORT}:8080 -d --network=hdnet --name=nifi apache/nifi:latest

docker exec -u root nifi sh -c "apt update"

docker exec nifi sh -c "wget http://www.ce.uniroma2.it/courses/sabd1819/projects/prj1_dataset.tgz; \
    tar -xvf prj1_dataset.tgz; \
    rm prj1_dataset.tgz; \
    mv prj1_dataset dataset;
    wget http://central.maven.org/maven2/com/github/hermannpencole/nifi-deploy-config/$NIFI_DEPLOY_JAR_VERSION/$NIFI_DEPLOY_JAR"

docker cp NiFi/build/config/hdfs-site.xml nifi:/opt/nifi/nifi-current/conf/hdfs-site.xml
docker cp NiFi/build/config/core-site.xml nifi:/opt/nifi/nifi-current/conf/core-site.xml
docker cp NiFi/build/config/$TEMPLATE nifi:/opt/nifi/nifi-current/$TEMPLATE
docker cp HBase/build/conf/hbase-site.xml nifi:/opt/nifi/nifi-current/hbase-site.xml

docker cp NiFi/$CITY_ATTR nifi:/opt/nifi/nifi-current/$CITY_ATTR
docker cp NiFi/$DESCR nifi:/opt/nifi/nifi-current/$DESCR
docker cp NiFi/$HUMIDITY nifi:/opt/nifi/nifi-current/$HUMIDITY
docker cp NiFi/$PRESSURE nifi:/opt/nifi/nifi-current/$PRESSURE
docker cp NiFi/$TEMPERATURE nifi:/opt/nifi/nifi-current/$TEMPERATURE
docker cp NiFi/$BLANK_SPACES nifi:/opt/nifi/nifi-current/$BLANK_SPACES
docker cp NiFi/$LISTEN_FOR_METRICS nifi:/opt/nifi/nifi-current/$LISTEN_FOR_METRICS
docker cp NiFi/$COMPUTE_METRICS nifi:/opt/nifi/nifi-current/$COMPUTE_METRICS


until $(curl --output /dev/null --silent --head --fail http://localhost:${NIFI_PORT}); do
    printf '.'
    sleep 5
done

printf '\n'

docker exec nifi sh -c "java -jar $NIFI_DEPLOY_JAR -nifi http://nifi:${NIFI_PORT}/nifi-api -conf $TEMPLATE -m deployTemplate"

