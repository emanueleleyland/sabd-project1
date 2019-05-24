# SABD_PROJECT_1


## Local deployment
To deploy architecture locally you can use Docker. Startup scripts are available in /docker directory. Run `./start-all.sh` to start all architecture. Use `./stop-all.sh` instead to stop all containers.
#### Nifi configuration
To configure NiFi set file core-site.xml file in the directory NiFi/build/config/ set property `fs.default.name` to `hdfs://master:54310`.
Once Nifi container is started, a pre-configured template is installed on it. To access to the UI connect to localhost:8080/nifi and enable enable all the controller 
services as described at https://nifi.apache.org/docs/nifi-docs/html/user-guide.html#Enabling_Disabling_Controller_Services.
Once all controller services are running, start all the processors and let the data flow to HDFS. 
#### HBase configuration
To configure NiFi in file hbase-site.xml in the directory HBase/build/conf/ set property `hbase.rootdir` to `hdfs://master:54310` to use pseudo-locally distributed storage for HBase.
#### HDFS configuration
To configure HDFS set file core-site.xml file in the directory HDFS/build/config/ set property `fs.default.name` to `hdfs://master:54310`.
#### Spark deployment
In `./start-spark.sh` configure SPARK_APPLICATION_JAR_LOCATION environment variable with the application JAR path.
Once dataset is stored in HDFS execute script `./start-spark.sh` to run a pseudo-locally distributed cluster.

## Cloud Deployment
Several scripts are available in directory /aws/. Install awscli and configure it with command `aws configure`. You need to install boto3 tool via `pip` to run scripts. 
To setup VPC and emr cluster start script `start-emr.py` from /aws/ directory. Once EMR cluster is in waiting phase you can access master node via ssh.
To configure NiFi set in docker/NiFi/build/config/ `fs.default.name` property with master DNS address. Then run `start-nifi.py`. When Nifi instance is completely configured, access it via ssh and follow the instruction in Nifi configuration section to start all the controller services and processors.

When data are imported in HDFS. Load JAR to S3 and than submit a new step to EMR cluster as it follow setting the number of desired runs to complete:
```
spark-submit --deploy-mode cluster --packages org.apache.spark:spark-avro_2.12:2.4.3 
--class Main s3://PATH-TO-JAR master-dns-address:8020 nruns
```
