import boto3
from zipfile import ZipFile
from config import KEY_PEM, NIFI_BUCKET, HOSTEDZONEID

# This script has to be launched after start-emr.py. It creates one NiFi node in the same subnet of EMR cluster

AMI_NIFI = 'ami-090f10efc254eaf55'
INSTANCE_TYPE_NIFI = 't3.large'

# Filtering scripts

filter_city_attributes = '../docker/NiFi/filter_city_attributes.groovy'
filter_description = '../docker/NiFi/filter_description.groovy'
filter_humidity = '../docker/NiFi/filter_humidity.groovy'
filter_pressure = '../docker/NiFi/filter_pressure.groovy'
filter_temperature = '../docker/NiFi/filter_temperature.groovy'
skip_blank = '../docker/NiFi/skip_blank_spaces_for_avro.groovy'

# Metrics script

compute_metrics = '../docker/NiFi/compute_metrics.groovy'
metrics_collector = '../docker/NiFi/metrics_collector.groovy'

# Configuration files

hdfs_site = '../docker/NiFi/build/config/hdfs-site.xml'
core_site = '../docker/NiFi/build/config/core-site.xml'
hbase_site = '../docker/HBase/build/conf/hbase-site.xml'
template = '../docker/NiFi/build/config/template7.0.xml'
ec2 = boto3.resource('ec2')
ec2_client = boto3.client('ec2')

# Create S3 bucket for NiFi configuration file if not exists yet

s3 = boto3.client('s3')

buckets = s3.list_buckets()
found = False
for bucket in buckets['Buckets']:
	if bucket['Name'] == NIFI_BUCKET:
		found = True
		break

if not found:
	s3.create_bucket(
		Bucket=NIFI_BUCKET,
		CreateBucketConfiguration={
			'LocationConstraint': 'eu-central-1'
		}
	)

zip_name = "nifi-conf.zip"
file_list = [filter_city_attributes, filter_description, filter_humidity, filter_pressure, filter_temperature,
			 skip_blank, hdfs_site, core_site, hbase_site, template, compute_metrics, metrics_collector]

with ZipFile(zip_name, 'w') as zip:
	for file in file_list:
		zip.write(file)

# Upload all the configuration file to S3

s3 = boto3.resource('s3')
s3.meta.client.upload_file("nifi-conf.zip", NIFI_BUCKET, 'nifi-conf.zip', ExtraArgs={'ACL': 'public-read'})

filters = [{'Name': 'tag:Name', 'Values': ['vpc']}]
vpcs = ec2_client.describe_vpcs(Filters=filters)
vpc_id = vpcs['Vpcs'][0]['VpcId']

filters = [{'Name': 'tag:Name', 'Values': ['subnet']}]
subnets = ec2_client.describe_subnets(Filters=filters)
subnet_id = subnets['Subnets'][0]['SubnetId']

# Create a security group for the NiFi instance enabling inbound traffic for ssh connection (port 22) and
# for web UI interface (port 8080)

nifi_sg = ec2.create_security_group(
	GroupName='nifi', Description='nifi sec group', VpcId=vpc_id)
nifi_sg.authorize_ingress(
	IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
print(nifi_sg.id)


user_data_nifi = """#!/bin/bash
sudo -s;
apt-get update;
apt-get install -y openjdk-8-jdk;
apt install -y unzip;
wget http://it.apache.contactlab.it/nifi/1.9.2/nifi-1.9.2-bin.tar.gz;
tar xvzf nifi-1.9.2-bin.tar.gz;
rm nifi-1.9.2-bin.tar.gz;
mv nifi-1.9.2/ nifi-current;
mkdir -p /opt/nifi/;
mv nifi-current/ /opt/nifi/;
echo JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64/" >> /etc/environment;
source /etc/environment;
sudo su ubuntu -c "source /etc/environment";
cd /opt/nifi/nifi-current/;
chmod 777 *;
wget http://www.ce.uniroma2.it/courses/sabd1819/projects/prj1_dataset.tgz;
tar -xvf prj1_dataset.tgz;
rm prj1_dataset.tgz;
mv prj1_dataset/ dataset
wget http://central.maven.org/maven2/com/github/hermannpencole/nifi-deploy-config/1.1.32/nifi-deploy-config-1.1.32.jar;
wget https://s3.eu-central-1.amazonaws.com/sabd-nifi-conf/nifi-conf.zip;
unzip nifi-conf.zip;
mv docker/HBase/build/conf/hbase-site.xml .;
mv docker/NiFi/build/config/core-site.xml conf/;
mv docker/NiFi/build/config/hdfs-site.xml conf/;
mv docker/NiFi/build/config/template7.0.xml .;
rm -r  docker/NiFi/build/;
mv -v docker/NiFi/* .;
bin/nifi.sh start;
until $(curl --output /dev/null --silent --head --fail http://localhost:8080); do
printf '.';
sleep 5;
done;
printf '\n';
java -jar nifi-deploy-config-1.1.32.jar -nifi http://localhost:8080/nifi-api -conf template7.0.xml -m deployTemplate;
"""

# Launch EC2 instance

nifi_instance = ec2.create_instances(
	InstanceType=INSTANCE_TYPE_NIFI,
	ImageId=AMI_NIFI,
	UserData=user_data_nifi,

	NetworkInterfaces=[
		{
			'DeviceIndex': 0,
			'SubnetId': subnet_id,
			'AssociatePublicIpAddress': True,
			'Groups': [nifi_sg.id]
		},
	],
	KeyName=KEY_PEM,
	MinCount=1,
	MaxCount=1,
)
print(nifi_instance[0].id)

