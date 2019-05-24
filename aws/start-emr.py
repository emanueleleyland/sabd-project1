import time

import boto3
from config import KEY_PEM, HOSTEDZONEID

NUMWORKERS = 4

# AWS cloud deployment entry point. This script create a VPC that hosts all the necessary machines: Spark cluster
# virtual machines and the NiFi server
# It is necessary to install boto3 package via pip and set up the authentication credential via awscli (aws configure command)

emr_client = boto3.client('emr')
ec2 = boto3.resource('ec2')

# Create VPC

vpc = ec2.create_vpc(CidrBlock='10.0.0.0/16')
ec2_client = boto3.client('ec2')

ec2_client.modify_vpc_attribute(

	EnableDnsHostnames={
		'Value': True
	},
	# EnableDnsSupport={
	#	'Value': True
	# },

	VpcId=vpc.id
)

vpc.create_tags(Tags=[{"Key": "Name", "Value": "vpc"}])
vpc.wait_until_available()

print(vpc.id)

# create subnet for NiFi instance and EMR cluster. All this instances run on the same subnet.

subnet = ec2.create_subnet(CidrBlock='10.0.0.0/16', VpcId=vpc.id,
						   AvailabilityZone='eu-central-1a')
subnet.create_tags(Tags=[{'Key': 'Name', 'Value': 'subnet'}])

# Create internet gateway and route table for the subnet

ig = ec2.create_internet_gateway()
vpc.attach_internet_gateway(InternetGatewayId=ig.id)
print(ig.id)

route_table = vpc.create_route_table()
route = route_table.create_route(
	DestinationCidrBlock='0.0.0.0/0',
	GatewayId=ig.id
)

route_table.associate_with_subnet(SubnetId=subnet.id)

master_sg = ec2.create_security_group(
	GroupName='master', Description='master sec group', VpcId=vpc.id)
master_sg.authorize_ingress(
	IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
print(master_sg.id)

slave_sg = ec2.create_security_group(
	GroupName='slave', Description='slave sec group', VpcId=vpc.id)
slave_sg.authorize_ingress(
	IpPermissions=[{'IpProtocol': 'icmp', 'FromPort': -1, 'ToPort': -1, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 8080, 'ToPort': 8080, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
				   {'IpProtocol': 'tcp', 'FromPort': 0, 'ToPort': 65535, 'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}])
print(slave_sg.id)

# Create EMR cluster with the following application installed:
# - Hadoop
# - HBase
# - Spark
# The cluster is composed of and unique master node and two worker nodes

cluster = emr_client.run_job_flow(
	Name='SABD-Cluster',
	LogUri='s3://sabd-emr-log/elasticmapreduce/',
	ReleaseLabel="emr-5.23.0",
	Instances={
		'EmrManagedMasterSecurityGroup': master_sg.id,
		'EmrManagedSlaveSecurityGroup': slave_sg.id,
		'Ec2KeyName': KEY_PEM,
		'InstanceFleets': [
			{
				'Name': 'master',
				'InstanceFleetType': 'MASTER',
				'TargetOnDemandCapacity': 1,
				'InstanceTypeConfigs': [
					{
						'InstanceType': 'm4.xlarge',
						'WeightedCapacity': 1
					}
				]
			},
			{
				'Name': 'slave',
				'InstanceFleetType': 'CORE',
				'TargetOnDemandCapacity': NUMWORKERS,
				'InstanceTypeConfigs': [
					{
						'InstanceType': 'm4.xlarge',
						'WeightedCapacity': 1
					}
				]
			}
		],
		'KeepJobFlowAliveWhenNoSteps': True,
		'Ec2SubnetIds': [
			subnet.id
		],

	},
	Applications=[
		{
			'Name': 'Spark',
		},
		{
			'Name': 'Hadoop',
		},
		{
			'Name': 'HBase',
		}
	],
	Configurations=[
		{
			'Classification': 'hdfs-site',
			'Properties': {
				'dfs.permissions.enabled': 'false'
			}
		},

	],
	VisibleToAllUsers=True,
	JobFlowRole='EMR_EC2_DefaultRole',
	ServiceRole='EMR_DefaultRole',

)
job_flow_id = cluster['JobFlowId']
print(job_flow_id)
found = False

# Wait until EMR cluster is ready to run steps (WAITING state)
while True:
	if found:
		break
	cluster_list = emr_client.list_clusters(ClusterStates=['WAITING'])
	if len(cluster_list['Clusters']) > 0:
		for cluster in list(cluster_list['Clusters']):
			if cluster['Status']['State'] == 'WAITING':
				cluster_id = cluster['Id']
				found = True
				break
	time.sleep(5)


# get master dns address automatically assigned by AWS
master_dns = emr_client.describe_cluster(ClusterId=cluster_id)['Cluster']['MasterPublicDnsName']
print(master_dns)

# Register a DNS entry with route53 service

route53 = boto3.client('route53')
master_dns_record = route53.change_resource_record_sets(
	HostedZoneId=HOSTEDZONEID,
	ChangeBatch={
		'Changes': [
			{
				'Action': 'UPSERT',
				'ResourceRecordSet': {
					'Name': 'master.cini-project.cloud',
					'Type': 'CNAME',
					'TTL': 60,
					'ResourceRecords': [
						{
							'Value': master_dns
						},
					],

				},
			},
		]
	},
)

print(master_dns_record)
