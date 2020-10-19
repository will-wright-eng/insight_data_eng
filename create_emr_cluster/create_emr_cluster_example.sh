#!/bin/bash

pem_file="~/.ssh/<your pem file name>.pem"
key_name="<your pem file name>"
bootstrap_path="s3://<your/bootstrap/key/>"
subnet_id="<desired subnet (optional)>"
logs_path="s3://<desired log key (optional)>/"

aws s3 cp ./bootstrap.sh $bootstrap_path

ID=$(aws emr create-cluster \
--name <cluster name> \
--use-default-roles \
--release-label emr-5.30.1 \
--instance-count 4 \
--application Name=Spark Name=Hive Name=Ganglia Name=Zeppelin \
--ec2-attributes KeyName=$key_name,SubnetId=$subnet_id \
--instance-type m4.large \
--bootstrap-actions Path=${bootstrap_path}bootstrap.sh \
--query ClusterId \
--output text \
--log-uri ${logs_path})