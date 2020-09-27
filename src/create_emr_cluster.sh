#!/bin/bash

pem_file="~/.ssh/WilliamWrightIAM-keypair.pem"
bootstrap_path="s3://will-cc-bucket/bootstrap_script/"
subnet_id="subnet-029348fc5d33bcf5a"
logs_path="s3://aws-logs-190846325093-us-west-1/elasticmapreduce/"

aws s3 cp ./bootstrap.sh $bootstrap_path

ID=$(aws emr create-cluster \
--name spark-data-processing \
--use-default-roles \
--release-label emr-5.30.1 \
--instance-count 2 \
--application Name=Spark Name=Hive Name=Ganglia Name=Zeppelin \
--ec2-attributes KeyName=WilliamWrightIAM-keypair,SubnetId=$subnet_id \
--instance-type m4.large \
--bootstrap-actions Path=${bootstrap_path}bootstrap.sh \
--query ClusterId \
--output text \
--log-uri ${logs_path})