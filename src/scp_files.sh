#!/bin/bash

emr_dns_path="hadoop@ec2-54-67-96-129.us-west-1.compute.amazonaws.com:/home/hadoop"

scp get_data.py  $emr_dns_path
scp warc.paths.2020.1.100.gz  $emr_dns_path
scp cc-get-data.sh  $emr_dns_path