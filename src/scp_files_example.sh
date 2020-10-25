#!/bin/bash

emr_dns_path="hadoop@<emr cluster master DNS>:/home/hadoop"

scp spark_job.sh  $emr_dns_path
scp spark_job.py  $emr_dns_path
scp config.py  $emr_dns_path
scp wet_paths_10series.csv $emr_dns_path