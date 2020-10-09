#!/bin/bash

start=`date +%s`
mkdir tmp

spark-submit \
--packages org.apache.hadoop:hadoop-aws:3.2.0 \
--verbose \
spark_job.py

end=`date +%s`
runtime=$((end-start))
echo process runtime in seconds
echo $runtime