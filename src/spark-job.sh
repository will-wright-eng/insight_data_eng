#!/bin/bash

spark-submit \
--py-files sparkcc.py word_count.py \
--input warc.paths.2020.1.100.gz \
--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--output {s3 bucket path}

spark-submit \
warc.paths.2020.1.100.gz \
test_word_count \
--py-files sparkcc.py word_count.py \
--verbose \
--driver-class-path  your jar[s]  \
--jars your jar[s] \


spark-submit \
--packages com.amazonaws:aws-java-sdk:1.7.4,org.apache.hadoop:hadoop-aws:2.7.7 \
--conf spark.executor.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--conf spark.driver.extraJavaOptions=-Dcom.amazonaws.services.s3.enableV4=true \
--master local s3_spark_test.py


spark-submit \
--packages org.apache.hadoop:hadoop-aws:3.2.0 \
--verbose \
get_data.py \
--input "CC-MAIN-2020-34"