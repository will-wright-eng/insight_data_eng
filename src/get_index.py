'''
use this script to grab the cc 
'''

import argparse
import logging
import os
import re

from io import BytesIO
from tempfile import TemporaryFile

import boto3
import botocore

from warcio.archiveiterator import ArchiveIterator
from warcio.recordloader import ArchiveLoadFailed

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

name = 'CCSparkJob'

output_schema = StructType([
    StructField("key", StringType(), True),
    StructField("val", LongType(), True)
])

def run():
    name = name
    description = name
    conf = SparkConf()

    sc = SparkContext(
        appName=name,
        conf=conf)
    sqlc = SQLContext(sparkContext=sc)

    # input_data = sc.textFile(input,
    #                          minPartitions=num_input_partitions)

    # output = input_data.mapPartitionsWithIndex(process_warcs) \
    #     .reduceByKey(reduce_by_key_func)

    # sqlc.createDataFrame(output, schema=output_schema) \
    #     .coalesce(num_output_partitions) \
    #     .write \
    #     .format(output_format) \
    #     .option("compression", output_compression) \
    #     .options(**get_output_options()) \
    #     .saveAsTable(output)

    sc.stop()

if __name__ == '__main__':
    run()