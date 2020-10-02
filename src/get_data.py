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

def reduce_by_key_func(a, b):
    return a + b

def process_warcs(id_, iterator):
    s3pattern = re.compile('^s3://([^/]+)/(.+)')
    base_dir = os.path.abspath(os.path.dirname(__file__))

    # S3 client (not thread-safe, initialize outside parallelized loop)
    no_sign_request = botocore.client.Config(
        signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)

    for uri in iterator:
        #warc_input_processed.add(1)
        if uri.startswith('s3://'):
            get_logger().info('Reading from S3 {}'.format(uri))
            s3match = s3pattern.match(uri)
            # if s3match is None:
            #     get_logger().error("Invalid S3 URI: " + uri)
            #     continue
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b')#,dir=local_temp_dir)
            try:
                s3client.download_fileobj(bucketname, path, warctemp)
            except botocore.client.ClientError as exception:
                # get_logger().error(
                #     'Failed to download {}: {}'.format(uri, exception))
                # warc_input_failed.add(1)
                # warctemp.close()
                continue
            warctemp.seek(0)
            stream = warctemp

        elif uri.startswith('hdfs://'):
            # get_logger().error("HDFS input not implemented: " + uri)
            continue
        else:
            # get_logger().info('Reading local stream {}'.format(uri))
            if uri.startswith('file:'):
                uri = uri[5:]
            uri = os.path.join(base_dir, uri)
            try:
                stream = open(uri, 'rb')
            except IOError as exception:
                # get_logger().error(
                #     'Failed to open {}: {}'.format(uri, exception))
                # warc_input_failed.add(1)
                continue

        # no_parse = (not warc_parse_http_header)
        no_parse = (True)
        try:
            archive_iterator = ArchiveIterator(stream,
                                               no_record_parse=no_parse)
            for res in iterate_records(uri, archive_iterator):
                yield res
        except ArchiveLoadFailed as exception:
            continue
            # warc_input_failed.add(1)
            # get_logger().error(
            #     'Invalid WARC: {} - {}'.format(uri, exception))
        finally:
            stream.close()

def process_record(record):
    raise NotImplementedError('Processing record needs to be customized')

def iterate_records(_warc_uri, archive_iterator):
    """Iterate over all WARC records. This method can be customized
       and allows to access also values from ArchiveIterator, namely
       WARC record offset and length."""
    for record in archive_iterator:
        for res in process_record(record):
            yield res
        records_processed.add(1)
        # WARC record offset and length should be read after the record
        # has been processed, otherwise the record content is consumed
        # while offset and length are determined:
        #  warc_record_offset = archive_iterator.get_record_offset()
        #  warc_record_length = archive_iterator.get_record_length()

def is_wet_text_record(record):
    """Return true if WARC record is a WET text/plain record"""
    return (record.rec_type == 'conversion' and
            record.content_type == 'text/plain')

def is_wat_json_record(record):
    """Return true if WARC record is a WAT record"""
    return (record.rec_type == 'metadata' and
            record.content_type == 'application/json')

def is_html(record):
    """Return true if (detected) MIME type of a record is HTML"""
    html_types = ['text/html', 'application/xhtml+xml']
    if (('WARC-Identified-Payload-Type' in record.rec_headers) and
        (record.rec_headers['WARC-Identified-Payload-Type'] in
         html_types)):
        return True
    for html_type in html_types:
        if html_type in record.content_type:
            return True
    return False

def run():
    name = name
    description = name
    conf = SparkConf()

    sc = SparkContext(
        appName=name,
        conf=conf)
    #sqlc = SQLContext(sparkContext=sc)

    bucket = 's3a://will-cc-bucket/cc-index/cc_index_CC-MAIN-2020-16.parquet'
    sqldf = load_dataframe(sc, bucket)

    # input_data = sc.textFile(input,
    #                          minPartitions=num_input_partitions)

    output = input_data.mapPartitionsWithIndex(process_warcs) \
        .reduceByKey(reduce_by_key_func)

    sqlc.createDataFrame(output, schema=self.output_schema) \
        .coalesce(100) \
        .write \
        .mode("overwrite") \
        .parquet(output_path)

    sc.stop()


if __name__ == '__main__':
    run()
