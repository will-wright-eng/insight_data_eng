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

num_input_partitions = 400
num_output_partitions = 10


def get_output_options(self):
    return {x[0]: x[1] for x in map(lambda x: x.split('=', 1),
                                    self.args.output_option)}

def run(self):
    description = self.name
    conf = SparkConf()

    sc = SparkContext(
        appName=self.name,
        conf=conf)
    sqlc = SQLContext(sparkContext=sc)

    #self.init_accumulators(sc)

    #self.run_job(sc, sqlc)
    input_data = sc.textFile(self.args.input,
                             minPartitions=self.args.num_input_partitions)

    output = input_data.mapPartitionsWithIndex(self.process_warcs) \
        .reduceByKey(self.reduce_by_key_func)

    sqlc.createDataFrame(output, schema=self.output_schema) \
        .coalesce(self.args.num_output_partitions) \
        .write \
        .format(self.args.output_format) \
        .option("compression", self.args.output_compression) \
        .options(**self.get_output_options()) \
        .saveAsTable(self.args.output)

    #self.log_aggregators(sc)

    if self.args.spark_profiler:
        sc.show_profiles()

    sc.stop()

@staticmethod
def reduce_by_key_func(a, b):
    return a + b

def process_warcs(self, id_, iterator):
    s3pattern = re.compile('^s3://([^/]+)/(.+)')
    base_dir = os.path.abspath(os.path.dirname(__file__))

    # S3 client (not thread-safe, initialize outside parallelized loop)
    no_sign_request = botocore.client.Config(
        signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)

    for uri in iterator:
        self.warc_input_processed.add(1)
        if uri.startswith('s3://'):
            self.get_logger().info('Reading from S3 {}'.format(uri))
            s3match = s3pattern.match(uri)
            if s3match is None:
                self.get_logger().error("Invalid S3 URI: " + uri)
                continue
            bucketname = s3match.group(1)
            path = s3match.group(2)
            warctemp = TemporaryFile(mode='w+b',
                                     dir=self.args.local_temp_dir)
            try:
                s3client.download_fileobj(bucketname, path, warctemp)
            except botocore.client.ClientError as exception:
                self.get_logger().error(
                    'Failed to download {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                warctemp.close()
                continue
            warctemp.seek(0)
            stream = warctemp
        elif uri.startswith('hdfs://'):
            self.get_logger().error("HDFS input not implemented: " + uri)
            continue
        else:
            self.get_logger().info('Reading local stream {}'.format(uri))
            if uri.startswith('file:'):
                uri = uri[5:]
            uri = os.path.join(base_dir, uri)
            try:
                stream = open(uri, 'rb')
            except IOError as exception:
                self.get_logger().error(
                    'Failed to open {}: {}'.format(uri, exception))
                self.warc_input_failed.add(1)
                continue

        no_parse = (not self.warc_parse_http_header)
        try:
            archive_iterator = ArchiveIterator(stream,
                                               no_record_parse=no_parse)
            for res in self.iterate_records(uri, archive_iterator):
                yield res
        except ArchiveLoadFailed as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error(
                'Invalid WARC: {} - {}'.format(uri, exception))
        finally:
            stream.close()

def process_record(self, record):
    raise NotImplementedError('Processing record needs to be customized')

def iterate_records(self, _warc_uri, archive_iterator):
    """Iterate over all WARC records. This method can be customized
       and allows to access also values from ArchiveIterator, namely
       WARC record offset and length."""
    for record in archive_iterator:
        for res in self.process_record(record):
            yield res
        self.records_processed.add(1)
        # WARC record offset and length should be read after the record
        # has been processed, otherwise the record content is consumed
        # while offset and length are determined:
        #  warc_record_offset = archive_iterator.get_record_offset()
        #  warc_record_length = archive_iterator.get_record_length()

@staticmethod
def is_wet_text_record(record):
    """Return true if WARC record is a WET text/plain record"""
    return (record.rec_type == 'conversion' and
            record.content_type == 'text/plain')

@staticmethod
def is_wat_json_record(record):
    """Return true if WARC record is a WAT record"""
    return (record.rec_type == 'metadata' and
            record.content_type == 'application/json')

@staticmethod
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


