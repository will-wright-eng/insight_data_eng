'''
whats the same and whats different
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





def load_table(self, sc, spark, table_path, table_name):
    df = spark.read.load(table_path)
    df.createOrReplaceTempView(table_name)
    self.get_logger(sc).info(
        "Schema of table {}:\n{}".format(table_name, df.schema))

def execute_query(self, sc, spark, query):
    sqldf = spark.sql(query)
    self.get_logger(sc).info("Executing query: {}".format(query))
    sqldf.explain()
    return sqldf

def load_dataframe(self, sc, partitions=-1):
	'''
	partitions not necessary for smaller dataframes
		or can be specified in the select statement
	'''
    session = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
    if self.args.query is not None:
        self.load_table(sc, session, self.args.input, self.args.table)
        sqldf = self.execute_query(sc, session, self.args.query)
    else:
        sqldf = session.read.format("csv").option("header", True) \
            .option("inferSchema", True).load(self.args.csv)
    sqldf.persist()

    num_rows = sqldf.count()
    self.get_logger(sc).info(
        "Number of records/rows matched by query: {}".format(num_rows))

    if partitions > 0:
        self.get_logger(sc).info(
            "Repartitioning data to {} partitions".format(partitions))
        sqldf = sqldf.repartition(partitions)

    return sqldf

def fetch_process_warc_records(self, rows):
    no_sign_request = botocore.client.Config(
        signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)
    bucketname = "commoncrawl"
    no_parse = (not self.warc_parse_http_header)

    for row in rows:
        url = row[0]
        warc_path = row[1]
        offset = int(row[2])
        length = int(row[3])
        self.get_logger().debug("Fetching WARC record for {}".format(url))
        rangereq = 'bytes={}-{}'.format(offset, (offset+length-1))
        try:
            response = s3client.get_object(Bucket=bucketname,
                                           Key=warc_path,
                                           Range=rangereq)
        except botocore.client.ClientError as exception:
            self.get_logger().error(
                'Failed to download: {} ({}, offset: {}, length: {}) - {}'
                .format(url, warc_path, offset, length, exception))
            self.warc_input_failed.add(1)
            continue
        record_stream = BytesIO(response["Body"].read())
        try:
            for record in ArchiveIterator(record_stream,
                                          no_record_parse=no_parse):
                for res in self.process_record(record):
                    yield res
                self.records_processed.add(1)
        except ArchiveLoadFailed as exception:
            self.warc_input_failed.add(1)
            self.get_logger().error(
                'Invalid WARC record: {} ({}, offset: {}, length: {}) - {}'
                .format(url, warc_path, offset, length, exception))


def run_job(self, sc, sqlc):
	'''
	this code
		1. loads the crawl index df from personal s3
		2. partitions by 100
		3. maps those partitions to the fetch_process_warc_records
		4. loads output to s3
	'''
    sqldf = self.load_dataframe(sc, self.args.num_output_partitions)
    # option 1
    sqldf.write \
        .format(self.args.output_format) \
        .option("compression", self.args.output_compression) \
        .options(**self.get_output_options()) \
        .saveAsTable(self.args.output)
    # option 2
	output_path = "my_s3_output_path"
	sqlc.createDataFrame(output, schema=self.output_schema) \
	    .coalesce(100) \
	    .write \
	    .mode("overwrite") \
	    .parquet(output_path)


	sqldf = self.load_dataframe(sc, "my_s3_crawl_index_location")
	warc_recs = sqldf.select("url", "warc_filename", "warc_record_offset",
	                         "warc_record_length").repartition(100).rdd
	output = warc_recs.mapPartitions(self.fetch_process_warc_records)



if __name__ == "__main__":
	run_job()