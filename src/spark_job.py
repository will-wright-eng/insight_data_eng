'''
spark job to be run via spark_job.sh bash script

Author: William Wright
'''

from tempfile import NamedTemporaryFile
from csv import reader

import boto3
import botocore
from warcio.archiveiterator import ArchiveIterator

import pyspark
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql.types import StructType, StructField, StringType, LongType

import config

def pathlist_from_csv(filename):
    '''docstring for pathlist_from_csv'''
    with open(filename, 'r') as read_obj:
        csv_reader = reader(read_obj)
        list_of_rows = list(csv_reader)
    pathlist = [i[0] for i in list_of_rows]    
    return pathlist

def s3_key_filename_dict(paths):
    '''docstring for s3_key_filename_dict'''
    return {path:path.split('wet/')[-1] for path in paths}


def download_wet_from_s3(_key,_temp,s3client):
    '''docstring for download_wet_from_s3'''
    print('downloading...')
    bucket = 'commoncrawl'
    try:
        s3client.download_fileobj(bucket, _key, _temp)
    except botocore.client.ClientError as exception:
        print('Failed to download')
    _temp.seek(0)
    return print('download complete')

def extract_lines_from_wet(_temp):
    '''docstring for extract_lines_from_wet'''
    warc_dates = []
    text_stream = []
    #with open(wet_file, 'rb') as stream:
    with open(_temp.name, 'rb') as stream:
        for record in ArchiveIterator(stream):
            if record.rec_type == 'warcinfo':
                warc_dates.append(record.rec_headers.get_header('WARC-Date'))
            if record.rec_type == 'conversion':
                text = record.content_stream().read()
                text_stream.append(text.decode('utf-8'))
    return warc_dates, text_stream
    
def analyze_contents(contents):
    '''docstring for analyze_contents'''
    _dates = contents[0]
    text_stream = contents[1]
    min_date = min(_dates)
    max_date = max(_dates)
    date_dict = {'min_date':min_date,'max_date':max_date}
    # FAANG
    brands = ['facebook', 'apple', 'amazon', 'netflix', 'google']
    brand_count = [sum([1 if brand in i.lower() else 0 for i in text_stream]) for brand in brands]
    brand_dict = {i:j for i,j in zip(brands,brand_count)}
    return date_dict, brand_dict


def process_files(iterator):
    '''docstring for process_files
    S3 client (not thread-safe, initialize outside parallelized loop)'''
    no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)
    for _key in iterator:
        _temp = NamedTemporaryFile(mode='w+b',dir='tmp/')
        download_wet_from_s3(_key,_temp,s3client)
        contents = extract_lines_from_wet(_temp)
        _temp.close()
        results = analyze_contents(contents)
        _date = results[0]['min_date']
        for entity in list(results[1]):
            yield (_key,_date,entity,results[1][entity])

def run():
    '''docstring for run'''
    spark = SparkSession.builder.appName('spark-cc-analysis').getOrCreate()
    conf = SparkConf()
    conf.set("spark.default.parallelism", 100)
    sc = spark.sparkContext(conf=conf)

    filename = config.input_file
    pathlist = pathlist_from_csv(filename)

    rdd = sc.parallelize(pathlist)
    results = rdd.mapPartitions(process_files, preservesPartitioning=False).collect()

    columns = ['file_name','timestamp','entity','entity_count']
    df = spark.createDataFrame(results,columns)
    df.show()
    output = config.output
    df.write.mode('overwrite').parquet(output)

if __name__ == '__main__':
    run()