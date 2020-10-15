'''
spark job to be run via spark_job.sh bash script

Author: William Wright
'''

from tempfile import NamedTemporaryFile
from csv import reader
from collections import Counter
import string

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

def process_files(iterator):
    '''docstring for process_files
    S3 client not thread-safe, initialize outside parallelized loop
    '''
    no_sign_request = botocore.client.Config(signature_version=botocore.UNSIGNED)
    s3client = boto3.client('s3', config=no_sign_request)
    for _key in iterator:
        _temp = NamedTemporaryFile(mode='w+b',dir='tmp/')
        try:
            download_wet_from_s3(_key,_temp,s3client)
            brands = ['facebook', 'apple', 'amazon', 'netflix', 'google']
            with open(_temp.name, 'rb') as stream:
                for record in ArchiveIterator(stream):
                    print(record.rec_type)
                    if record.rec_type == 'conversion':
                        text = record.content_stream().read()
                        text = text.decode('utf-8').lower().translate(str.maketrans('', '', string.punctuation))
                        brand_count = [sum([1 if brand in text else 0]) for brand in brands]
                        lines = text.split('\n')
                        words = [i.split(' ') for i in lines]
                        words = [item for sublist in words for item in sublist]
                        c = Counter(words)
                        brand_nums = [c[brand] for brand in brands]
                        # results [[wet file data], [brand simple count], [brand total count]]
                        results = [[record.rec_headers.get_header('WARC-Target-URI')
                                    , record.rec_headers.get_header('WARC-Date')
                                    , record.rec_headers.get_header('Content-Length')]
                                   , brand_count
                                   , brand_nums]
                        for i in range(len(brands)):
                            # cols = ['target_uri','timestamp','content_length','entity','entity_count','entity_total']
                            yield tuple(results[0]+[brands[i]]+[results[1][i]]+[results[2][i]])
        except ArchiveLoadFailed as e:
            print(e)
        finally:
            _temp.close()

def run():
    '''docstring for run'''
    n = 690
    conf = SparkConf() \
        .set("spark.default.parallelism", n) #\
        # .set("spark.driver.maxResultSize", "2g")
    sc = SparkContext(
        appName='spark-cc-analysis',
        conf=conf)
    sqlc = SQLContext(sparkContext=sc)

    filename = config.input_file
    pathlist = pathlist_from_csv(filename)[:n]

    rdd = sc.parallelize(pathlist)
    results = rdd.mapPartitions(process_files)
    cols = ['target_uri','timestamp','content_length','entity','entity_count','entity_total']
    df = sqlc.createDataFrame(results,cols)
    df.show()
    output = config.output
    df.write.mode('overwrite').parquet(output)

if __name__ == '__main__':
    run()