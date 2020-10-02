#https://medium.com/@bazill_theG/measuring-internet-links-accessing-the-common-crawl-dataset-using-emr-and-pyspark-in-aws-fcf5eb26afd9

from pyspark.sql import functions as F

bucket_in = 's3://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2020-16/'
df = spark.read.parquet(bucket_in)
bucket_out = 's3a://will-cc-bucket/cc-index/cc_index_CC-MAIN-2020-16.parquet'
df.write.parquet(bucket_out,mode="overwrite")