# __BrandView__ 
### Brand Visability within a Web Crawl Corpus

## Table of Contents
1. [Summary](README.md#summary)
2. [Setup](README.md#setup)
3. [Links](README.md#links)
4. [Data](README.md#data)
5. [Future Work](README.md#future-work)

## Summary
### Introduction
Shifting the consumer's eye via digital advertising has become a 400 billion dollar industry. Brands and consultancies use rapid monitoring tools in order to quickly respond to the public's response to advertising along with various other market events. The aim of my project was to take a zoomed-out view on brand visability by unlocking potential within alternative data sources.

The Common Crawl corpus is composed of a monthly wide crawl of the internet, aggregating teabytes of web data. By analyzing a random sampling of pages through a batch process I determined entity visability of companies accross websites found within the corpus.

### Process flow
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/new_process_flow.png)

## Setup
_examples present where credentials or addresses needed_
1. run `preprocessing.py` to randomly sample `wet.path.gz` files and generate project files
2. run `create_emr_cluster.sh`
3. adjust `~/.ssh/config` and `spc_files.sh` such that they reflect your cluster's __Master public DNS__ 
4. scp files to cluster with `spc_files.sh` (or change `config.py` file to pull files from S3)
5. ssh into master node
6. `chmod +x spark_job.sh`
7. `./spark_job.sh`

## Links
- [Slide deck](https://docs.google.com/presentation/d/1Snfb07JO33BxOD7dne0vgiSb7Koa0BrrAwoh-_eo1_U/edit?usp=sharing)
- [Tableau dashboard](https://public.tableau.com/profile/will.wright6939#!/vizhome/brand_visability_within_web_corpus/BrandVisabilitywithinWebCorpus?publish=yes)
- [Common Crawl](https://registry.opendata.aws/commoncrawl/)

## Data
- __date range:__ April 2014 - Sept 2020
- __source files:__ 69 path files (historical data isn't exactly by month)
- __analyzed:__ 102.79 GBs of data by avg estimation (sampled 1.5 GBs of data from each path file)
- __results:__ 167,335,570 rows (2.7 GB parquet / 21.4 GB csv)

### ERD
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/results_erd.png)

### User Interface
![user_interface](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/brand_visability_within_web_corpus.png)  

## Future Work
### Need
- ~add elements to spark process to increase depth of anaysis~
- segment out records by target url and it's persistence between months
- create OOP-class structure that abstracts WET file _access and download_ from the _analysis_ of the text

### Want
- AWS CLI call for master DNS and cluster ID
- add runtime log file to spark app w/ project, process time, input_file, output_file, and file_count

### Nice to have
- AWS CLI script for creating VPC and subnets
- add badge/shield from [shield.io](https://shields.io/category/platform-support)
