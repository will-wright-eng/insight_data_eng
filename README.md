# Brand Visability within Web Corpus

## Table of Contents
1. [Summary](README.md#summary)
2. [Links](README.md#links)
3. [Setup](README.md#setup)
4. [Data](README.md#data)
5. [Future Work](README.md#future-work)

## Summary
### Instroduction
Shifting the consumer's eye via digital advertising has become a 400 billion dollar industry. Brands and consultancies use rapid monitoring tools in order to quickly respond to the public's response to advertising along with various other market events. The aim of my project was to take a zoomed-out view on brand visability by unlocking potential within alternative data sources.

The Common Crawl corpus is composed of a wide monthly crawl of the internet, aggregating teabytes of web data. By analyzing each page through a batch process I determined entity visability of companies accross websites found within the corpus.

### Process flow
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_project_processflow.png)

### Tech Stack
![tech_stack](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_dataeng_techstack.png)  

### User Interface
![user_interface](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/brand_visability_within_web_corpus.png)  

## Links
- [Slide deck](https://docs.google.com/presentation/d/1Snfb07JO33BxOD7dne0vgiSb7Koa0BrrAwoh-_eo1_U/edit?usp=sharing)
- [Tableau dashboard](https://public.tableau.com/profile/will.wright6939#!/vizhome/brand_visability_within_web_corpus/BrandVisabilitywithinWebCorpus?publish=yes)
- [Common Crawl](https://registry.opendata.aws/commoncrawl/)

## Setup
_examples present where credentials or addresses needed_
- randomly sample `wet.path.gz` files and generate `.csv`
- run `create_emr_cluster.sh`
- adjust `~/.ssh/config` and `spc_files.sh` such that they reflect the new __Master public DNS__ 
- scp files to cluster with `spc_files.sh`
- ssh into master node
- `chmod +x spark_job.sh`
- `./spark_job.sh`

## Data
- date range: April 2014 - Sept 2020
- 69 path files (historical data isn't exactly by month)
- `2020-10-12_wet_paths_10series.csv` contains paths for 102.79 GBs of data by avg estimation (sampled 1.5 GBs of data from each path file)
- results produced table with 167,335,570 rows (2.7 GB parquet / 21.4 GB csv)

## Future Work
### Need
- add elements to spark process for increased depth of anaysis in dashboard
- segment out records by target url and it's persistence between months

### Want
- bash script of environmental vars w/ AWS CLI call for master DNS
- add log file to spark app w/ process time | input | output etc.
- create framework for non-overlaping repeat analysis (add record of analyzed files to S3)
- add badge/shield from [shield.io](https://shields.io/category/platform-support)

### Nice to have
- AWS CLI script for creating VPC and subnets