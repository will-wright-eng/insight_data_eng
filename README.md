** project in process **

# Insight Data Engineering Fellowship

## Table of Contents
1. Project Summary
2. Project Links
3. Setup

## Project Summary
### Instroduction
- Shifting the consumer's eye via digital advertising has become a 400 billion dollar industry. Brands and consultancies use rapid monitoring tools in order to quickly respond to the public's response to advertising along with various other market events. The aim of my project was to take a zoomed out view on brand visability in order to understand alternative methods of measuring marketing spend.

The CommonCrawl corpus is composed of a monthly crawl of the internet, aggregating teabytes of web data. By analyzing each page through a batch process I determined entity visability of S&P 500 companies accross all websites found within the corpus, then projected marketing events onto each companies timeseries.

### Process flow
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_project_proposal.png)

### Tech Stack
![tech_stack](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_data_eng_tech_stack.png)  

### User Interface
![user_interface](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/blarg.png)  

## Project links
- [Common Crawl](https://registry.opendata.aws/commoncrawl/)
- [Slides link](https://docs.google.com/presentation/d/1Snfb07JO33BxOD7dne0vgiSb7Koa0BrrAwoh-_eo1_U/edit?usp=sharing)

## Setup
My process for setting up this project:
- run `create_emr_cluster.sh` locally
- adjust `~/.ssh/config` file such that `emr_cluster` reflects the new __Master public DNS__ (automate this step with env vars --> [SO post](https://stackoverflow.com/questions/64200760/capture-master-public-dns-when-creating-cluster-via-aws-cli))
- scp files to cluster with `spc_files.sh`
- run `spark-job.sh` on master node

