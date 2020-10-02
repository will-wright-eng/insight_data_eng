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

### Slide deck
- [Slides link](https://docs.google.com/presentation/d/1uK3b4Ao3yxKsF9-GCcx_akWJcEAtI2VjG-0nB8l8U2g/edit#slide=id.p)

### Process flow
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_project_proposal.png)

### Tech Stack
![tech_stack](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_data_eng_tech_stack.png)  

### User Interface
![user_interface](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/blarg.png)  

## Project links
- [Trello](https://trello.com/c/8pUyiHno/129-insight-data-eng-project)  
- [LucidChart](https://app.lucidchart.com/documents/edit/3b5ef670-2697-4919-8d18-66a808348285/0_0#?folder_id=home&browser=icon) 
- CommonCrawl

## Setup
- run `create_emr_cluster.sh` locally
- adjust `~/.ssh/config` file such that `emr_cluster` reflects the new _Master public DNS_
- scp four files to cluster with `spc_files.sh` (`sparkcc.py`  `warc.paths.2020.1.100.gz` `word_count.py` `spark-job.sh`)
- run `spark-job.sh` on cluster
