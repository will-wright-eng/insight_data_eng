# Brand Visability within Web Corpus

## Table of Contents
1. Project Summary
2. Project Links
3. Setup

## Project Summary
### Instroduction
Shifting the consumer's eye via digital advertising has become a 400 billion dollar industry. Brands and consultancies use rapid monitoring tools in order to quickly respond to the public's response to advertising along with various other market events. The aim of my project was to take a zoomed out view on brand visability in order to understand alternative methods of measuring marketing spend.

The Common Crawl corpus is composed of a monthly crawl of the internet, aggregating teabytes of web data. By analyzing each page through a batch process I determined entity visability of companies accross websites found within the corpus.

### Process flow
![process_flow](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_project_proposal.png)

### Tech Stack
![tech_stack](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/insight_data_eng_tech_stack.png)  

### User Interface
![user_interface](https://github.com/william-cass-wright/insight_data_eng/blob/master/images/user_interface.png)  

## Project links
- [Slide deck](https://docs.google.com/presentation/d/1Snfb07JO33BxOD7dne0vgiSb7Koa0BrrAwoh-_eo1_U/edit?usp=sharing)
- [Tableau dashboard](https://public.tableau.com/profile/will.wright6939#!/vizhome/brand_visability_within_web_corpus/BrandVisabilitywithinWebCorpus?publish=yes)
- [Common Crawl](https://registry.opendata.aws/commoncrawl/)

## Setup
- randomly sample `wet.path.gz` files and generate `.csv`
- run `create_emr_cluster.sh`
- adjust `~/.ssh/config` and `spc_files.sh` such that they reflect the new __Master public DNS__ 
- scp files to cluster with `spc_files.sh`
- ssh into master node
- `chmod +x spark_job.sh`
- `./spark_job.sh`
