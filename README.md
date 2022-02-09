# Automatic clustering and partitioning of Stitch data tables

Stitch is an ELT/ETL tool that can sync data from a 100+ sources to your data warehouse. See link: https://www.stitchdata.com/

If you are synching data to BigQuery, one of the first things you should do is to partition and cluster your tables. 
This helps keeping your cost down and improve performance. Tables synched by Stitch is by default not partitioned or clustered. 

This repository contains a script that recreates Stitch tables as partitioned and clustered. 

It partitions by _sdc_batched_at, which is a timestamp added by Stitch for when the data was loaded into your datawarehouse. It clusters by the primary id of each table.


I have run this script inside of my GCP account (on a vm running Jupyter lab) where the authentication was done by the service account of the machine. 
If you are running outside of GCP you need to add some authentication. 
For info about that and getting started with the Python BigQuery library read [here](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries)

## Settings for the script: 

**project_id:** Your GCP project id, where the datasets are. 
**stitch_datasets:** A list of your stitch dataset names in BigQuery
