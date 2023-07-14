# DS4A-LT16
Repository for learning team 16
# ETL Pipeline for Dataset Extraction, Transformation, and Loading into Redshift

This repository contains the Python scripts and files required to perform the extract and transform process for a dataset. 
The ETL pipeline is designed to extract data from various sources and perform necessary transformations to prepare for loading into a centralized Redshift data warehouse.

## Overview

The ETL pipeline consists of the following main components:

1. `storage2.py`: Python script responsible for extracting data from the data sources. This script fetches the dataset from different sources such as CSV files and APIs to store into the team's S3 buckets.

2. `transformation2.py`: Python script that performs the necessary transformations on the extracted data from S3 buckets. This includes cleaning the data, applying datatype logic, joining datasets, and preparing the data for loading into Redshift.

3. `transformation_final.py`: Python script that performs remaining transformations on files for datatypes to load into Redshift. 

## Prerequisites

To run the ETL pipeline, ensure that you have the following prerequisites set up:

1. Python 3.x installed on your system.

2. Required Python packages installed. You can install the necessary packages by running the following command in your terminal or command prompt:

      pip install boto3
   
      pip install python-dotenv
   
      pip install yfinance --upgrade --no-cache-dir
   
      pip install yahooquery
   
      pip install aiohttp
   
      pip install dask "dask[complete]"
   
      pip install geopy

## Usage

1. Update the configuration files and scripts with the appropriate credentials, file paths, and any other necessary configurations.

2. Run the stages of pipeline by executing the following command in your terminal or command prompt:
       python storage2.py
       python transformation2.py
       python transformation_final.py
   
   Sequentially execute steps for extraction and transformation of the datasets. 
4. Monitor the console output for any errors or log messages during the execution of the pipeline.
