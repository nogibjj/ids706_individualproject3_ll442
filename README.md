# ids706_project11_ll442

In this project, a data pipeline is created using Databricks, with one data source and data sink included. 
Specifically, first, I connect this github repo to Azure Databrick. Then I write three files in my repo for Ingest, Prepare, and Analyze on Azure databricks. 

## Step 1: Ingest data
In this step, dataset OccupationalEmploymentandWageStatistics_final.csv uploaded to Databrick file system is ingested to the cluster of workspace with Spark
![step1-1](./step1_1.png)
![stpe1-2](./step1_2.png)

## Step 2: Prepare data
In this step, the dataset is converted to table with df.createOrReplaceTempView("kk_table")
![step2](./step2.png)

## Step 3: Analyze data
Find the mean, max, min value
![stpe3](./step3.png)
