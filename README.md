# ids706_individualProject3_ll442

In this project, Databricks ETL (Extract Transform Load) Pipeline is implemented, validated and visualized.

## Databricks Notebook
The step-by-step explanation of the databricks notebook is covered in this module

### Step1: Import the required library
In this project, `Pyspark` and `Matplotlib` are used as the fundamental dependecies for ETL pipeline and data analysis visualization. 
![step2](./library.png)

### Step 2: ETL Pipeline
1. Extract Operation:
After initializing a SparkSession, a dataset of Occupational Employment and Wage Statistics is loaded and converted to a table `kk_table`
![step2](./extract.png)

2. Transform Operation:
The dataframe of loaded `kk_table` has been transformed through **deleting illegal characters in data cells** with `regexp_replace()` and **renaming column header** with `withColumnRenamed()`.
![step2](./extract.png)
The data table transformed is shown as below:
![step2](./extract.png)

3. Load Operation
Transformed datatable has been loaded to **Delta Lake** for further convenient operation.
![step2](./extract.png)

### Step3: Data Validation
The transformed table stored in Delta Lake has been loaded and its value validity has been checked.
![step2](./extract.png)

### Step4: Data Visualization and Conclusion
1. Data Visualization
The mean, min, max value of the wages of different industries and experience level has been calculated and visualized to the graph shown below.
![stpe3](./step3.png)

## Recommendation and Conclusion based on visualization

## Spark SQL

## Delta Lake

## Project Structurem


