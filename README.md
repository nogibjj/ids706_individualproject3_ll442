# ids706_individualProject3_ll442

In this project, Databricks ETL (Extract Transform Load) Pipeline is implemented, validated and visualized.

## Databricks Notebook
The step-by-step explanation of the databricks notebook is covered in this module

### Step1: Import the required library
In this project, `Pyspark` and `Matplotlib` are used as the fundamental dependecies for ETL pipeline and data analysis visualization. 
![library](./image/library.png)

### Step 2: ETL Pipeline
1. Extract Operation:
After initializing a SparkSession, a dataset of Occupational Employment and Wage Statistics is loaded and converted to a table `kk_table`
![extract](./image/extract.png)

2. Transform Operation:
The dataframe of loaded `kk_table` has been transformed through **deleting illegal characters in data cells** with `regexp_replace()` and **renaming column header** with `withColumnRenamed()`.
![transform1](./image/transform1.png)
![transform2](./image/transform2.png)
The data table transformed is shown as below:
![transform3](./image/transform3.png)

4. Load Operation
Transformed datatable has been loaded to **Delta Lake** for further convenient operation.
![load1](./image/load1.png)
![load2](./image/load2.png)

### Step3: Data Validation
The transformed table stored in Delta Lake has been loaded and its value validity has been checked.
![validation](./image/validation.png)

### Step4: Data Visualization and Conclusion
1. Data Visualization
The mean, min, max value of the wages of different industries and experience level has been calculated and visualized to the graph shown below.
![visualization1](./image/visualization1.png)
![visualization2](./image/visualization2.png)
![visualization3](./image/visualization3.png)
2. Reccomendation and Conclusion
As can be seen from the visualization graph, overally, it can be concluded that more experienced employee can be paid more in North Carolina in different fields, with the highest wage in their 90th. Specifically, there is a significant salary difference between 75th and 90th. Additionally, for employees before median experience level, they are paid similarly because they do not have significant experience difference.
As a result, after choosing the primary career path, it is recommended to **persist** to it as wage will increase gradually with experience in the majority of the fields. For entry-level employee who found minimal increase in their wage, it suggested to **keep on the path for longer time** since there will be an increase in salary after early career stage.  

## Spark SQL
1. Dataset extraction
  A spark session is initialized at the start for dataset extraction and table loading.
  `spark = SparkSession.builder.appName("CSVIngestion").getOrCreate()`
  `df.createOrReplaceTempView("kk_table")`
2. SQL implementation
  Spark library can be used for convenient SQL implementation in python file.
  `result = spark.sql("SELECT * FROM kk_table WHERE Occupation = 'Legal'")`
3. Transformation
   Based on the Spark Data frame, different transformation including filtering, renaming and replacing over the original dataset can be carried out efficiently.

## Delta Lake
Dataset transformed `ncemployment` are stored in Delta Lake and can be found in hive_metastore in the cluster where the notebook runs.`dbfs:/user/hive/warehouse/ncemployment`

## Project Structure



