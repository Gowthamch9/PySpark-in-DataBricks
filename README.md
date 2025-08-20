# ⚡ PySpark in Databricks  

This repository contains practice work with **PySpark** inside **Databricks**, focusing on data manipulation, transformations, and analytics at scale.  

The goal is to demonstrate how big data can be processed efficiently using Spark's distributed computing capabilities.  

---

## 📂 Repository Structure  

- **`PySpark_Fundamentals`** → Practice file containing PySpark code for data exploration, cleaning, and transformations in Databricks.  

---

## 🛠️ Skills & Concepts Demonstrated  

- **PySpark Basics**  
  - Creating DataFrames from CSV/JSON/Parquet files  
  - Inspecting schema and data types  
  - Selecting, filtering, and transforming data  

- **Data Transformation**  
  - GroupBy and aggregations  
  - Window functions (ROW_NUMBER, RANK)  
  - Joins between large datasets  

- **Data Cleaning**  
  - Handling nulls and missing values  
  - Casting data types  
  - Using `withColumn` and `when/otherwise`  

- **Performance Optimization**  
  - Partitioning & caching  
  - Writing results back to Parquet/Delta format  

---

## ⚡ Example Workflow  

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Start Spark Session
spark = SparkSession.builder.appName("PySparkPractice").getOrCreate()

# Load data
df = spark.read.csv("/databricks-datasets/retail-data/by-day/*.csv", header=True, inferSchema=True)

# Inspect schema
df.printSchema()

from pyspark.sql.functions import *
data = [(1, 'Gowtham', 'male', 3000,'IT'), (2, 'abcd', None, 4000, 'HR'), (3, 'efgh', 'male', 4000, None), (4, 'Sanjana', 'female', 6000, 'IT'), (5, 'Grishma', 'female', 6000, 'IT'), (6, None, 'male', 3000, 'HR')]
schema = ['id', 'name', 'gender', 'salary', 'dep']
df = spark.createDataFrame(data, schema)
df.show()
df.fillna('unkown').show()
df.na.fill('unkown', ['dep']).show()
