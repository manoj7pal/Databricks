# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ingest Contructors.json file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# DDL Type Schema
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

constructor_df = spark.read.json(path = "/mnt/formula1projectstorage/raw/constructors.json", schema = constructor_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Drop unwanted columns using drop()

# COMMAND ----------

#from pyspark.sql.functions import col
# constructor_dropped_df = constructor_df.drop(col(['url']))

# constructor_dropped_df = constructor_df.drop(constructor_df['url'])
# constructor_dropped_df = constructor_df.drop(constructor_df.url)

constructor_dropped_df = constructor_df.drop('url')

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Rename a column and adding new columns in the same step

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

constructor_final_df = constructor_dropped_df.withColumnRenamed('constructorId', 'constructor_id')\
    .withColumnRenamed('constructorRef', 'constructor_ref')\
    .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step4 - Store the data to ADLS as parquet file

# COMMAND ----------

path = "/mnt/formula1projectstorage/processed/constructors/"
constructor_final_df.write.parquet(path, mode='overwrite')

# COMMAND ----------

# Notes:
# display(constructor_df.printSchema())    
# display(constructor_final_df)
# display(dbutils.fs.ls(path))