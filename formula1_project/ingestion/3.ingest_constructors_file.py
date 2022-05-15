# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### Ingest Contructors.json file

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# Adding widgets to pass runtime notebook parameters
dbutils.widgets.text("p_datasource", "constructors")
v_datasource = dbutils.widgets.get("p_datasource")

dbutils.widgets.text("p_env", "Development")
v_env = dbutils.widgets.get("p_env")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 - Read the JSON file using the spark dataframe reader

# COMMAND ----------

# DDL Type Schema
constructor_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"

path = f"{raw_dir_path}/constructors.json"
constructor_df = spark.read.json(path = path, schema = constructor_schema)

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

from pyspark.sql.functions import current_timestamp, lit

constructor_final_df = add_ingestion_date(constructor_dropped_df)\
                        .withColumnRenamed('constructorId', 'constructor_id')\
                        .withColumnRenamed('constructorRef', 'constructor_ref')\
                        .withColumn("datasource", lit(v_datasource))\
                        .withColumn("environment", lit(v_env))    

#     .withColumn('ingestion_date', current_timestamp())

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step4 - Store the data to ADLS as parquet file

# COMMAND ----------

path = f"{processed_dir_path}/constructors/"
constructor_final_df.write.parquet(path, mode='overwrite')

# COMMAND ----------

# Notes:
# display(constructor_df.printSchema())    
# display(constructor_final_df)
# display(dbutils.fs.ls(path))

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.exit(f"{notebook_name}: Success")