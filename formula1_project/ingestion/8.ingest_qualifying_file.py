# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest qualifying.json files
# MAGIC 
# MAGIC ```
# MAGIC  - Multiple multi-line .json files in a folder
# MAGIC  - multiLine = True, wjhile loading the CSV files. 
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step1 - Read the JSON files in the qualifying folder, using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

qualify_schema = StructType(fields=[
    StructField("qualifyingId", IntegerType(), nullable=False),
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("constructorId", IntegerType(), nullable=False),
    StructField("number", IntegerType(), nullable=False),
    StructField("position", IntegerType(), nullable=True),  
    StructField("q1", StringType(), nullable=True),    
    StructField("q2", StringType(), nullable=True),    
    StructField("q3", StringType(), nullable=True)
])

# COMMAND ----------

raw_path = "/mnt/formula1projectstorage/raw/qualifying/"
file_name = "qualifying_split_*.json"

qualifying_df = spark.read.json(path=f"{raw_path}{file_name}", schema=qualify_schema, multiLine=True)

# COMMAND ----------

qualifying_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Rename and add the columns
# MAGIC ```
# MAGIC   1. Rename driverID and raceId
# MAGIC   2. Add ingestion_Date with current_timestamp
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

qualifying_final_df = qualifying_df.withColumnRenamed("qualifyingId", "qualifying_id")\
                              .withColumnRenamed("driverId", "driver_id")\
                              .withColumnRenamed("raceId", "race_id")\
                              .withColumnRenamed("constructorId", "constructor_id")\
                              .withColumn("ingestion_date", current_timestamp())      

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Write output to processed container in parquet format

# COMMAND ----------

processed_path = "/mnt/formula1projectstorage/processed/qualifying/"

qualifying_final_df.write.parquet(path=processed_path, mode="overwrite")

# COMMAND ----------

# display(spark.read.parquet(processed_path))

# COMMAND ----------

