# Databricks notebook source
# MAGIC %md
# MAGIC ##### Ingest pit_stops.json file
# MAGIC 
# MAGIC ```
# MAGIC  - Multiline json file, only add 'multiLine=True' option during loading.
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step1 - Read the JSON file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

pit_stops_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("stop", IntegerType(), nullable=True),
    StructField("lap", IntegerType(), nullable=True),
    StructField("time", StringType(), nullable=True),
    StructField("duration", StringType(), nullable=True),    
    StructField("milliseconds", IntegerType(), nullable=True),    
])

# COMMAND ----------

raw_path = "/mnt/formula1projectstorage/raw/"
file_name = "pit_stops.json"

pit_stops_df = spark.read.json(path=f"{raw_path}{file_name}", schema=pit_stops_schema, multiLine=True )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Rename and add the columns
# MAGIC ```
# MAGIC   1. Rename driverID and raceId
# MAGIC   2. Add ingestion_Date with current_timestamp
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

pit_stops_final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id")\
                              .withColumnRenamed("raceId", "race_id")\
                              .withColumn("ingestion_date", current_timestamp())      

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Write output to processed container in parquet format

# COMMAND ----------

processed_path = "/mnt/formula1projectstorage/processed/pit_stops/"

pit_stops_final_df.write.parquet(path=processed_path, mode="overwrite")

# COMMAND ----------

# display(spark.read.parquet(processed_path))