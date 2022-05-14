# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Drivers.json file

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step1 - Read the JSON file using the spark dataframe reader API
# MAGIC ```
# MAGIC It is a nested JSON file - so we will define 2 schemas - one at the file level, and the other for the nested object.
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

name_schema = StructType(fields = [ StructField("forename", StringType(), nullable=True), StructField("surname", StringType(), nullable=True) ])

driver_schema = StructType(fields = 
                              [
                                  StructField("driverId", IntegerType(), nullable=False),
                                  StructField("driverRef", StringType(), nullable=True),
                                  StructField("code", StringType(), nullable=True),
                                  StructField("name", name_schema, nullable=True),
                                  StructField("dob", DateType(), nullable=True),
                                  StructField("nationality", StringType(), nullable=True),
                                  StructField("url", StringType(), nullable=True)
                              ])


# COMMAND ----------

drivers_df = spark.read.json("/mnt/formula1projectstorage/raw/drivers.json", schema = driver_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Rename the columns and add new columns
# MAGIC ```
# MAGIC   1. driverid renamed to driver_id
# MAGIC   2. driverRef renamed to driver_ref
# MAGIC   3. ingestion date added
# MAGIC   4. name added with concatenation of forename and lastname
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp, lit, concat

# COMMAND ----------

drivers_renamed_df = drivers_df.withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("driverRef", "driver_ref")\
                        .withColumn("ingestion_date", current_timestamp())\
                        .withColumn("name", concat(col("name.forename"), lit(" ") , col("name.surname")) )

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Drop the unwanted columns
# MAGIC ```
# MAGIC   1. name.forname and name.lastname
# MAGIC ```

# COMMAND ----------

drivers_final_df = drivers_renamed_df.drop(col("url"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step4 - Write the output to the processed container in parquet format

# COMMAND ----------

processed_path = "/mnt/formula1projectstorage/processed/drivers"
drivers_final_df.write.parquet(path=processed_path, mode="overwrite")

# COMMAND ----------

# display(spark.read.parquet(proicessed_path))
