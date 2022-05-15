# Databricks notebook source
# MAGIC %md
# MAGIC ## Ingest Drivers.json file

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

file_name = "drivers.json"
path = f"{raw_dir_path}/{file_name}"

drivers_df = spark.read.json(path, schema = driver_schema)

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

drivers_renamed_df = add_ingestion_date(drivers_df)\
                        .withColumnRenamed("driverId", "driver_id")\
                        .withColumnRenamed("driverRef", "driver_ref")\
                        .withColumn("name", concat(col("name.forename"), lit(" ") , col("name.surname")) )\
                        .withColumn("datasource", lit(v_datasource))\
                        .withColumn("environment", lit(v_env)) 

#                         .withColumn("ingestion_date", current_timestamp())\

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

processed_path = f"{processed_dir_path}/drivers"
drivers_final_df.write.parquet(path=processed_path, mode="overwrite")

# COMMAND ----------

# display(spark.read.parquet(proicessed_path))


# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.exit(f"{notebook_name}: Success")