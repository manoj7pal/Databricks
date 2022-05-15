# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest lap_times.csv file
# MAGIC 
# MAGIC ```
# MAGIC  - Multiple files in a folder
# MAGIC ```

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
# MAGIC ##### Step1 - Read the CSV files in the lap_times folder, using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# COMMAND ----------

lap_times_schema = StructType(fields=[
    StructField("raceId", IntegerType(), nullable=False),
    StructField("driverId", IntegerType(), nullable=False),
    StructField("lap", IntegerType(), nullable=True),
    StructField("position", IntegerType(), nullable=True),
    StructField("time", StringType(), nullable=True),    
    StructField("milliseconds", IntegerType(), nullable=True),    
])

# COMMAND ----------

file_name = "lap_times*.csv"
path = f"{raw_dir_path}/lap_times/{file_name}"

lap_times_df = spark.read.csv(path=path, schema=lap_times_schema)

# COMMAND ----------

lap_times_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Rename and add the columns
# MAGIC ```
# MAGIC   1. Rename driverID and raceId
# MAGIC   2. Add ingestion_Date with current_timestamp
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

lap_times_final_df = add_ingestion_date(lap_times_df)\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumn("datasource", lit(v_datasource))\
                                .withColumn("environment", lit(v_env)) 

#                                 .withColumn("ingestion_date", current_timestamp())      

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Write output to processed container in parquet format

# COMMAND ----------

processed_path = f"{processed_dir_path}/lap_times/"

lap_times_final_df.write.parquet(path=processed_path, mode="overwrite")

# COMMAND ----------

# display(spark.read.parquet(processed_path))

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.exit(f"{notebook_name}: Success")