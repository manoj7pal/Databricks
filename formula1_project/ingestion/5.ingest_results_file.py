# Databricks notebook source
# MAGIC %md
# MAGIC ##### Step1 - Load the JSON file
# MAGIC ```
# MAGIC   Single line - Plain JSON file
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

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType

# COMMAND ----------

results_schema = StructType(fields=[
                                StructField("resultId", IntegerType(), nullable=False),
                                StructField("raceId", IntegerType(), nullable=True),
                                StructField("driverId",IntegerType(), nullable=True),
                                StructField("constructorId",IntegerType(), nullable=True),
                                StructField("number", IntegerType(), nullable=True),
                                StructField("position", IntegerType(), nullable=True),
                                StructField("positionText", StringType(), nullable=True),
                                StructField("positionOrder", IntegerType(), nullable=True),
                                StructField("points", FloatType(), nullable=True),
                                StructField("laps", IntegerType(), nullable=True),
                                StructField("time", StringType(), nullable=True),
                                StructField("milliseconds", IntegerType(), nullable=True),
                                StructField("fastestLap", IntegerType(), nullable=True),
                                StructField("rank", IntegerType(), nullable=True),
                                StructField("fastestLapTime", StringType(), nullable=True),
                                StructField("fastestLapSpeed", FloatType(), nullable=True),    
                                StructField("statusId", IntegerType(), nullable=True),    
                            ])

# COMMAND ----------

file_name = "results.json"
path = f"{raw_dir_path}/{file_name}"

results_df = spark.read.json(path=path, schema=results_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step2 - Transformation and Drop Columns
# MAGIC ```
# MAGIC   1. Rename the columns to match _ case
# MAGIC   2. Add the ingested_date
# MAGIC   3. Drop StatusId columns
# MAGIC ```

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

results_renamed_df = add_ingestion_date(results_df)\
                                .withColumnRenamed("resultId", "result_id")\
                                .withColumnRenamed("raceId", "race_id")\
                                .withColumnRenamed("resultId", "result_id")\
                                .withColumnRenamed("driverId", "driver_id")\
                                .withColumnRenamed("constructorId", "constructor_id")\
                                .withColumnRenamed("positionText", "position_text")\
                                .withColumnRenamed("positionOrder", "position_order")\
                                .withColumnRenamed("fastestLap", "fastest_lap")\
                                .withColumnRenamed("fastestLapTime", "fastest_lap_time")\
                                .withColumnRenamed("fastestLapSpeed", "fastest_lap_speed")\
                                .withColumn("datasource", lit(v_datasource))\
                                .withColumn("environment", lit(v_env)) 
                                
# .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

results_final_df = results_renamed_df.drop("statusId")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Write the data to ADLS as parquet format

# COMMAND ----------

processed_path = f"{processed_dir_path}/results"

results_final_df.write.parquet(path=processed_path, mode="overwrite", partitionBy="race_id")

# COMMAND ----------

#Displaying on Race_ID=1 results
# display(spark.read.parquet(f"{processed_path}/race_id=1/"))

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.exit(f"{notebook_name}: Success")