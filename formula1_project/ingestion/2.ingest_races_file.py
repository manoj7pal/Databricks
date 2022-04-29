# Databricks notebook source
# Define the schema
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType

# Create schema - drop url column
race_schema = StructType(fields=[ 
    StructField("raceid", IntegerType(), nullable=False),
    StructField("year", IntegerType(), nullable=True),
    StructField("round", IntegerType(), nullable=True),
    StructField("circuitId", IntegerType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("date", DateType(), nullable=True),
    StructField("time", StringType(), nullable=True),
    StructField("url", StringType(), nullable=True)
])

# COMMAND ----------

# Load the file with the above schema
file_name = "races.csv"
path = "/mnt/formula1projectstorage/raw/"

race_df = spark.read.csv(path+file_name, header=True, schema=race_schema)
# display(race_df)

# COMMAND ----------

# Select relevant columns
race_selected_df = race_df.select("raceid", "year", "round", "circuitid", "name", "date", "time")


# COMMAND ----------

# Rename Columns
race_renamed_df = race_selected_df.withColumnRenamed("raceid", "race_id")\
    .withColumnRenamed("year", "race_year")\
    .withColumnRenamed("circuitid", "circuit_id")


# COMMAND ----------

# Add new columns
from pyspark.sql.functions import current_timestamp, lit, to_timestamp, concat, col

race_final_df = race_renamed_df.withColumn("ingestion_date", current_timestamp())\
    .withColumn("race_timestamp", to_timestamp( concat( col("date"),lit(""),col("time") ), "yyyy-MM-ddHH:mm:ss") )

# COMMAND ----------

# Write file back to ADLS, as a parquet file

path = "/mnt/formula1projectstorage/processed/races/"
race_final_df.write.parquet(path, mode="overwrite")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC #### Partition the data while writing to ADLS

# COMMAND ----------

# Write file back to ADLS, as a parquet file, and parition it by race_year
# Partition-at-rest --> race_year

path = "/mnt/formula1projectstorage/processed/races_partition/"
race_final_df.write.partitionBy('race_year').parquet(path, mode="overwrite")

# COMMAND ----------

display(spark.read.parquet(path))

# COMMAND ----------

# display(race_newcol_df)
# race_df.printSchema()