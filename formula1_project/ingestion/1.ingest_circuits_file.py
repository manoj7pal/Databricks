# Databricks notebook source
# MAGIC %md
# MAGIC #### Ingest circuits.csv file

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step1 - Read CSV file using the spark dataframe reader
# MAGIC ```
# MAGIC Notes: 
# MAGIC   1. header : to specify that the first line contains the column names.
# MAGIC   2. inferSchema = automatically detects the correct datatype. 
# MAGIC   3. schema = inferSchema process is inefficient, as the driver node goes through entire dataset to determine the types. To improvise, we should explicity define and change the schema while loading.
# MAGIC   4. 
# MAGIC ```

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# Define schema
circuits_schema = StructType(
                        fields = [
                                    StructField("circuitId", IntegerType(), nullable=False),
                                    StructField("circuitRef", StringType(), nullable=True),
                                    StructField("name", StringType(), nullable=True),
                                    StructField("location", StringType(), nullable=True),
                                    StructField("country", StringType(), nullable=True),
                                    StructField("lat", DoubleType(), nullable=True),
                                    StructField("lng", DoubleType(), True),
                                    StructField("alt", IntegerType(), True),
                                    StructField("url", StringType(), nullable=True)
                                ] )

# Load csv file
file = "/mnt/formula1projectstorage/raw/circuits.csv"
circuits_df = spark.read.csv(file , header = True, schema = circuits_schema)

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step2 - Select only required columns 
# MAGIC ```
# MAGIC   - 4 ways to select the required columns
# MAGIC   - the plain select does not allow to perform any column-level operations(like alias etc.), whereas the other ways mentioned-below allowsthe same.
# MAGIC ```

# COMMAND ----------

# circuits_selected_df = circuits_df.select("circuitId", "circuitRef", "name", "location", "country", "lat", "lng", "alt")
# circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)
# circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# from pyspark.sql.functions import col
# circuits_selected_df = circuits_df.select( col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt") )

circuits_selected_df = circuits_df.select(circuits_df.circuitId, circuits_df.circuitRef, circuits_df.name, circuits_df.location, 
                                              circuits_df.country, circuits_df.lat, circuits_df.lng, circuits_df.alt)


# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step3 - Rename the columns
# MAGIC ```
# MAGIC 1. withColumnRenamed(existing, new)
# MAGIC 
# MAGIC ```

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed( "circuitId", "circuit_id" )\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step4 - Add Ingestion date and a literal column i.e adding a column and a non-column type in existing dataframe

# COMMAND ----------

# adding a new column - here current_timestamp() return a column value. If we want to add a constant/literal value for each row we have wrap lit(<constant>) around the constant value.
from pyspark.sql.functions import current_timestamp, lit

circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())\
                    .withColumn("env", lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step5 - Write data to Azure DataLake as Parquet file

# COMMAND ----------

storage_account_name = "formula1projectstorage"
processed_path = f"/mnt/{storage_account_name}/processed/circuits"

circuits_final_df.write.parquet(processed_path, mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls("/mnt/formula1projectstorage/processed/circuits"))

# COMMAND ----------

#Read the written file
df = spark.read.parquet("/mnt/formula1projectstorage/processed/circuits")
display(df)

# COMMAND ----------

# #Notes: 
# circuits_df = spark.read.option("header", True).csv("dbfs:/mnt/formula1projectstorage/raw/circuits.csv")
# circuits_df = spark.read.csv("dbfs:/mnt/formula1projectstorage/raw/circuits.csv", header = True, inferSchema = True)
# type(circuits_df)
# circuits_df.show()
# circuits_df.printSchema()
# display(circuits_df.describe())
# display(circuits_df)
# %fs
# ls /mnt/formula1projectstorage/processed/circuits
# display(circuits_final_df)