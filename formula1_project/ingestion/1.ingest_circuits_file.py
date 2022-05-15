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
# MAGIC 
# MAGIC   - Including variables from other notebooks - includes/configuration and includes/common_functions
# MAGIC   - Added Widgets -> to pass runtime notebook parameters
# MAGIC ```

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# Adding widgets to pass runtime notebook parameters
dbutils.widgets.text("p_datasource", "")
v_datasource = dbutils.widgets.get("p_datasource")

dbutils.widgets.text("p_env", "")
v_env = dbutils.widgets.get("p_env")

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
file = f"{raw_dir_path}/circuits.csv"

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

from pyspark.sql.functions import lit

circuits_renamed_df = circuits_selected_df.withColumnRenamed( "circuitId", "circuit_id" )\
    .withColumnRenamed("circuitRef", "circuit_ref")\
    .withColumnRenamed("lat", "latitude")\
    .withColumnRenamed("lng", "longitude")\
    .withColumnRenamed("alt", "altitude")\
    .withColumn("datasource", lit(v_datasource))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step4 - Add Ingestion date and a literal column i.e adding a column and a non-column type in existing dataframe

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# adding a new column - here current_timestamp() return a column value. If we want to add a constant/literal value for each row we have wrap lit(<constant>) around the constant value.
# from pyspark.sql.functions import current_timestamp, lit

# circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())\
#                     .withColumn("env", lit("Production"))

circuits_final_df = add_ingestion_date(circuits_renamed_df).withColumn("environment", lit(v_env))

# COMMAND ----------

# MAGIC %md
# MAGIC 
# MAGIC ##### Step5 - Write data to Azure DataLake as Parquet file

# COMMAND ----------

# storage_account_name = "formula1projectstorage"
# processed_path = f"/mnt/{storage_account_name}/processed/circuits"
processed_path = f"{processed_dir_path}/circuits"


circuits_final_df.write.parquet(processed_path, mode="overwrite")

# COMMAND ----------

display(dbutils.fs.ls(f"{processed_dir_path}/circuits"))

# COMMAND ----------

#Read the written file
df = spark.read.parquet(f"{processed_dir_path}/circuits")
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

# COMMAND ----------

notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()

dbutils.notebook.exit(f"{notebook_name}: Success")