# Databricks notebook source
# MAGIC %md
# MAGIC ##### Load the required files in the dataframes

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_dir_path}/races").withColumnRenamed("name", "race_name")
circuits_df = spark.read.parquet(f"{processed_dir_path}/circuits")
drivers_df = spark.read.parquet(f"{processed_dir_path}/drivers").withColumnRenamed("name", "driver_name")
constructors_df = spark.read.parquet(f"{processed_dir_path}/constructors").withColumnRenamed("name", "team")
results_df = spark.read.parquet(f"{processed_dir_path}/results")

# COMMAND ----------

# display(drivers_df)

# dfs = ["races_df", "circuits_df", "drivers_df", "constructors_df", "results_df"]


# COMMAND ----------

races_df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, desc

# COMMAND ----------

race_results_df = circuits_df.join(races_df, on=(circuits_df.circuit_id==races_df.circuit_id) )\
                            .join(results_df, on=(results_df.race_id == races_df.race_id) )\
                            .join(drivers_df, on=(results_df.driver_id == drivers_df.driver_id) )\
                            .join(constructors_df, on=(results_df.constructor_id == constructors_df.constructor_id) )\
                            .select(races_df.race_year, races_df.race_name, races_df.race_timestamp, circuits_df.location, drivers_df.driver_name, drivers_df.driver_id, drivers_df.nationality, constructors_df.team, results_df.rank,  results_df.grid, results_df.fastest_lap, results_df.time, results_df.points)\
                            .withColumnRenamed("time", "race_time")\
                            .withColumnRenamed("name", "race_name")\
                            .withColumnRenamed("race_timestamp", "race_date")\
                            .withColumnRenamed("location", "circuit_location")\
                            .withColumnRenamed("driver_id", "driver_number")\
                            .withColumnRenamed("nationality", "driver_nationality")\
                            .withColumn("created_date", current_timestamp())
    

# COMMAND ----------

#Abu Dhabi Results, order by points

display( race_results_df.where("race_year=2020 and race_name='Abu Dhabi Grand Prix'  ").orderBy(race_results_df.points.desc() ) )


# COMMAND ----------

path = f"{presentation_dir_path}/race_results"

# race_results_df.write.parquet(path=path, mode="overwrite", partitionBy=["race_year", "race_name"])

race_results_df.write.parquet(path=path, mode="overwrite")