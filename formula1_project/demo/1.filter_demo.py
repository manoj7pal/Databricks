# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

races_df = spark.read.parquet(f"{processed_dir_path}/races")

# COMMAND ----------

races_filtered_df1 = races_df.where( (races_df.race_year==2019) & (races_df.round<=5) )

races_filtered_df1.count()

# COMMAND ----------

races_filtered_df2 = races_df.where(" race_year=2019 and round<=5 ")

races_filtered_df2.count()

# COMMAND ----------

