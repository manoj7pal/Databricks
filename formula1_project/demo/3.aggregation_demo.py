# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Built-in Aggregate functions

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

path=f"{presentation_dir_path}/race_results/"

race_results_df = spark.read.parquet(path)

# COMMAND ----------

display(race_results_df)

# COMMAND ----------

# 2020 races

df_2020 = race_results_df.where(" race_year=2020 ")
display(df_2020)


# COMMAND ----------

from pyspark.sql.functions import count, countDistinct, sum, avg, desc

# COMMAND ----------

df_2020.select( count("*") ).show()

# COMMAND ----------

df_2020.select(count('race_name')).show()

# COMMAND ----------

df_2020.select(countDistinct('race_name')).show()

# COMMAND ----------

df_2020.select( sum('points') ).show()

# COMMAND ----------

df_2020.where("driver_name= 'Lewis Hamilton' ").select( sum('points') ).show()

# COMMAND ----------

df_2020.where(" driver_name='Lewis Hamilton' ").select( sum('points'), countDistinct('race_name') )\
        .withColumnRenamed('sum(points)', 'total_points')\
        .withColumnRenamed('count(DISTINCT race_name)', 'number_of_races')\
        .show()

# COMMAND ----------

df_2020.groupBy("driver_name")\
        .sum("points")\
        .withColumnRenamed("sum(points)", "total_points")\
        .orderBy(desc('total_points'))\
        .show()

# COMMAND ----------

df_2020.groupBy('driver_name')\
        .agg( sum("points"), countDistinct("race_name") )\
            .withColumnRenamed("sum(points)", "total_points")\
            .withColumnRenamed("count(race_name)", "no_of_races")\
        .orderBy( desc("total_points") )\
        .show()

# COMMAND ----------

# Applying Multiple aggregate functions over the dataframe, sorting it, and creating alias for the aggregated columns using alias method

df_2020.groupBy('driver_name')\
        .agg( sum("points").alias("total_points"), countDistinct("race_name").alias("no_of_races") )\
        .orderBy( desc("total_points") )\
        .show()