# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

circuits_df = spark.read.parquet(f"{processed_dir_path}/circuits")\
                    .withColumnRenamed("name", "circuit_name")

races_df = spark.read.parquet(f"{processed_dir_path}/races")\
                .withColumnRenamed("name", "race_name")\
                .where(" race_year=2019")


# COMMAND ----------

# INNER JOIN - By Default
#  2019 Races can be run on the same circuits, Many races on same circuits

races_circuits_df = races_df.join(circuits_df, on="circuit_id")
display(races_circuits_df)


# COMMAND ----------

races_circuits_df = races_df.join(circuits_df, on=(races_df.circuit_id == circuits_df.circuit_id), how="inner" )\
                            .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(races_circuits_df)

# COMMAND ----------

#LEFT OUTER JOIN - 

circuits_left_join_races_df = circuits_df.join(races_df, on=(circuits_df.circuit_id == races_df.circuit_id), how='left')\
                                        .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(circuits_left_join_races_df)

# COMMAND ----------

#RIGHT OUTER JOIN - 

races_left_join_circuits_df = races_df.join(circuits_df, on=(circuits_df.circuit_id == races_df.circuit_id), how='right')\
                                        .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(circuits_left_join_races_df)

# COMMAND ----------

#FULL OUTER JOIN - 

races_full_outer_join_circuits_df = races_df.join(circuits_df, on=(circuits_df.circuit_id == races_df.circuit_id), how='full')\
                                        .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

display(races_full_outer_join_circuits_df)

# COMMAND ----------

#CROSS JOIN - 

races_cross_join_circuits_df = races_df.join(circuits_df, how='cross')\
                                        .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country, races_df.race_name, races_df.round)

print(races_cross_join_circuits_df.count())

# COMMAND ----------

# SEMI JOINS - Access only left side table columns

circuits_semi_join_races_df = circuits_df.join(races_df, on=(circuits_df.circuit_id == races_df.circuit_id), how='semi')\
                                        .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country)

display(circuits_semi_join_races_df)

# COMMAND ----------

# ANTI JOINS -  left side table rows which does not EXIST in right side table, A-B

circuits_anti_join_races_df = circuits_df.join(races_df, on=(circuits_df.circuit_id == races_df.circuit_id), how='anti')\
                                         .select(circuits_df.circuit_id, circuits_df.circuit_name, circuits_df.location, circuits_df.country)

display(circuits_anti_join_races_df)