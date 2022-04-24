# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 4 Utilities:
# MAGIC * DB Filesystem Utilities
# MAGIC * Notebook Utilities
# MAGIC * Widgets Utilities
# MAGIC * Secret Utilities
# MAGIC * Library Utilities

# COMMAND ----------

# MAGIC %fs
# MAGIC ls

# COMMAND ----------

dbutils.fs.ls('/')

# COMMAND ----------

for folder_name in dbutils.fs.ls('/'):
    print(folder_name)

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

dbutils.fs.help("mount")

# COMMAND ----------

dbutils.notebook.help()

# COMMAND ----------

# Calling a chidl notebook
dbutils.notebook.run("./ChildNotebook", 10, {"input": "Called from Main Notebook...."}) # timeout  10secs

# COMMAND ----------

#Library Utilities
%pip install pandas

# COMMAND ----------

