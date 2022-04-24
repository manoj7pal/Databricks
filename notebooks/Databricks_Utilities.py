# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC #### 4 Utilities:
# MAGIC * DB Filesystem Utilities : dbutils.
# MAGIC * Notebook Utilities: dbutils.notebook.
# MAGIC * Widgets Utilities: dbutils.widgets.
# MAGIC * Secret Utilities: 
# MAGIC * Library Utilities: %pip install .....
# MAGIC 
# MAGIC ######  Help: dbutils.fs.help(), dbutils.notebook.help("<function_name>"), dbutils.widgets.help() and so on

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

