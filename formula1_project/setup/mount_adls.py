# Databricks notebook source
# MAGIC %md
# MAGIC #### Mounting Azure DataLake Storage Account on Databricks using Azure Key Vault Secrets:
# MAGIC 
# MAGIC ```
# MAGIC 1. Create Container, if not exists
# MAGIC 2. Create Service Principal from Azure AD > App Registrations. Note the Client and the Tenant ID
# MAGIC 3. Create a Secret Key under the above created Service principal > Certificates & Secrets > New Client Secret. Note the Secret Value
# MAGIC 4. Create a Azure Key Vault for the Client Id, Tenant ID and the Client Secret Value.
# MAGIC 5. Create a Databricks Secre Scope by appending '#secrets/createScope/' in the databricks home page url. Enter the required DNS and resource id of the Azure Key Vault Secret under properties.
# MAGIC 6. Use with dbutils.secrets methods
# MAGIC ```

# COMMAND ----------

# Fetching the secrets value from the Azure Key Vault, to connect with the Service Principal. Then fetch it using the databricks secret scope. Here formula1-scope is the databricks secret scope whic is mapped with KeyVault.
storage_account_name = "formula1projectstorage"
client_id = dbutils.secrets.get(scope = "formula1-scope", key="databricks-app-client-id") # Service Principal - Client(Application) ID
tenant_id = dbutils.secrets.get(scope = "formula1-scope", key="databricks-app-tenant-id") # Service Principal - Tenant(Directory) ID
client_secret = dbutils.secrets.get(scope = "formula1-scope", key="databricks-app-client-secret-value") # Service Principal - Client Secret Value

# COMMAND ----------

configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id": f"{client_id}",
    "fs.azure.account.oauth2.client.secret": f"{client_secret}",
    "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
}

# COMMAND ----------

#Generic function to mount container
def mount_adls(container_name):
    dbutils.fs.mount(
        source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
        mount_point = f"/mnt/{storage_account_name}/{container_name}",
        extra_configs = configs
    )

# COMMAND ----------

#Mounting the Container from DBFS
containers_to_mount = ["raw", "processed"]

for container in containers_to_mount:
    mount_adls("processed")

# COMMAND ----------

for mnt in dbutils.fs.mounts():
    print(mnt)

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectstorage/raw")

# COMMAND ----------

dbutils.fs.ls("/mnt/formula1projectstorage/processed/")

# COMMAND ----------

#Unmounting the Container from DBFS
path = "/mnt/formula1projectstorage/"
containers_to_unmount = ["raw", "processed"]

for container in containers_to_unmount:
    dbutils.fs.unmount(path+container)

# COMMAND ----------

for mnt in dbutils.fs.mounts():
    print(mnt)

# COMMAND ----------

# Notes: 

# #Mount container
# container_name = "raw"

# dbutils.fs.mount(
#     source = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
#     mount_point = f"/mnt/{storage_account_name}/{container_name}",
#     extra_configs = configs
# )

# dbutils.fs.ls("/mnt/formula1projectstorage/raw")

# dbutils.fs.mounts()

# mount_adls("processed")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### 3 steps to secure the sensisitve information: 
# MAGIC   1. Create Azure KeyVault Secrets 
# MAGIC   2. Map it with Databricks SecretSCope, so that we can use it in Databricks Notebooks 
# MAGIC   3. Fecth the values of the variables using dbutils.secrets methods.
# MAGIC 
# MAGIC * Exposing the Storage name, client id, tenant id, and client secret can be risky, so we have to keep it in a secret place. In Azure, that place is Azure Key Vault > Secrets. 
# MAGIC * To create a Databricks Secre Scope --> Append '#secrets/createScope/' after the Databricks home page url
# MAGIC * Vault URI(DNS) and the Resource ID can be found under Key Vault Secrets Properties, which we have to enter in DB SceretScope.
# MAGIC * Then, we can use the dbutils.secrets method to access the values. use dbutils.screts.help() for more information.
# MAGIC 
# MAGIC ```
# MAGIC a. dbutils.secrets.help()<br>
# MAGIC b. dbutils.secrets.listScopes()<br>
# MAGIC c. dbutils.secrets.list("formula1-scope")<br>
# MAGIC d. dbutils.secrets.get(scope = "formula1-scope", key="databricks-app-client-id")<br>
# MAGIC ```