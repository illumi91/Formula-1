# Databricks notebook source
# MAGIC %md
# MAGIC #### Mount Azure Data Lake using Service Principle
# MAGIC 1. Get client_id, tenant_id and clieant_secret from key vault
# MAGIC 3. Set Spark Config with App / Client ID, Directory/Tenant ID and Secret
# MAGIC 3. Call file system utility mount to mount the storage
# MAGIC 4. Explore other file system utilities related to mount (list all mounts, unmount)

# COMMAND ----------

dbutils.secrets.listScopes()

# COMMAND ----------

client_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-client-id")
tenant_id = dbutils.secrets.get(scope="formula1-scope", key="formula1-tenant-id")
client_secret = dbutils.secrets.get(scope="formula1-scope", key="formula1-client-secret")

configs = {"fs.azure.account.auth.type": "OAuth",
          "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
          "fs.azure.account.oauth2.client.id": client_id,
          "fs.azure.account.oauth2.client.secret": client_secret,
          "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"}


# COMMAND ----------

def mount_container(container, storage_account, configs):
    """Mount a container to DBFS if it doesn't exist already
    """
    if any(mount.mountPoint == f"/mnt/{storage_account}/{container}" for mount in dbutils.fs.mounts()):
        print(f"Skipping mount for container `{container}` as it already exists.")
    else:
        dbutils.fs.mount(
            source = f"abfss://{container}@{storage_account}.dfs.core.windows.net/",
            mount_point = f"/mnt/{storage_account}/{container}",
            extra_configs = configs
        )
        print(f"Mounted container `{container}`.")

# COMMAND ----------

containers_to_mount = ["demo", "raw", "processed", "presentation"]
storage_account = "f1databrickscourself"

for container in containers_to_mount:
    mount_container(container, storage_account, configs)

display(dbutils.fs.mounts())
