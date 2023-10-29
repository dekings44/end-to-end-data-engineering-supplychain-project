# Databricks notebook source
configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
  source = "abfss://supply-chain-raw@supplychainobject.dfs.core.windows.net/",
  mount_point = "/mnt/supply-chain-raw",
  extra_configs = configs)

# COMMAND ----------

# Check the list of folders or files in the container
dbutils.fs.ls("/mnt/supply-chain-raw")
