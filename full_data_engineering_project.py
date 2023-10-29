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

# COMMAND ----------

configs = {
  "fs.azure.account.auth.type": "CustomAccessToken",
  "fs.azure.account.custom.token.provider.class": spark.conf.get("spark.databricks.passthrough.adls.gen2.tokenProviderClassName")
}

# Mounting the denormalized data container.
dbutils.fs.mount(
  source = "abfss://denormalized@supplychainobject.dfs.core.windows.net/",
  mount_point = "/mnt/denormalized",
  extra_configs = configs)

# COMMAND ----------

# Check the list of folders or files in the denormalized container
dbutils.fs.ls("/mnt/denormalized")

# COMMAND ----------

data = '/mnt/supply-chain-raw/data_e06f395b-5a79-4bb5-b089-ae66af73fdf6_a3f3d274-99c8-44de-937c-5d6150a2811f.parquet'

# COMMAND ----------

supply_data = spark.read.format('parquet').load(data)

# COMMAND ----------

display(supply_data)

# COMMAND ----------

supply_data.schema.names

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import from_utc_timestamp, date_format
from pyspark.sql.functions import year, quarter, month, weekofyear, dayofmonth, hour
from pyspark.sql.types import TimestampType
spark = SparkSession.builder.appName("datetime_columns_extraction").getOrCreate()
