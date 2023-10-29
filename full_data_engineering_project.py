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

# COMMAND ----------

# Creating the Date Dimension table
import pyspark.sql.functions as F
date_dimension = supply_data.select("order_date_dateorders").distinct()


date_dimension = date_dimension.withColumn("Year", F.year(date_dimension["order_date_dateorders"]))
date_dimension = date_dimension.withColumn("Quarter", F.quarter(date_dimension["order_date_dateorders"]))
date_dimension = date_dimension.withColumn("Month", F.month(date_dimension["order_date_dateorders"]))
date_dimension = date_dimension.withColumn("Week", F.weekofyear(date_dimension["order_date_dateorders"]))
date_dimension = date_dimension.withColumn("Day", F.dayofmonth(date_dimension["order_date_dateorders"]))
date_dimension = date_dimension.withColumn("Hour", F.hour(date_dimension["order_date_dateorders"]))

# COMMAND ----------

# Creating the Product Dimension table
product_dimension = supply_data.select("product_name", "product_status", "product_price", "product_card_id").distinct()

# COMMAND ----------

# Creating the Category Dimension table
category_dimension = supply_data.select("category_id", "category_name").distinct()

# COMMAND ----------

# Creating the Department Dimension table
department_dimension = supply_data.select('department_id','department_name').distinct()

# COMMAND ----------

# Creating the Customer Dimension table
customer_dimension = supply_data.select('customer_email','customer_fname','customer_id','customer_lname','customer_segment').distinct()

# COMMAND ----------

# Creating the Customer and Order Region Dimension table
region_dimension = supply_data.select('customer_city','customer_country','customer_state','customer_street','customer_zipcode','latitude','longitude','order_city','order_country').distinct()

# COMMAND ----------

# Creating the Fact table
supplychain_fact_table = supply_data.select('type','days_for_shipping_real','days_for_shipment_scheduled','benefit_per_order','sales_per_customer','delivery_status','late_delivery_risk',
 'category_id','customer_id','customer_zipcode','department_id','latitude','longitude','market','order_customer_id','order_date_dateorders','order_id','order_item_cardprod_id','order_item_discount','order_item_discount_rate','order_item_id','order_item_product_price','order_item_profit_ratio','order_item_quantity','sales','order_item_total','order_profit_per_order','order_region','order_state','order_status','product_card_id','shipping_order_date','shipping_mode')

# COMMAND ----------



# Replace "path_in_data_lake" with the desired path within your Azure Data Lake Storage container

source = "abfss://denormalized@supplychainobject.dfs.core.windows.net/"
mount_point = "/mnt/denormalized"
#path_in_data_lake = "/mnt/denormalized/"

# Save DataFrames to Azure Data Lake Storage
supplychain_fact_table.write.parquet(source + mount_point + " " + "supplychain_fact_table")
region_dimension.write.parquet(source + mount_point + " " + "region_dimension")
customer_dimension.write.parquet(source + mount_point + " " + "customer_dimension")
department_dimension.write.parquet(source + mount_point + " " + "department_dimension")
category_dimension.write.parquet(source + mount_point + " " + "category_dimension")
product_dimension.write.parquet(source + mount_point + " " + "product_dimension")
date_dimension.write.parquet(source + mount_point + " " + "date_dimension")

# COMMAND ----------

category_data = '/mnt/denormalized/denormalized category_dimension/'

# COMMAND ----------

dbutils.fs.ls("/mnt/denormalized/mnt/denormalized category_dimension/")

# COMMAND ----------

cat_data = '/mnt/denormalized/mnt/denormalized category_dimension/part-00000-tid-4490939333151561952-e15b6beb-00ec-4c64-957c-a4d5e52b4d5a-35-1.c000.snappy.parquet'

# COMMAND ----------

dim_data = spark.read.format('parquet').load(cat_data)

# COMMAND ----------

display(dim_data)

# COMMAND ----------

display(dim_data.category_id)
