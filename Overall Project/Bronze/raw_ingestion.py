# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

raw_df=spark.read.format('csv').option('header',True).option('inferschema',True).load('/Volumes/workspace/default/de-1/de-1.csv')
display(raw_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists de1

# COMMAND ----------

# MAGIC %sql 
# MAGIC create schema if not exists de1.bronze

# COMMAND ----------

raw_df.write.format('delta').saveAsTable('de1.bronze.raw_df')

# COMMAND ----------

spark.read.table('de1.bronze.raw_df').display()

# COMMAND ----------

# %sql
# drop table if exists de1.bronze.raw_df

# COMMAND ----------

