# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

raw_df=spark.read.table('de1.bronze.raw_df')

# COMMAND ----------

# MAGIC %sql
# MAGIC select merchant_id
# MAGIC from de1.bronze.raw_df
# MAGIC where merchant_id not like 'MERCH-%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select payment_method
# MAGIC from de1.bronze.raw_df
# MAGIC where payment_method not in('Credit Card','Debit Card','Cash',"Wallet","Bank Transfer")

# COMMAND ----------

# MAGIC %sql
# MAGIC     select transaction_currency
# MAGIC     from de1.bronze.raw_df
# MAGIC     where transaction_currency not in("USD", "EUR", "AUD","GBP", "INR", "CAD")

# COMMAND ----------

# MAGIC %sql
# MAGIC select transaction_id
# MAGIC from de1.bronze.raw_df
# MAGIC where transaction_id not LIKE  'TXN-%'

# COMMAND ----------

# MAGIC %sql
# MAGIC select region
# MAGIC from de1.bronze.raw_df
# MAGIC where region not in ("India","Europe","Latin America",'Asia-Pacific','US-West','US-East')

# COMMAND ----------

