# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 : Segregate Data 
# MAGIC #### - **Valid:** Passes all mandatory rules (2-7)
# MAGIC #### - **Invalid:** Fails one or more mandatory rules
# MAGIC #### - **Anomaly:** Passes mandatory rules but detected in Rules 8-10

# COMMAND ----------

sample_df=spark.read.table('de1.silver.valid_df')
display(sample_df)

# COMMAND ----------

print(sample_df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC #### This is the final_valid_df

# COMMAND ----------

final_valid_df=sample_df.select("transaction_id",'customer_id','merchant_id','transaction_amount','transaction_currency','transaction_date','payment_method','product_category','product_name','status','region','timestamp','ip_address','device_type')
display(final_valid_df)

# COMMAND ----------

print(final_valid_df.count())

# COMMAND ----------

sample_invalid_df=spark.read.table('de1.silver.tasked_df')
display(sample_invalid_df)

# COMMAND ----------

checking_invalid_df=sample_invalid_df.filter(
    (col("rule2_status") == "Invalid") |
    (col("rule3_status") == "Invalid") |
    (col("rule4_status") == "Invalid") |
    (col("rule5_status") == "Invalid") |
    (col("rule6_status") == "Invalid") |
    (col("rule7_status") == "Invalid")
)

display(checking_invalid_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### This is the final_invalid_df

# COMMAND ----------

final_invalid_df=checking_invalid_df.select("transaction_id",'customer_id','merchant_id','transaction_amount','transaction_currency','transaction_date','payment_method','product_category','product_name','status','region','timestamp','ip_address','device_type')
display(final_invalid_df)

# COMMAND ----------

sample_anomaly_df=spark.read.table('de1.silver.tasked_df')
display(sample_anomaly_df)

# COMMAND ----------

checking_anomaly_df =sample_anomaly_df.filter(
    (col("rule2_status") == "Valid") &
    (col("rule3_status") == "Valid") &
    (col("rule4_status") == "Valid") &
    (col("rule5_status") == "Valid") &
    (col("rule6_status") == "Valid") &
    (col("rule7_status") == "Valid") &
    (
        (col("rule8_status") == "Anomaly") |
        (col("rule10_status") == "Anomaly")
    )
)
display(checking_anomaly_df)

# COMMAND ----------

checking_anomaly_df.select("transaction_id").filter(
    (col('rule8_status')=="Normal") &
    (col('rule10_status')=='Normal')
    ).show()

# COMMAND ----------

# MAGIC %md
# MAGIC #### This is the final_anomaly_df

# COMMAND ----------

final_anomaly_df=checking_anomaly_df.select("transaction_id",'customer_id','merchant_id','transaction_amount','transaction_currency','transaction_date','payment_method','product_category','product_name','status','region','timestamp','ip_address','device_type')
display(final_anomaly_df)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists de1.gold

# COMMAND ----------

# %sql
# drop table if exists de1.gold.final_anomaly_df


# COMMAND ----------

final_valid_df.write.format('delta').saveAsTable('de1.gold.final_valid_df')
final_invalid_df.write.format('delta').saveAsTable('de1.gold.final_invalid_df')
final_anomaly_df.write.format('delta').saveAsTable('de1.gold.final_anomaly_df')

# COMMAND ----------

spark.read.table('de1.gold.final_valid_df').display()


# COMMAND ----------

spark.read.table('de1.gold.final_invalid_df').display()


# COMMAND ----------

spark.read.table('de1.gold.final_anomaly_df').display()

# COMMAND ----------

