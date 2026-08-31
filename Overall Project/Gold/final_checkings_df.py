# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

raw_df=spark.read.table('de1.bronze.raw_df')
tasked_df=spark.read.table('de1.silver.tasked_df')
valid_df=spark.read.table('de1.silver.valid_df')
anomaly_df=spark.read.table('de1.gold.final_anomaly_df')
invalid_df=spark.read.table('de1.gold.final_invalid_df')


# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 5 : Generate Quality Report
# MAGIC #### - Overall quality score 
# MAGIC #### - Total records processed and breakdown by category 
# MAGIC #### - Rule-by-rule failure counts
# MAGIC #### - Data profile (distribution by region, payment method, etc.)
# MAGIC #### - Top failing merchants/customers
# MAGIC #### - Actionable recommendations
# MAGIC

# COMMAND ----------

#1.Overall Quality Score
total_records=raw_df.count()
valid_records=valid_df.count()
quality_score=(valid_records/total_records)*100

print("----------Overall Quality Records--------")
print("Total Records: ",total_records)
print("Valid Records: ",valid_records)
print("Quality Score: ",quality_score)

# COMMAND ----------

#2.Total records processed and breakdown by category 
#Here we processed the raw data in the silver layer and divided 2 df's called tasked_df and valid_df so 

total_records=raw_df.count()
processed_records=tasked_df.count()

print('----------Total Processed Records---------')
print('Total Records: ',total_records)
print('Processed Records: ',processed_records)

#Here the Sum of valid and invalid records should be equal to the total_records in the raw_df
print('----------Breakdown by Category---------')
print('total_records: ',raw_df.count())
print('valid_records',valid_df.count())
print('invalid_records',invalid_df.count())
print('Anomaly_records: ',anomaly_df.count())



# COMMAND ----------

# 3.Rule-by-rule failure counts
print('----------Rule-by-rule failure counts---------')
print('---This is for the rule1---')
checking_df=tasked_df.groupBy('rule1_status').count().filter(col('rule1_status')=='Invalid').show()
print('---This is for the rule2---')
checking_df=tasked_df.groupBy('rule2_status').count().filter(col('rule2_status')=='Invalid').show()
print('---This is for the rule3---')
checking_df=tasked_df.groupBy('rule3_status').count().filter(col('rule3_status')=='Invalid').show()
print('---This is for the rule4---')
checking_df=tasked_df.groupBy('rule4_status').count().filter(col('rule4_status')=='Invalid').show()
print('---This is for the rule5---')
checking_df=tasked_df.groupBy('rule5_status').count().filter(col('rule5_status')=='Invalid').show()
print('---This is for the rule6---')
checking_df=tasked_df.groupBy('rule6_status').count().filter(col('rule6_status')=='Invalid').show()
print('---This is for the rule7---')
checking_df=tasked_df.groupBy('rule7_status').count().filter(col('rule7_status')=='Invalid').show()
print('---This is for the rule8---')
checking_df=tasked_df.groupBy('rule8_status').count().filter(col('rule8_status')=='Anomaly').show()
print('---This is for the rule10---')
checking_df=tasked_df.groupBy('rule10_status').count().filter(col('rule10_status')=='Anomaly').show()
#

# COMMAND ----------

#4.Data Profile(distribution by region, payment method)
print('----------Data Profile(distribution by region, payment method)---------')
print('---This is for the region---')
checking_df=tasked_df.groupBy('region').count().show()
print('---This is for the payment method---')
checking_df=tasked_df.groupBy('payment_method').count().show()

# COMMAND ----------

#5.Top failing merchants/customers
print('----------Top failing merchants/customers---------')
print('---This is for the top failing merchants---')
checking_df=invalid_df.groupBy('merchant_id').count().orderBy(col('count').desc()).limit(10).show()
print('---This is for the top failing customers---')
checking_df=invalid_df.groupBy('customer_id').count().orderBy(col('count').desc()).limit(10).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## task 7 :Code Quality & Testing
# MAGIC #### Here we will be doing the data quality checks like
# MAGIC #### Row_count check
# MAGIC #### Random_record_checks
# MAGIC #### Stratifictaion_checks

# COMMAND ----------

# MAGIC %sql
# MAGIC --  # We will be checking wheather the data is safely landed in the Traget table or not
# MAGIC --  #1.Random Record Check
# MAGIC
# MAGIC  select * 
# MAGIC  from de1.bronze.raw_df
# MAGIC  where transaction_id = 'TXN-1000008'

# COMMAND ----------

# MAGIC %sql
# MAGIC  select * 
# MAGIC  from de1.gold.final_valid_df
# MAGIC  where transaction_id = 'TXN-1000008'

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #2.Row Count Check
# MAGIC
# MAGIC select count(*)
# MAGIC from de1.bronze.raw_df
# MAGIC where status='Completed'
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from de1.silver.tasked_df
# MAGIC where status='Completed'
# MAGIC     
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC -- #3.Stratification Checks.
# MAGIC
# MAGIC select sum(transaction_amount) as total_amount
# MAGIC from de1.bronze.raw_df
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select sum(transaction_amount) as total_amount
# MAGIC from de1.silver.tasked_df

# COMMAND ----------

