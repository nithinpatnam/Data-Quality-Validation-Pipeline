# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task-1
# MAGIC ##### 1.Loading the data into the Pyspark df

# COMMAND ----------

raw_df=spark.read.table('de1.bronze.raw_df')
display(raw_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Handling Nulls/Malformed records

# COMMAND ----------

#nulls check
null_counts = raw_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in raw_df.columns])
null_counts.show()

filled_df=raw_df.fillna('Invalid',subset=['transaction_id','merchant_id','transaction_currency','product_name','status','region','device_type'])\
    .fillna(0,subset=['transaction_amount'])\
    .fillna('N/A',subset=['customer_id','transaction_date','payment_method','timestamp','ip_address','region','product_category'])
display(filled_df)


# COMMAND ----------

null_counts = filled_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in raw_df.columns])
null_counts.show()

# COMMAND ----------

#Checking the malformed records in the dataset
#As we can see we have 123456 as merchant_id in the dataset but we need to have like 'MERCH-123456'
#So this is one of the malformed record

valid_currency = ["USD", "EUR", "AUD","GBP", "INR", "CAD"]

merchant_trim_df=filled_df.withColumn('merchant_id',trim(col('merchant_id')))

status_df = filled_df.withColumn(
    "merchant_id_valid",
    when(
        col("merchant_id").rlike("^MERCH-[0-9]+$"),
        True
    ).otherwise(False)
)\
.withColumn('customer_id',trim(col('customer_id')))\
.withColumn('customer_id_valid',when(col('customer_id').rlike("^[0-9]+$"),True).otherwise(False))\
    .withColumn(
    "payment_method",
    trim(lower(col("payment_method")))
)\
    .withColumn(
    "payment_method",
    when(col("payment_method") == "credit card", "Credit Card")
    .when(col("payment_method") == "cc", "Credit Card")
    .otherwise(initcap(col("payment_method")))
)\
.withColumn(
    "currency_status",
    when(col("transaction_currency").isNull(), "MISSING")
    .when(~col("transaction_currency").isin(valid_currency), "INVALID")
    .otherwise("VALID")
)
display(status_df)

# COMMAND ----------

df=status_df.select(col('customer_id')).filter(col('customer_id_valid')=="False").show()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Logging the Input records
# MAGIC

# COMMAND ----------

print("------------- Data Ingestion Details -------------")
print("Total Records",raw_df.count())
print("Total Columns",len(raw_df.columns))

print("Column Names :")
print(raw_df.columns)

print("Schema:")
raw_df.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC select count(*)
# MAGIC from de1.bronze.raw_df
# MAGIC where transaction_id is null

# COMMAND ----------

# MAGIC %md
# MAGIC ### Task-2
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 1
# MAGIC #### - Verify all required columns are present
# MAGIC #### - Flag missing columns as INVALID
# MAGIC

# COMMAND ----------

required_columns=['transaction_id','customer_id','merchant_id','transaction_amount','transaction_currency','transaction_date','payment_method','product_category','product_name','status','region','timestamp','ip_address','device_type']

dataset_columns=[c for c in raw_df.columns if c not in raw_df.columns]

rule1_df=status_df.withColumn('rule1_status'
                              ,when(lit(len(dataset_columns))==0,"Valid").otherwise("Invalis")
                              )
display(rule1_df)

# COMMAND ----------

sample_df=rule1_df.select(col('rule1_status')).filter(col('rule1_status')=='Invalid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Rule-2
# MAGIC #### - `transaction_id` must be unique and non-null
# MAGIC #### - `customer_id` must be non-null
# MAGIC #### - Flag nulls or duplicates as INVALID
# MAGIC

# COMMAND ----------

duplicate_records=(
    rule1_df.groupBy('transaction_id').count().filter(col("count")>1)
)

duplicates_list=[row['transaction_id']
                 for row in duplicate_records.collect()]

# COMMAND ----------

rule2_df=rule1_df.withColumn('rule2_status',
                             when(
                                 col('transaction_id').isNull()|
                                 col('customer_id').isNull()|
                                 col('transaction_id').isin(duplicates_list),"Invalid"
                             ).otherwise("Valid")
                             )
display(rule2_df)

# COMMAND ----------

rule2_df.select('transaction_id','customer_id','rule2_status').filter(col('rule2_status')=='Invalid').show()

# COMMAND ----------

rule2_df.select('transaction_id','customer_id','rule2_status').filter(col('rule2_status')=='Valid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule-3 : Domain Value Validation
# MAGIC #### - `payment_method` must be one of: [Credit Card, Debit Card, Wallet, Bank Transfer, Cash]
# MAGIC #### - `status` must be one of: [Completed, Failed, Pending, Refunded]
# MAGIC #### - `transaction_currency` must be valid ISO code
# MAGIC #### - Flag invalid values as INVALID
# MAGIC

# COMMAND ----------

payment_options=["Credit Card", "Debit Card", "Wallet", "Bank Transfer", "Cash"]

status_options=["Completed", "Failed", "Pending", "Refunded"]

currency_options=["USD" ,"INR","EUR","GBP" ,"JPY" ,"AUD" ,"CAD"]

rule3_df=rule2_df.withColumn('rule3_status',
                             when(
                                col('payment_method').isin(payment_options) &
                                col('status').isin(status_options) &
                                col('transaction_currency').isin(currency_options),
                                "Valid"
                            ).otherwise("Invalid")
                             )
display(rule3_df)




# COMMAND ----------

rule3_df.select("payment_method","transaction_currency","status",'rule3_status').filter(col('rule3_status')=='Invalid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule-4 : Referential Integrity
# MAGIC #### - `merchant_id` must follow format: "MERCH-" + 6 digits
# MAGIC #### - `customer_id` must be positive integer between 1 and 999,999
# MAGIC #### - Flag malformed IDs as INVALID
# MAGIC

# COMMAND ----------

rule4_df=rule3_df.withColumn('rule4_status',
                            when(
                            col('merchant_id').rlike ("^MERCH-[0-9]{6}$") &
                            col('customer_id').try_cast('int').between(1,999999),"Valid"
                            ).otherwise("Invalid")
) 
display(rule4_df)

# COMMAND ----------

rule4_df.select("merchant_id",'customer_id','rule4_status').filter(col('rule4_status')=='Invalid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule-5:Numeric Range Validation
# MAGIC #### - `transaction_amount` must be > 0 and < 1,000,000
# MAGIC #### - Flag out-of-range as INVALID
# MAGIC

# COMMAND ----------

rule5_df=rule4_df.withColumn('rule5_status',
                            when(
                            col('transaction_amount').cast('int').between(1,999999),"Valid"
                            ).otherwise("Invalid")
) 
display(rule5_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 6: Timestamp Validation
# MAGIC #### - `transaction_date` must be valid date (YYYY-MM-DD format)
# MAGIC #### - Date must be >= 2020-01-01 and <= today
# MAGIC #### - Flag invalid/future dates as INVALID
# MAGIC

# COMMAND ----------

rule6_df=rule5_df.withColumn('rule6_status',
                when(
                    col('transaction_date').between('2020-01-01','2026-08-21'),'valid'
                ).otherwise('Invalid')
                )
display(rule6_df)

# COMMAND ----------

rule6_df.select('transaction_date','rule6_status').filter(col('rule6_status')=='Invalid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 7: Logical Consistency
# MAGIC #### - If status = "Refunded", amount should be negative OR flag as INVALID
# MAGIC #### - If status = "Completed", amount should be positive
# MAGIC #### - Flag inconsistent logic as INVALID
# MAGIC

# COMMAND ----------

rule7_df=rule6_df.withColumn('rule7_status',
                when(
                    (col('status')=='Refunded') & (col('transaction_amount')<0) |
                    (col('status')=='Completed') & (col('transaction_amount')>0),
                    'Valid'
                ).otherwise('Invalid')
                )
display(rule7_df)

# COMMAND ----------

rule7_df.select('status','transaction_amount','rule7_status').filter(col('rule7_status')=='Invalid').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 8 : Outlier Detection
# MAGIC #### -Flag Transactions with amount > 3 standard deviations from mean as Anomaly
# MAGIC #### -These are valid records but Unusual 

# COMMAND ----------

from pyspark.sql.functions import stddev as spark_stddev
# Calculate mean and standard deviation for transaction_amount
values = rule7_df.select(
    avg('transaction_amount').alias('average_amount'),
    spark_stddev('transaction_amount').alias('std_amount')
).collect()[0]

mean = values['average_amount']
stddev = values['std_amount']

print(f"Mean transaction amount: {mean}")
print(f"Standard deviation: {stddev}")

# COMMAND ----------

rule8_df=rule7_df.withColumn('rule8_status',
                             when(
                                abs(col('transaction_amount')-mean) > (3*stddev),"Anomaly"
                             ).otherwise('Normal')
                             )
display(rule8_df)

# COMMAND ----------

rule8_df.select('transaction_amount','rule8_status').filter(col('rule8_status')=='Anomaly').show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule 9 : Completeness Check
# MAGIC #### - Calculate % of null values per column
# MAGIC #### Create a new colum like status and we will be assigning values as ok for values who have <20% and investigate which has >20%
# MAGIC #### - Log as informational (not blocking)
# MAGIC

# COMMAND ----------

total_records=raw_df.count()
for i in raw_df.columns:
    null_count=raw_df.filter(col(i).isNull()).count()
    null_percentage=(null_count/total_records)*100
    if null_percentage > 20:
        print(i,'->',null_percentage,"%Null's -> Investigation Requied")
    else:
        print(i,'->',null_percentage,"%Null's -> Accepted")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Rule: 10: Freshness Check
# MAGIC #### - Flag data older than 90 days as ANOMALY
# MAGIC #### - Ensure not too much stale data is present
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import current_date, datediff

rule10_df=rule8_df.withColumn('rule10_status',
                              when(
                                 try_to_date(col('transaction_date')) < date_sub(current_date(),90),"Anomaly"
                                  ).otherwise('Normal')
                              )
display(rule10_df)


# COMMAND ----------

rule10_df.select('transaction_date','rule10_status').filter(col('rule10_status')=="Anomaly").show()

# COMMAND ----------

tasked_df=rule10_df.withColumn('rule6_status',initcap('rule6_status'))
display(tasked_df)

# COMMAND ----------

valid_df = tasked_df.filter(
    (col("rule2_status") == "Valid") &
    (col("rule3_status") == "Valid") &
    (col("rule4_status") == "Valid") &
    (col("rule5_status") == "Valid") &
    (col("rule6_status") == "Valid") &
    (col("rule7_status") == "Valid")
)

display(valid_df)

# COMMAND ----------

total_records=raw_df.count()
valid_records=valid_df.count()
quality_records=(valid_records/total_records)* 100

print('total_records: ',total_records)
print("valid_records: ",valid_records)
print("quality_records: ",quality_records)

# COMMAND ----------

# MAGIC %sql
# MAGIC create schema if not exists de1.silver

# COMMAND ----------

# %sql
# -- drop table if exists de1.silver.taksed_df
# -- drop table if exists de1.silver.valid_df

# COMMAND ----------

valid_df.write.format('delta').saveAsTable('de1.silver.valid_df')

# COMMAND ----------

spark.read.table('de1.silver.tasked_df').display()

# COMMAND ----------

spark.read.table('de1.silver.valid_df').display()

# COMMAND ----------

