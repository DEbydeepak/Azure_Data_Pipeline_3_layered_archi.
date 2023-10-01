# Databricks notebook source
# MAGIC %sql
# MAGIC use silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER.CLAIM DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver.claim (
# MAGIC claim_id int, policy_id int, date_of_claim date, claim_amount double, claim_status string, LastUpdatedTimeStamp timestamp, merged_timestamp timestamp
# MAGIC
# MAGIC ) using delta location "/mnt/silver/claim"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.claim

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC LOADING BRONZE.CLAIM TABLE WHERE MERGE_FLAG = FLASE

# COMMAND ----------

df = spark.sql("select * from bronze.claim where merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL ROWS WHERE POLICY ID NOT EXISTS IN POLICY TABLE

# COMMAND ----------

df_policyid = spark.sql("select C.* from bronze.claim as C inner join bronze.policy as P on C.policy_id = P.policy_id where C.merge_flag = false ")
display(df_policyid)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL WHERE CLAIM ID, POLICY ID, CLAIM STATUS, CLAIM AMOUNT, LASTUPDATEDTIMESTAMP NULL

# COMMAND ----------

df_dropna = df_policyid.na.drop(subset = ['claim_id', 'policy_id', 'claim_status', 'claim_amount', 'LastUpdatedTimeStamp'])
display(df_dropna)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CONVERT DATE_OF_CLAIM TO DATE COLUMN WITH FORMAT (MM-dd-yyyy)

# COMMAND ----------

from pyspark.sql.functions import date_format, date_part, col, column

# COMMAND ----------

df_datecol = df_dropna.select('claim_id', 'policy_id','claim_amount', 'claim_status', 'LastUpdatedTimeStamp', (date_format(col('date_of_claim'), 'MM-dd-yyyy')).alias('date_of_claim'))
display(df_datecol)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC ENSURE CLAIM_AMOUNT > 0

# COMMAND ----------

df_claimamount = df_datecol.filter(col('claim_amount') > 0)
display(df_claimamount)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE INTO SILVER.CLAIM USING BRONZE.CLAIM

# COMMAND ----------

df.createOrReplaceTempView('clean_claim')

# COMMAND ----------

spark.sql("merge into silver.claim as A using clean_claim as B on A.claim_id = B.claim_id when matched then update set A.claim_id = B.claim_id, A.policy_id = B.policy_id, A.date_of_claim = B.date_of_claim, A.claim_amount = B.claim_amount, A.claim_status = B.claim_status, A.LastUpdatedTimeStamp = B.LastUpdatedTimeStamp, A.merged_timestamp = current_timestamp() when not matched then insert (claim_id, policy_id, date_of_claim, claim_amount, claim_status, LastUpdatedTimeStamp, merged_timestamp) values(B.claim_id,B. policy_id, B.date_of_claim, B.claim_amount, B.claim_status, B.LastUpdatedTimeStamp, current_timestamp() )")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.claim

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC UPDATE MERGE_FLAG = TRUE IN BRONGE.CLAIM

# COMMAND ----------

spark.sql('update bronze.claim set merge_flag = true where merge_flag = false')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from  bronze.claim

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC --------------------- CONGRATULATIONS!! YOU ARE COMPLETED CREATING SILVER.CLAIM USING BRONZE.CLAIM -------------------