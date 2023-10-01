# Databricks notebook source
# MAGIC %sql
# MAGIC use silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER.POLICY DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver.policy (
# MAGIC policy_id int, policy_type string,  customer_id int, start_date timestamp, end_date timestamp, premium double, coverage_amount double, merged_timestamp timestamp
# MAGIC
# MAGIC ) using delta location "/mnt/silver/policy"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.policy

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL THE ROWS WHERE CUSTOMER_ID, POLICY_ID IS NULL

# COMMAND ----------

df = spark.sql("select * from bronze.policy where customer_id is not null and policy_id is not null and merge_flag = false ")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL ROWS WHERE CUSTOMER ID NOT EXISTS IN CUSTOMER TABLE

# COMMAND ----------

df = spark.sql("select P.* from bronze.policy as P inner join bronze.customer as C on P.customer_id = C.customer_id where P.customer_id is not null and P.policy_id is not null and P.merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC EVERY POLICY MUST HAVE PREMIUM AND COVERAGE AMOUNT > 0

# COMMAND ----------

df = spark.sql("select P.* from bronze.policy as P inner join bronze.customer as C on P.customer_id = C.customer_id where P.customer_id is not null and P.policy_id is not null and P.premium >0 and P.coverage_amount >0 and P.merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC VALIDATE END DATE > START DATE

# COMMAND ----------

df_final = spark.sql("select P.* from bronze.policy as P inner join bronze.customer as C on P.customer_id = C.customer_id where P.customer_id is not null and P.policy_id is not null and P.premium >0 and P.coverage_amount >0 and P.merge_flag = false and P.end_date > P.start_date")
display(df_final)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE THE TABLE WITH MERGED_TIMESTAMP AS CURRENT_TIMESTAMP

# COMMAND ----------

df_final.createOrReplaceTempView('clean_policy')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("merge into silver.policy AS A USING clean_policy as B on B.policy_id = A.policy_id when matched then update set A.policy_type = B.policy_type, A.customer_id = B.customer_id, A.start_date = B.start_date, A.end_date = B.end_date,  A.premium = B.premium, A.coverage_amount = B.coverage_amount, A.merged_timestamp = current_timestamp()  when not matched then insert (policy_id, policy_type, customer_id, start_date, end_date, premium, coverage_amount, merged_timestamp )  values(B.policy_id, B.policy_type, B.customer_id, B.start_date, B.end_date, B.premium, B.coverage_amount, current_timestamp() )")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.policy

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC UPDATE MERGE_FLAG = TRUE IN BRONGE.CUSTOMER

# COMMAND ----------

spark.sql("update bronze.policy set merge_flag = True where merge_flag = false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.policy

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC --------------------- CONGRATULATIONS!! YOU ARE COMPLETED CREATING SILVER.POLICY USING BRONZE.POLICY -------------------