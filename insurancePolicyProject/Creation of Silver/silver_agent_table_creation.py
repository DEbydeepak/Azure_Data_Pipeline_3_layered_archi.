# Databricks notebook source
# MAGIC %sql
# MAGIC use silver

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER.AGENT DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table silver.agent(
# MAGIC   agent_id integer, agent_name string, agent_email string, agent_phone string, branch_id string, create_timestamp timestamp, merge_timestamp timestamp
# MAGIC )
# MAGIC using delta location "/mnt/silver/agent"
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.agent

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC SELECT AGENT DATA WHERE MERGE_FLAG = FALSE

# COMMAND ----------

df = spark.sql("select * from bronze.agent where merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL ROWS WHERE BRANCH ID NOT EXISTS IN BRANCH TABLE

# COMMAND ----------

df_branchid = spark.sql("select A.* from bronze.agent A \
          inner join bronze.branch B on A.branch_id = B.branch_id \
          where A.merge_flag = false")

# COMMAND ----------

display(df_branchid)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC ENSURE ALL PHONE_NOS HAVE VALID 10 DIGITS

# COMMAND ----------

from pyspark.sql.functions import col, length

# COMMAND ----------

df_phone = df_branchid.filter(length(col('agent_phone')) == 10)

# COMMAND ----------

display(df_phone)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CHECKING PHONE_NUM AND BRANCH_ID

# COMMAND ----------

df_res = spark.sql("select A.* from bronze.agent A inner join bronze.branch B on A.branch_id = B.branch_id \
    where A.merge_flag = false and length(A.agent_phone) = 10 ")
display(df_res)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REPLACE ALL NULL, EMPTY EMAIL WITH ADMIN@AZURELIB.COM

# COMMAND ----------

df_fillna = df_phone.fillna('admin@azurelib.com', subset='agent_email')

# COMMAND ----------

display(df_fillna)

# COMMAND ----------

from pyspark.sql.functions import regexp_replace

# COMMAND ----------

df_fillna.createOrReplaceTempView('agent_temp')

# COMMAND ----------

df_res.createOrReplaceTempView('agent_temp2')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from agent_temp where agent_email = ''

# COMMAND ----------

df_email = spark.sql("select agent_id, agent_name, agent_phone, branch_id, create_timestamp, regexp_replace(agent_email, '', 'admin@azurelib.com') as agent_email from agent_temp where agent_email = '' ")
display(df_email)

# COMMAND ----------

df_mail = spark.sql("select agent_id, agent_name, agent_phone, branch_id, create_timestamp, merge_flag, \
                     regexp_replace(agent_email, '', 'admin@azurelib.com') as agent_email from agent_temp where agent_email = '' UNION \
                        select agent_id, agent_name, agent_phone, branch_id, create_timestamp, merge_flag, \
                     agent_email from agent_temp where agent_email != '' ")
display(df_mail)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE_TIMESTAMP WITH CURRENT_TIMESTAMP IN SILVER.AGENT

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, current_date

# COMMAND ----------

df_result = df_mail.withColumn('merge_timestamp', current_timestamp())
display(df_result)

# COMMAND ----------

df_result.createOrReplaceTempView('clean_agent')

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGING ON SILVER.AGENT USING BRONGE.AGENT

# COMMAND ----------

spark.sql("merge into silver.agent as T using clean_agent S  on T.agent_id = S.agent_id \
    when matched then update set T.agent_name = S.agent_name, T.agent_phone = S.agent_phone, T.branch_id = S.branch_id, T.agent_email = S.agent_email, \
        T.create_timestamp = S.create_timestamp, T.merge_timestamp = current_timestamp() \
        when not matched then insert (agent_id, agent_name, agent_phone, branch_id, agent_email, create_timestamp, merge_timestamp) \
            values(S.agent_id, S.agent_name, S.agent_phone, S.branch_id, S.agent_email, S.create_timestamp, current_timestamp()) ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.agent

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC UPDATE MERGE_FLAG = TRUE IN BRONGE.AGENT

# COMMAND ----------

spark.sql("update bronze.agent set merge_flag = True where merge_flag = false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.agent

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC ---------------------- CONGRATULATIONS!!  YOU ARE COMPLETED CREATING SILVER.AGENT TABLE USING BRONZE.AGENT  ------------------