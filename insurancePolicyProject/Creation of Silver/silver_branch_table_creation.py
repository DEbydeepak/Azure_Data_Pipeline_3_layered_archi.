# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER.BRANCH DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.branch(
# MAGIC     branch_id int, 
# MAGIC     branch_country string,
# MAGIC     branch_city string,
# MAGIC     merged_timestamp timestamp
# MAGIC ) using delta location "/mnt/silver/branch"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL RECORDS WHERE BRANCH_ID IS NOT NULL

# COMMAND ----------

df = spark.sql("select * from bronze.branch where merge_flag = false")
display(df)

# COMMAND ----------

df_branchid = spark.sql("select * from bronze.branch where branch_id is not null and merge_flag = false")
display(df_branchid)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL LEADING AND TAILING SPACES IN BRANCH COUNTRY AND CONVERT IT INTO UPPER CASE

# COMMAND ----------

from pyspark.sql.functions import upper, trim, lower, 

# COMMAND ----------

df_branchcountry = spark.sql("select branch_id, branch_city, upper(trim(branch_country)) as branch_country \
     from bronze.branch where branch_id is not null and merge_flag = false")
display(df_branchcountry)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE INTO SILVER BRANCH TABLE

# COMMAND ----------

df_branchcountry.createOrReplaceTempView("clean_branch")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("merge into silver.branch as A USING clean_branch as B on B.branch_id = A.branch_id \
    when matched then update set A.branch_country = B.branch_country, A.branch_city = B.branch_city, A.merged_timestamp = current_timestamp() \
        when not matched then insert (branch_id, branch_country, branch_city, merged_timestamp) \
            values (B.branch_id, B.branch_country, B.branch_city, current_timestamp()) ")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC UPDATE MERGE_FLAG = TRUE IN BRONGE.BRANCH

# COMMAND ----------

spark.sql("update bronze.branch set merge_flag = true where merge_flag = false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.branch

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC --------------------- CONGRATULATIONS!! YOU ARE COMPLETED CREATING SILVER.BRANCH USING BRONZE.BRANCH -------------------