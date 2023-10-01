# Databricks notebook source
# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create database bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC use bronze

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING OF LANDING CONTAINER

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://landing@missionadestgacc468.blob.core.windows.net', 
                 mount_point= '/mnt/landing', extra_configs ={'fs.azure.sas.landing.missionadestgacc468.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&se=2023-10-05T10:45:40Z&st=2023-09-26T02:45:40Z&spr=https&sig=slcQVJwm11vgxOA%2F%2Bxl41HFzgLPaH%2FlSMtBmotx8vNU%3D'})



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING OF PROCESSED CONTAINER

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://processed@missionadestgacc468.blob.core.windows.net', 
                 mount_point= '/mnt/processed', extra_configs ={'fs.azure.sas.processed.missionadestgacc468.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&se=2023-10-05T10:45:40Z&st=2023-09-26T02:45:40Z&spr=https&sig=slcQVJwm11vgxOA%2F%2Bxl41HFzgLPaH%2FlSMtBmotx8vNU%3D'})



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING OF BRONZE CONTAINER

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://bronze@missionadestgacc468.blob.core.windows.net', 
                 mount_point= '/mnt/bronze', extra_configs ={'fs.azure.sas.bronze.missionadestgacc468.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&se=2023-10-05T10:45:40Z&st=2023-09-26T02:45:40Z&spr=https&sig=slcQVJwm11vgxOA%2F%2Bxl41HFzgLPaH%2FlSMtBmotx8vNU%3D'})



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING AGENT DATA FROM LANDING AND WRITING INTO BRONZE CONTAINER WITH MERGE_FLAG FALSE

# COMMAND ----------

from pyspark.sql.functions import lit
agent_schema = ["agent_id integer, agent_name string, agent_email string, agent_phone integer, branch_id string, create_timestamp timestamp"]
df = spark.read.parquet('/mnt/landing/agent/*.parquet', schema = agent_schema)
df_with_flag = df.withColumn('merge_flag', lit(False))
df_with_flag.write.option('path', '/mnt/bronze/agent').mode('append').saveAsTable('bronze.agent')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.agent

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOVING LANDING/AGENT FOLDER TO PROCESSED/AGENT FOLDER

# COMMAND ----------

from datetime import datetime
current_date = datetime.now().strftime('%m-%d-%y')
print(current_date)

new_folder = '/mnt/processed/agent/'+current_date
print(new_folder)

# COMMAND ----------

dbutils.fs.mv('/mnt/landing/agent', '/mnt/processed/agent/'+current_date, True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING BRANCH DATA FROM LANDING AND WRITING INTO BRONZE CONTAINER WITH MERGE_FLAG FALSE

# COMMAND ----------

from pyspark.sql.functions import lit
branch_schema = "branch_id integer, branch_country string, branch_city string"
df = spark.read.parquet('/mnt/landing/branch/*.parquet', schema = branch_schema)
df_with_flag = df.withColumn('merge_flag', lit(False))
df_with_flag.write.option('path', '/mnt/bronze/branch').mode('append').saveAsTable('bronze.branch')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.branch

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOVING LANDING/BRANCH FOLDER TO PROCESSED/BRANCH FOLDER

# COMMAND ----------

from datetime import datetime
current_date = datetime.now().strftime('%m-%d-%y')
print(current_date)

new_folder = '/mnt/processed/branch/'+current_date
print(new_folder)


# COMMAND ----------

dbutils.fs.mv('/mnt/landing/branch', '/mnt/processed/branch/'+current_date, True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING CLAIM DATA FROM LANDING AND WRITING INTO BRONZE CONTAINER WITH MERGE_FLAG FALSE

# COMMAND ----------

from pyspark.sql.functions import lit
claim_schema = ["claim_id int, policy_id int, date_of_claim timestamp, claim_amount double, claim_status string, LastUpdatedTimeStamp timestamp"]
df = spark.read.parquet('/mnt/landing/claim/*.parquet', schema = claim_schema)
df_with_flag = df.withColumn('merge_flag', lit(False))
df_with_flag.write.option('path', '/mnt/bronze/claim').mode('append').saveAsTable('bronze.claim')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.claim

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOVING LANDING/CLAIM FOLDER TO PROCESSED/CLAIM FOLDER

# COMMAND ----------

from datetime import datetime
def getfilePathwithDates(filepath):
    current_date = datetime.now().strftime('%m-%d-%y')
    new_folder = filepath +'/'+ current_date
    return new_folder
getfilePathwithDates('/mnt/landing/temp')

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/claim", getfilePathwithDates("/mnt/processed/claim"), True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING CUSTOMER DATA FROM LANDING AND WRITING INTO BRONZE CONTAINER WITH MERGE_FLAG FALSE

# COMMAND ----------

from pyspark.sql.functions import lit
customer_schema = "customer_id int,first_name string,last_name string,email string,phone string,country string,city string,registration_date timestamp,date_of_birth timestamp,gender string"
df = spark.read.csv('/mnt/landing/customer/*.csv', schema = customer_schema, header = True)
df_merge_flag = df.withColumn('merge_flag', lit(False))
df_merge_flag.write.option('path', '/mnt/bronze/customer').mode('append').saveAsTable('bronze.customer')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOVING LANDING/CUSTOMER FOLDER TO PROCESSED/CUSTOMER FOLDER

# COMMAND ----------

from datetime import datetime
def getfilePathwithDates(filepath):
    current_date = datetime.now().strftime('%m-%d-%y')
    new_folder = filepath +'/'+ current_date
    return new_folder


# COMMAND ----------

getfilePathwithDates('/mnt/landing/temp')

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/customer", getfilePathwithDates("/mnt/processed/customer"), True)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC READING POLICY DATA FROM LANDING AND WRITING INTO BRONZE CONTAINER WITH MERGE_FLAG FALSE

# COMMAND ----------

from pyspark.sql.functions import lit
policy_schema = "policy_id int, policy_type string, customer_id int, start_date timestamp, end_date timestamp, premium double, coverage_amount double"
df = spark.read.json('/mnt/landing/policy/*.json', schema = policy_schema)
df_merge_flag = df.withColumn('merge_flag', lit(False))
df_merge_flag.write.option('path', '/mnt/bronze/policy').mode('append').saveAsTable('bronze.policy')



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.policy

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOVING LANDING/POLICY FOLDER TO PROCESSED/POLICY FOLDER

# COMMAND ----------

from datetime import datetime
def getfilePathwithDates(filepath):
    current_date = datetime.now().strftime('%m-%d-%y')
    new_folder = filepath +'/'+ current_date
    return new_folder


# COMMAND ----------

getfilePathwithDates('/mnt/landing/temp')

# COMMAND ----------

dbutils.fs.mv("/mnt/landing/policy", getfilePathwithDates("/mnt/processed/policy"), True)

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/processed/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/landing/

# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/bronze/

# COMMAND ----------

