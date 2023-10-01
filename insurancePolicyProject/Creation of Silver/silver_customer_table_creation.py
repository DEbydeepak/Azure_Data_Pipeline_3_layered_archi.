# Databricks notebook source
# MAGIC %sql 
# MAGIC use silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER.BRANCH DELTA TABLE

# COMMAND ----------

# MAGIC %sql
# MAGIC create table silver.customer(
# MAGIC   customer_id int,first_name string,last_name string,email string,phone string,country string,city string,registration_date timestamp,date_of_birth timestamp,gender string, merged_timestamp timestamp
# MAGIC ) using delta location "/mnt/silver/customer"

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC LOAD BRONZE.CUSTOMER DATA WHERE MERGE_FLAG = FALSE

# COMMAND ----------

df = spark.sql("select * from bronze.customer where merge_flag = false")
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL RECORDS WHERE CUSTOMER ID IS NOT NULL

# COMMAND ----------

df_customerid = spark.sql("select * from bronze.customer where customer_id is not null and merge_flag = false")
display(df_customerid)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE RECORDS WHERE GENDER IS OTHER THAN MALE/FEMALE

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_gender = df_customerid.filter(col('gender').isin('Male', 'Female'))
display(df_gender)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC REMOVE ALL RECORDS FOR WHICH REGESTRATION DATE > DATE OF BIRTH

# COMMAND ----------

df_date = df_gender.where(col('registration_date') > col('date_of_birth'))
display(df_date)

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE INTO SILVER CUSTOMER TABLE USING BRONZE CUSTOMER TABLE

# COMMAND ----------

df_date.createOrReplaceTempView('clean_customer')

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

spark.sql("merge into silver.customer as A using clean_customer as B on A.customer_id = B.customer_id when matched then update set A.first_name = B.first_name, A.last_name = B.last_name, A.email = B.email, A.phone = B.phone, A.country = B.country, A.city = B.city, A.registration_date = B.registration_date, A.date_of_birth = B.date_of_birth, A.gender = B.gender, A.merged_timestamp = current_timestamp() when not matched then insert (customer_id, first_name, last_name, email, phone,country,city, registration_date, date_of_birth, gender, merged_timestamp) values(B.customer_id, B.first_name, B.last_name, B.email, B.phone,B.country,B.city, B.registration_date, B.date_of_birth, B.gender, current_timestamp() ) ")
            

# COMMAND ----------

# MAGIC %sql select * from silver.customer

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC UPDATE MERGE_FLAG = TRUE IN BRONGE.CUSTOMER

# COMMAND ----------

spark.sql("update bronze.customer set merge_flag = True where merge_flag = false")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from bronze.customer

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC --------------------- CONGRATULATIONS!! YOU ARE COMPLETED CREATING SILVER.CUSTOMER USING BRONZE.CUSTOMER -------------------