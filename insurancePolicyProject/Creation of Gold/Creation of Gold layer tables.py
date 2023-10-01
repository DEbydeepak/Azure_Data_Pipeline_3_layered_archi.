# Databricks notebook source
# MAGIC %sql
# MAGIC use gold

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING OF GOLD CONTAINER

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://gold@missionadestgacc468.blob.core.windows.net', 
                 mount_point= '/mnt/gold', extra_configs ={'fs.azure.sas.gold.missionadestgacc468.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-05T10:45:40Z&st=2023-09-26T02:45:40Z&spr=https&sig=slcQVJwm11vgxOA%2F%2Bxl41HFzgLPaH%2FlSMtBmotx8vNU%3D'})



# COMMAND ----------

# MAGIC %fs
# MAGIC ls /mnt/gold

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF TABLES

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC TOTAL SALES BASED ON POLICY TYPE AND MONTH

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.sales_by_policy_type_and_month (
# MAGIC   policy_type string, 
# MAGIC   sale_month string,
# MAGIC   total_premium integer,
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location "/mnt/gold/sales_by_policy_type_and_month"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CLAIMS BY POLICY TYPE AND STATUS: This Table would contain the number and amount of claims by POLICY TYPE AND CLAIM STATUS. It would be used to monitor claim process and identify trends on it.

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.claims_by_policy_type_and_status (
# MAGIC
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   total_claims int,
# MAGIC   total_claim_amount int, 
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location "/mnt/gold/claims_by_policy_type_and_status"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyse CLAIM data based on policy_type, claim_status, total_claims, Avg, Max, Min, Count

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.claims_analysis (
# MAGIC   policy_type string,
# MAGIC   claim_status string,
# MAGIC   avg_claim_amount int,
# MAGIC   max_claim_amount int,
# MAGIC   min_claim_amount int,
# MAGIC   total_claims int,
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location "/mnt/gold/claims_analysis"

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Analyse REGION WISE CLAIMS data

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace table gold.region_wise_sales (
# MAGIC
# MAGIC   country string,
# MAGIC   city string,
# MAGIC   policy_type string,
# MAGIC   total_claims int,
# MAGIC   total_claim_amount int, 
# MAGIC   updated_timestamp timestamp
# MAGIC ) using delta location "/mnt/gold/region_wise_sales"