# Databricks notebook source
# MAGIC %sql
# MAGIC use gold

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC Create a View table claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_gold_claims_by_policy_type_and_status 
# MAGIC as
# MAGIC select policy_type, claim_status, count(*) as total_claims, sum(claim_amount) as total_claim_amount
# MAGIC from silver.policy as P inner join silver.claim as C 
# MAGIC on P.policy_id = C.policy_id
# MAGIC group by policy_type, claim_status
# MAGIC having policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_gold_claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGE into gold.claims_by_policy_type_and_status using View

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into gold.claims_by_policy_type_and_status as A using vw_gold_claims_by_policy_type_and_status as B on A.policy_type = B.policy_type and A.claim_status = B.claim_status when matched then update set A.total_claims = B.total_claims, A.total_claim_amount = B.total_claim_amount, A.updated_timestamp = current_timestamp() when not matched then insert (policy_type, claim_status, total_claims, total_claim_amount, updated_timestamp) values (B.policy_type, B.claim_status, B.total_claims, B.total_claim_amount, current_timestamp() )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.claims_by_policy_type_and_status

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATE A VIEW TABLE vw_gold_claims_analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC create or replace temp view vw_gold_claims_analysis 
# MAGIC as 
# MAGIC select policy_type, avg(claim_amount) as avg_claim_amount, max(claim_amount) as max_claim_amount, min(claim_amount) as min_claim_amount,  
# MAGIC count(distinct claim_id) as total_claims 
# MAGIC from silver.policy as P inner join silver.claim as C 
# MAGIC on P.policy_id = P.policy_id 
# MAGIC group by policy_type 
# MAGIC having policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_gold_claims_analysis

# COMMAND ----------

# MAGIC %md
# MAGIC <B>
# MAGIC MERGING into gold.claims_analysis using Temp View

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC merge into gold.claims_analysis as A using vw_gold_claims_analysis as B 
# MAGIC on A.policy_type = B.policy_type  
# MAGIC when matched then update set  
# MAGIC A.avg_claim_amount = B.avg_claim_amount, A.max_claim_amount = B.max_claim_amount, A.min_claim_amount = B.min_claim_amount, A.total_claims = B.total_claims, 
# MAGIC A.updated_timestamp = current_timestamp()
# MAGIC when not matched then insert 
# MAGIC (policy_type, avg_claim_amount, max_claim_amount, min_claim_amount, total_claims, updated_timestamp) 
# MAGIC values (B.policy_type, B.avg_claim_amount, B.max_claim_amount, B.min_claim_amount, B.total_claims, current_timestamp() )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.claims_analysis

# COMMAND ----------

# MAGIC %md 
# MAGIC <b>
# MAGIC Create View of REGION WISE SALES

# COMMAND ----------

# MAGIC %sql
# MAGIC create or replace temp view vw_gold_region_wise_sales 
# MAGIC as
# MAGIC select 
# MAGIC country,
# MAGIC city ,
# MAGIC policy_type,
# MAGIC count(*) as total_claims,
# MAGIC sum(C.claim_amount) as total_claim_amount 
# MAGIC from silver.customer as Cu inner join silver.policy as P
# MAGIC on P.customer_id = Cu.customer_id
# MAGIC inner join silver.claim as C on P.policy_id = C.policy_id
# MAGIC group by Cu.country, Cu.city, P.policy_type
# MAGIC having policy_type is not null

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from vw_gold_region_wise_sales

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC MERGING into gold.region_wise_sales using Temp View

# COMMAND ----------

# MAGIC %sql
# MAGIC merge into gold.region_wise_sales as A using vw_gold_region_wise_sales as B 
# MAGIC on A.country = B.country and A.city = B.city and A.policy_type = B.policy_type
# MAGIC when matched then update set A.total_claims = B.total_claims, A.total_claim_amount = B.total_claim_amount, A.updated_timestamp = current_timestamp()
# MAGIC when not matched then insert (country, city, policy_type, total_claims, total_claim_amount, updated_timestamp)
# MAGIC values(B.country, B.city, B.policy_type, B.total_claims, B.total_claim_amount, current_timestamp() )

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold.region_wise_sales

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC -------------------------  CONGRATULATIONS!! YOU COMPLETED 3 LAYERED BRONZE, SILVER AND GOLD ARCHITECTURE WITH THIS STEP  ----------------------------------