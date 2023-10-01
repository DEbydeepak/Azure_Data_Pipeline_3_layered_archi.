# Databricks notebook source
# MAGIC %md
# MAGIC <b>
# MAGIC MOUNTING OF SILVER CONTAINER

# COMMAND ----------

dbutils.fs.mount( source = 'wasbs://silver@missionadestgacc468.blob.core.windows.net', 
                 mount_point= '/mnt/silver', extra_configs ={'fs.azure.sas.silver.missionadestgacc468.blob.core.windows.net':'?sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2023-10-05T10:45:40Z&st=2023-09-26T02:45:40Z&spr=https&sig=slcQVJwm11vgxOA%2F%2Bxl41HFzgLPaH%2FlSMtBmotx8vNU%3D'})

# COMMAND ----------

# MAGIC %md
# MAGIC <b>
# MAGIC CREATION OF SILVER DATABASE

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC create database silver

# COMMAND ----------

# MAGIC %sql
# MAGIC use silver

# COMMAND ----------

# MAGIC %sql
# MAGIC select current_database()

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC show tables