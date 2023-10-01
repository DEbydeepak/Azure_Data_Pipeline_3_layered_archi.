# Insurance Policy Project
# Created 3 layered (Bronze/Silver/Gold) architecture using Deltalake Framework.
 
ABC is an insurance company who sells insurance policy to retail customers. 
Developed an end-to-end data engineering pipeline for an insurance company to analyze claims data and perform customer segmentation. 
This will help the company to better understand their customers' needs and tailor their offerings accordingly.

Services used: Azure Data Factory, Azure Datalake storage (ADLS), Azure Databricks, Azure SQL Server.

These are the source system:
Source: CSV Files, JSON Files, SQL Server DB
Here we have Agent, Branch, Claim, Customer and Policy files from different sources. 


Data Ingestion:
Ingesting all data from SQL server to ADLS Landing Container using Azure Data Factory (ADF) with Incremental Pipeline. 

Data Transformation:

Bronze layer:
Create Bronze container in ADLS and Bronze Database in Databricks.
Using SAS token, connected the landing container to access the tables in Databricks i.e Mounting
Then, all files get written into bronze layer container and also saved as Bronze table.

Silver layer:
Create Silver container in ADLS and Silver Database in Databricks.
Mount the Silver container using SAS token.
Load all tables from Bronze Database.
Do cleaning, Removing Nulls, Outliers, validation checks and other basic transformations will be done. 
Finally, we will Merge all Transformed data into Silver tables.

Gold layer:
Create Gold container in ADLS and Gold Database in Databricks.
Mount the Gold container using SAS token.
Load all tables from Silver Database for analysis.
It’s more on finding the insights like doing Aggregation, Joining and others to get trends.

Scheduling:
Finally, Scheduling the Data Factory and Databricks pipelines on regular basis assuming that Insurance policy data arrives daily in Source location.

Flow of the Project:
Source -----> Azure Datalake Storage ------> Bronze ------> Silver ------> Gold 
