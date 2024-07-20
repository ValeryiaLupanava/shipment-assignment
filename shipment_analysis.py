# Databricks notebook source
from generic_functions import read_data_from_db

# DB connection
#secret_scope = "sc-shipments"
#db_connection_key = dbutils.secrets.get(scope=secret_scope, key=db_connection_key) 
db_connection_key = ""

# COMMAND ----------

# Reading shipment information
shipments_df = read_data_from_db(db_connection=db_connection_key, table_schema="dbo", table_name="shipments", filter="1=1")
shipments_df.display()

# COMMAND ----------

# Reading logger information
logger_df = read_data_from_db(db_connection=db_connection_key, table_schema="dbo", table_name="logger_temperature", filter="1=1")
logger_df.display()
