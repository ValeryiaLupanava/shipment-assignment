# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate("shipment_analysis")

# COMMAND ----------

# Reading shipment and logger information
shipments_df = spark.table("shipments.shipments")
logger_temperature_df = spark.table("shipments.logger_temperature")

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id, min(start_date) as start_date, max(end_date) as end_date from shipments.shipments group by customer_id

# COMMAND ----------

# MAGIC %sql
# MAGIC select to_date(lt.time) as report_date,
# MAGIC        sh.customer_id, 
# MAGIC        sh.logger_id, 
# MAGIC        sh.status,
# MAGIC        min(sh.start_date) as status_start_date, 
# MAGIC        max(sh.end_date) as status_end_date, 
# MAGIC        round(min(lt.value),2) as min_temperature, 
# MAGIC        round(max(lt.value),2) as max_temperature
# MAGIC from shipments.shipments sh left join shipments.logger_temperature lt on sh.logger_id = lt.logger_id and sh.start_date <= lt.time and sh.end_date >= lt.time
# MAGIC group by sh.customer_id, sh.logger_id, sh.status, to_date(lt.time)
# MAGIC order by report_date, sh.customer_id, status_start_date;
