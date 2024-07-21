# Databricks notebook source
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate("shipment_analysis")

# COMMAND ----------

# Reading shipment and logger information
shipments_df = spark.table("shipments.shipments")
logger_temperature_df = spark.table("shipments.logger_temperature")

# COMMAND ----------


