# Databricks notebook source
# Importing required libraries
import logging
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("shipment_analysis").getOrCreate()
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# COMMAND ----------

# Defining widgets
dbutils.widgets.text("process_name", "shipments_analysis")
dbutils.widgets.text("load_type", "INCREMENTAL")
dbutils.widgets.text("truncate_flag", "N")
dbutils.widgets.text("table_schema", "shipments")
dbutils.widgets.text("table_name", "shipments_analysis")

# Getting parameter values
process_name = dbutils.widgets.get("process_name").upper()
load_type = dbutils.widgets.get("load_type").upper()
truncate_flag = dbutils.widgets.get("truncate_flag").upper()
table_schema = dbutils.widgets.get("table_schema")
table_name = dbutils.widgets.get("table_name")
logger.info(f"The process {process_name} is started")
logger.info(f"Input parameters: load_type={load_type}, truncate_flag={truncate_flag}, table_schema={table_schema}, table_name={table_name}")

# COMMAND ----------

# Truncating the target table if required
if truncate_flag == "Y" and load_type == "INCREMETAL":
    logging.error(f"Truncating a table {table_schema}.{table_name} in INCREMETAL load_type is not allowed")
    raise AttributeError(f"Truncating a table {table_schema}.{table_name} in INCREMETAL load is not allowed")
elif truncate_flag == "Y" and load_type == "FULL":
    truncate_query = f"truncate table {table_schema}.{table_name}"
    logging.info(f"Code to execute: {truncate_query}")
    logging.info(f"Truncating the table {table_schema}.{table_name} is started")
    spark.sql(truncate_query)
    logging.info(f"Truncating the table {table_schema}.{table_name} is finished")

# COMMAND ----------

# Loading data into the target table
logging.info(f"Generating query for merge statement is started")
if load_type == "FULL":
    where_sql = "1=1"    
else:
    where_sql = "to_date(lt.time) >= to_date(dateadd(DAY, -1, current_timestamp)) and to_date(lt.time) < to_date(current_timestamp)"
logging.info(f"Where statement: {where_sql}")

merge_query = f"""
        merge into {table_schema}.{table_name} trg 
        using (select to_date(lt.time) as report_date,
                      sh.customer_id as customer_id,
                      cs.customer_name as customer_name, 
                      sh.status as status,
                      min(lt.time) as status_start_date, 
                      max(lt.time) as status_end_date, 
                      round(min(lt.value),2) as min_temperature, 
                      round(max(lt.value),2) as max_temperature
               from {table_schema}.shipments sh 
               left join {table_schema}.logger_temperature lt on sh.logger_id = lt.logger_id and sh.start_date <= lt.time and sh.end_date >= lt.time
               left join {table_schema}.loggers lg on sh.logger_id = lg.logger_id
               left join {table_schema}.customers cs on sh.customer_id = cs.customer_id
               where {where_sql}
               group by sh.customer_id, cs.customer_name, sh.status, to_date(lt.time)) src
        on (trg.report_date = src.report_date and trg.customer_id = src.customer_id and trg.status = src.status)
        when matched then update set trg.customer_name = src.customer_name,
                                     trg.status_start_date = src.status_start_date,
                                     trg.status_end_date = src.status_end_date, 
                                     trg.min_temperature = src.min_temperature, 
                                     trg.max_temperature = src.max_temperature
        when not matched then insert (report_date, 
                                      customer_id, 
                                      customer_name, 
                                      status, 
                                      status_start_date, 
                                      status_end_date, 
                                      min_temperature, 
                                      max_temperature)
                              values (src.report_date, 
                                      src.customer_id, 
                                      src.customer_name, 
                                      src.status, 
                                      src.status_start_date, 
                                      src.status_end_date, 
                                      src.min_temperature, 
                                      src.max_temperature)
        """
logging.info(f"Generating query for merge statement is finished")
logging.info(f"Code to execute: {merge_query}")

# COMMAND ----------

logging.info(f"Merging data into the table {table_schema}.{table_name} is started")
result_df = spark.sql(merge_query)
logging.info(f"Merging data into the table {table_schema}.{table_name} is finished")
logging.info(f"Statisticts: updated rows-{result_df.first()['num_updated_rows']}, inserted rows-{result_df.first()['num_inserted_rows']}")
logger.info(f"The process {process_name} is finished")
