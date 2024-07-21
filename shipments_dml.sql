--===================================================================
-- Data for shipments.customers
--===================================================================
insert into shipments.customers (customer_id, customer_name)
select 1 as customer_id, "customer1" as customer_name union all
select 2 as customer_id, "customer2" as customer_name union all
select 3 as customer_id, "customer3" as customer_name union all
select 4 as customer_id, "customer4" as customer_name union all
select 5 as customer_id, "customer5" as customer_name;

--===================================================================
-- Data for shipments.loggers
--===================================================================
insert into shipments.loggers (logger_id, logger_name)
select 1 as logger_id, "logger1" as logger_name union all
select 2 as logger_id, "logger2" as logger_name union all
select 3 as logger_id, "logger3" as logger_name union all
select 4 as logger_id, "logger4" as logger_name union all
select 5 as logger_id, "logger5" as logger_name;

--===================================================================
-- Data for shipments.shipments
--===================================================================
insert into shipments.shipments (customer_id, logger_id, status, start_date, end_date)
select sc.customer_id, sl.logger_id, "READY" as status, (current_timestamp - INTERVAL '10' DAY) as start_date, (current_timestamp - INTERVAL '9' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 1 and sl.logger_id = 1 union all
select sc.customer_id, sl.logger_id, "READY" as status, (current_timestamp - INTERVAL '10' DAY) as start_date, (current_timestamp - INTERVAL '9' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 2 and sl.logger_id = 2 union all
select sc.customer_id, sl.logger_id, "READY" as status, (current_timestamp - INTERVAL '10' DAY) as start_date, (current_timestamp - INTERVAL '9' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 3 and sl.logger_id = 3 union all
select sc.customer_id, sl.logger_id, "READY" as status, (current_timestamp - INTERVAL '10' DAY) as start_date, (current_timestamp - INTERVAL '9' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 4 and sl.logger_id = 4 union all
select sc.customer_id, sl.logger_id, "READY" as status, (current_timestamp - INTERVAL '10' DAY) as start_date, (current_timestamp - INTERVAL '9' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 5 and sl.logger_id = 5;

insert into shipments.shipments (customer_id, logger_id, status, start_date, end_date)
select sc.customer_id, sl.logger_id, "SHIPPING" as status, (current_timestamp - INTERVAL '9' DAY) as start_date, (current_timestamp - INTERVAL '8' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 1 and sl.logger_id = 1 union all
select sc.customer_id, sl.logger_id, "SHIPPING" as status, (current_timestamp - INTERVAL '9' DAY) as start_date, (current_timestamp - INTERVAL '8' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 2 and sl.logger_id = 2 union all
select sc.customer_id, sl.logger_id, "SHIPPING" as status, (current_timestamp - INTERVAL '9' DAY) as start_date, (current_timestamp - INTERVAL '8' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 3 and sl.logger_id = 3 union all
select sc.customer_id, sl.logger_id, "SHIPPING" as status, (current_timestamp - INTERVAL '9' DAY) as start_date, (current_timestamp - INTERVAL '8' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 4 and sl.logger_id = 4 union all
select sc.customer_id, sl.logger_id, "SHIPPING" as status, (current_timestamp - INTERVAL '9' DAY) as start_date, (current_timestamp - INTERVAL '8' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 5 and sl.logger_id = 5;

insert into shipments.shipments (customer_id, logger_id, status, start_date, end_date)
select sc.customer_id, sl.logger_id, "DELIVERED" as status, (current_timestamp - INTERVAL '8' DAY) as start_date, (current_timestamp - INTERVAL '7' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 1 and sl.logger_id = 1 union all
select sc.customer_id, sl.logger_id, "DELIVERED" as status, (current_timestamp - INTERVAL '8' DAY) as start_date, (current_timestamp - INTERVAL '7' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 2 and sl.logger_id = 2 union all
select sc.customer_id, sl.logger_id, "DELIVERED" as status, (current_timestamp - INTERVAL '8' DAY) as start_date, (current_timestamp - INTERVAL '7' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 3 and sl.logger_id = 3 union all
select sc.customer_id, sl.logger_id, "DELIVERED" as status, (current_timestamp - INTERVAL '8' DAY) as start_date, (current_timestamp - INTERVAL '7' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 4 and sl.logger_id = 4 union all
select sc.customer_id, sl.logger_id, "DELIVERED" as status, (current_timestamp - INTERVAL '8' DAY) as start_date, (current_timestamp - INTERVAL '7' DAY)  as end_date
from shipments.customers sc cross join shipments.loggers sl where sc.customer_id = 5 and sl.logger_id = 5;

--===================================================================
-- Data for shipments.logger_temperature is generated in a notebook
--===================================================================
hours = range(0, 25)

for i in hours:
    query = f"""
    INSERT INTO shipments.logger_temperature 
    SELECT logger_id, dateadd(HOUR, {i}, start_date) as time, (rand()*100) as value 
    FROM shipments.shipments 
    WHERE 1=1
    """
    spark.sql(query)
spark.sql("OPTIMIZE shipments.logger_temperature")
spark.sql("VACUUM shipments.logger_temperature")

--===================================================================
--===================================================================
