--===================================================================
-- Schema for shipments
--===================================================================
create schema valeria_lupanava.shipments;

--===================================================================
-- Table for customers
--===================================================================
create or replace table shipments.customers
(customer_id int NOT NULL,
 customer_name string,
 constraint pk_customer_id primary key (customer_id)
) using delta;

--===================================================================
-- Table for loggers
--===================================================================
create or replace table shipments.loggers
(logger_id int NOT NULL,
 logger_name string,
 constraint pk_logger_id primary key (logger_id)
) using delta;

--===================================================================
-- Table for shipments
--===================================================================
create or replace table shipments.shipments
(customer_id int NOT NULL,
 logger_id int NOT NULL,
 status string NOT NULL,
 start_date timestamp,
 end_date timestamp,
 constraint fk_shipments_customer_id foreign key (customer_id) REFERENCES shipments.customers(customer_id),
 constraint fk_shipments_logger_id foreign key (logger_id) REFERENCES shipments.loggers(logger_id)
) using delta;

--===================================================================
-- Table for logger_temperature
--===================================================================
create  or replace table shipments.logger_temperature
(logger_id int,
 time timestamp,
 value float
) using delta;
ALTER TABLE shipments.logger_temperature SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = '0 days');

--===================================================================
-- Table for shipment_analysis (dashboard)
--===================================================================
create or replace table shipments.shipments_analysis
(report_date date NOT NULL,
 customer_id int NOT NULL,
 customer_name string NOT NULL,
 status string,
 status_start_date timestamp NOT NULL,
 status_end_date timestamp,
 min_temperature float,
 max_temperature float,
 constraint fk_shipments_analysis_customer_id foreign key (customer_id) REFERENCES shipments.customers(customer_id)
) using delta;
ALTER TABLE shipments.shipments_analysis CLUSTER BY (report_date)

--===================================================================
--===================================================================