-- Databricks notebook source
create database if not exists dbacademy;
use dbacademy;

-- COMMAND ----------

drop table IF EXISTS health_tracker_data_2020_01; 
create TABLE if not exists health_tracker_data_2020_01
using JSON
location 'dbfs:/FileStore/tables/health_tracker_data_2020_01_dirty.json'
options (inferSchema=true);

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe EXTENDED health_tracker_data_2020_01;

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01;

-- COMMAND ----------

-- JSON is slow to query. Create Silver tables in parquet format,
-- cleaning the data as we go.
drop table IF EXISTS health_tracker_silver;

create table health_tracker_silver
USING PARQUET
PARTITIONED BY (p_device_id)
LOCATION "/dbacademy/DLRS/healthtracker/silver"
AS (
  SELECT name,
         heartrate,
         CAST(FROM_UNIXTIME(timestamp) AS TIMESTAMP) as time,
         CAST(FROM_UNIXTIME(timestamp) AS DATE) as dte,
         device_id AS p_device_id
  FROM health_tracker_data_2020_01
)

-- COMMAND ----------

-- Convert to delta table e.g, makes a copy
DESCRIBE DETAIL health_tracker_silver;  -- this is in parquet

drop table IF EXISTS health_tracker_silver_delta;

create table health_tracker_silver_delta
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "/dbacademy/DLRS/healthtracker/silver_delta"
AS (
  SELECT *
  FROM health_tracker_silver
)

-- COMMAND ----------

-- Convert original table to delta inplace
CONVERT TO DELTA
  parquet.`/dbacademy/DLRS/healthtracker/silver`
  PARTITIONED BY (p_device_id double)

-- COMMAND ----------

-- Recreate the table based on delta
drop table IF EXISTS health_tracker_silver;

CREATE TABLE health_tracker_silver
USING DELTA
LOCATION "/dbacademy/DLRS/healthtracker/silver"

-- COMMAND ----------

-- MAGIC %fs ls /dbacademy/DLRS/healthtracker/silver/

-- COMMAND ----------

-- MAGIC %fs head /dbacademy/DLRS/healthtracker/silver/_delta_log/00000000000000000000.json

-- COMMAND ----------

 
