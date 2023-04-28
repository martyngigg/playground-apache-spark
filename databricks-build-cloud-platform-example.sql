-- Databricks notebook source
create database if not exists dbacademy;
use dbacademy;

-- COMMAND ----------

-- Create the bronze table that directly looks at the JSON source
drop table IF EXISTS health_tracker_data_2020_01; 
create TABLE health_tracker_data_2020_01
using JSON
location 'dbfs:/FileStore/tables/health_tracker_data_2020_01.json'
options (inferSchema=true);

-- COMMAND ----------

show tables;

-- COMMAND ----------

describe EXTENDED health_tracker_data_2020_01;

-- COMMAND ----------

SELECT * FROM health_tracker_data_2020_01;

-- COMMAND ----------

-- MAGIC %md **THE NEXT CELL DELETES THE DELTA TABLES ON DISK!!! This for demo purposes. DO NOT DO THIS IN PRODUCTION**

-- COMMAND ----------

-- MAGIC %fs rm -r /dbacademy/DLRS/healthtracker/silver

-- COMMAND ----------

-- JSON is slow to query. Create Silver tables in delta (parquet + transaction) format,
-- cleaning the data as we go.
drop table IF EXISTS health_tracker_silver;

create table health_tracker_silver
USING DELTA
PARTITIONED BY (p_device_id)
LOCATION "/dbacademy/DLRS/healthtracker/silver"
AS (
  SELECT name,
         heartrate,
         CAST(FROM_UNIXTIME(timestamp) AS TIMESTAMP) as time,
         CAST(FROM_UNIXTIME(timestamp) AS DATE) as dte,
         device_id AS p_device_id
  FROM health_tracker_data_2020_01               -- FROM json.'dbfs:/FileStore/tables/health_tracker_data_2020_dirty.json' would avoid creating the bronze table but they can be useful
)

-- COMMAND ----------

-- MAGIC %fs ls /dbacademy/DLRS/healthtracker/silver/

-- COMMAND ----------

-- MAGIC %fs head /dbacademy/DLRS/healthtracker/silver/_delta_log/00000000000000000000.json

-- COMMAND ----------

-- MAGIC %fs ls /dbacademy/DLRS/healthtracker/silver/p_device_id=0/

-- COMMAND ----------

-- MAGIC %fs mounts

-- COMMAND ----------

select * FROM health_tracker_silver

-- COMMAND ----------

DESCRIBE DETAIL health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

-- MAGIC %md
-- MAGIC **NEXT CELL DELETES OLD FILES SO THE NOTEBOOK WILL RUN IF THE TRAINING HAS BEEN RUN BEFORE. DO NOT DO THIS IN PROD**

-- COMMAND ----------

-- MAGIC %fs rm -r /dbacademy/DLRS/healthtracker/gold

-- COMMAND ----------

DROP TABLE IF EXISTS health_tracker_user_analytics;

-- Create data mart for basic summary data. These tables are required for performance to avoid
-- recomputing expensive analytics on large datasets but may not be necessary...
CREATE TABLE health_tracker_user_analytics
USING DELTA
LOCATION 'dbfs:/dbacademy/DLRS/healthtracker/gold/health_tracker_user_analytics'
AS (
  SELECT p_device_id,
         AVG(heartrate) AS avg_heartrate,
         STD(heartrate) AS std_heartrate,
         MAX(heartrate) AS max_heartrate
  FROM health_tracker_silver GROUP BY p_device_id
)

-- COMMAND ----------

select * from health_tracker_user_analytics order by p_device_id

-- COMMAND ----------

-- Create the bronze table that directly looks at the second JSON source
drop table IF EXISTS health_tracker_data_2020_02; 
create TABLE health_tracker_data_2020_02
using JSON
location 'dbfs:/FileStore/tables/health_tracker_data_2020_02.json'
options (inferSchema=true);

-- Insert new data to existing silver tables
INSERT INTO health_tracker_silver
 SELECT name,
        heartrate,
        CAST(FROM_UNIXTIME(timestamp) AS TIMESTAMP) as time,
        CAST(FROM_UNIXTIME(timestamp) AS DATE) as dte,
        device_id AS p_device_id
FROM health_tracker_data_2020_02

-- COMMAND ----------

show tables;

-- COMMAND ----------

DESCRIBE HISTORY health_tracker_silver;

-- COMMAND ----------

-- MAGIC %fs ls dbfs:/dbacademy/DLRS/healthtracker/silver/_delta_log

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver

-- COMMAND ----------

SELECT COUNT(*) FROM health_tracker_silver VERSION AS OF 0

-- COMMAND ----------

-- Count number of records by device
SELECT p_device_id, COUNT(*) FROM health_tracker_silver GROUP BY p_device_id ORDER BY p_device_id

-- COMMAND ----------

-- Identify missing records
SELECT * FROM health_tracker_silver WHERE p_device_id IN (3,4)

-- COMMAND ----------

-- Identify broken records (heartrate < 0)
-- Use view rather than table on disk
CREATE OR REPLACE TEMPORARY VIEW broken_readings
AS (
  SELECT p_device_id, heartrate, time FROM health_tracker_silver
  WHERE heartrate < 0
  ORDER BY time
)

-- COMMAND ----------

SELECT * FROM broken_readings ORDER BY p_device_id

-- COMMAND ----------

SELECT SUM(broken_readings_count) FROM broken_readings

-- COMMAND ----------

-- UPSERT to fix broken data

CREATE OR REPLACE TEMPORARY VIEW updates
AS (
  SELECT name, (prev_amt+next_amt)/2 AS heartrate, time, dte, p_device_id
  FROM (
    SELECT *,
    LAG(heartrate) OVER (PARTITION BY p_device_id, time ORDER BY p_device_id, time) as prev_amt,
    LEAD(heartrate) OVER (PARTITION BY p_device_id, time ORDER BY p_device_id, time) as next_amt
    FROM health_tracker_silver
  )
  WHERE heartrate < 0
)

-- COMMAND ----------

select * from updates;

-- COMMAND ----------


