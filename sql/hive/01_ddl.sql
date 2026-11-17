-- Hive external tables over the Parquet files Spark wrote to HDFS.
-- External = Hive owns only the metadata; dropping a table never deletes data.
-- Run:  docker compose exec hive beeline -u jdbc:hive2://localhost:10000 -f /opt/sql/01_ddl.sql

CREATE DATABASE IF NOT EXISTS telecom;
USE telecom;

CREATE EXTERNAL TABLE IF NOT EXISTS dim_date (
  date_key INT, `date` DATE, `year` INT, `month` INT, `day` INT, day_name STRING, is_weekend BOOLEAN
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/dim_date';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_tower (
  tower_sk INT, tower_id STRING, region STRING, technology STRING, install_date DATE
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/dim_tower';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_plan (
  plan_sk INT, plan_id STRING, plan_name STRING, plan_type STRING, monthly_fee DOUBLE
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/dim_plan';

CREATE EXTERNAL TABLE IF NOT EXISTS dim_customer (
  customer_sk INT, customer_id STRING, home_region STRING, plan_sk INT, plan_id STRING,
  plan_name STRING, plan_type STRING, valid_from DATE, valid_to DATE, is_current BOOLEAN
) STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/dim_customer';

-- Fact table is partitioned by call_date (matches the Spark output folders call_date=YYYY-MM-DD).
CREATE EXTERNAL TABLE IF NOT EXISTS fact_usage (
  cdr_id STRING, date_key INT, call_hour INT, customer_sk INT, tower_sk INT, plan_sk INT,
  call_type STRING, status STRING, is_dropped BOOLEAN, duration_sec INT, data_mb DOUBLE, charge_aed DOUBLE
) PARTITIONED BY (call_date DATE)
STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/fact_usage';
MSCK REPAIR TABLE fact_usage;   -- register the partition folders Spark created

CREATE EXTERNAL TABLE IF NOT EXISTS agg_region_daily (
  region STRING, records BIGINT, drop_rate DOUBLE, revenue_aed DOUBLE, data_gb DOUBLE, active_customers BIGINT
) PARTITIONED BY (call_date DATE)
STORED AS PARQUET LOCATION 'hdfs://namenode:8020/telecom/marts/agg_region_daily';
