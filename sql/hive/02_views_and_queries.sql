-- Reporting views + example analytical queries. Run after 01_ddl.sql.
USE telecom;

CREATE VIEW IF NOT EXISTS v_daily_revenue_by_region AS
SELECT t.region, f.call_date, COUNT(*) AS records,
       ROUND(SUM(f.charge_aed), 2) AS revenue_aed,
       ROUND(AVG(CASE WHEN f.is_dropped THEN 1.0 ELSE 0.0 END), 4) AS drop_rate
FROM fact_usage f JOIN dim_tower t ON f.tower_sk = t.tower_sk
GROUP BY t.region, f.call_date;

CREATE VIEW IF NOT EXISTS v_revenue_by_plan AS
SELECT c.plan_name, c.plan_type, COUNT(*) AS records, ROUND(SUM(f.charge_aed), 2) AS revenue_aed
FROM fact_usage f JOIN dim_customer c ON f.customer_sk = c.customer_sk
GROUP BY c.plan_name, c.plan_type;

-- Q1: top 10 busiest towers and their share of all traffic (shows the skew)
SELECT t.tower_id, t.region, COUNT(*) AS records,
       ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct_of_traffic
FROM fact_usage f JOIN dim_tower t ON f.tower_sk = t.tower_sk
GROUP BY t.tower_id, t.region ORDER BY records DESC LIMIT 10;

-- Q2: partition pruning demo. Compare the plan of these two with EXPLAIN:
EXPLAIN SELECT COUNT(*) FROM fact_usage WHERE call_date = DATE '2026-09-15';   -- reads ONE partition
EXPLAIN SELECT COUNT(*) FROM fact_usage WHERE hour(date_key) = 1;              -- cannot prune

-- Q3: 7-day moving average of daily drop rate per region (window function)
SELECT region, call_date, drop_rate,
       ROUND(AVG(drop_rate) OVER (PARTITION BY region ORDER BY call_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW), 4) AS drop_rate_7d_avg
FROM v_daily_revenue_by_region ORDER BY region, call_date;

-- Q4: history is preserved (SCD2): revenue by the plan the customer was on AT THE TIME of the call
SELECT c.plan_name, d.is_weekend, ROUND(SUM(f.charge_aed), 2) AS revenue_aed
FROM fact_usage f
JOIN dim_customer c ON f.customer_sk = c.customer_sk
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY c.plan_name, d.is_weekend ORDER BY revenue_aed DESC;
