# Databricks notebook source
# MAGIC %md
# MAGIC # customer_master

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customer_master AS
# MAGIC SELECT 
# MAGIC   c.customer_id,
# MAGIC   c.name AS customer_name,
# MAGIC   c.email,
# MAGIC   c.country,
# MAGIC   c.risk_profile
# MAGIC FROM silver.customers_clean c;

# COMMAND ----------

# MAGIC %md
# MAGIC # asset_prices_daily
# MAGIC ## Daily OHLC + returns + volatility flag

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.asset_prices_daily AS 
# MAGIC WITH base AS (
# MAGIC   SELECT
# MAGIC     asset,
# MAGIC     to_date(date) AS price_date,
# MAGIC     open,
# MAGIC     high,
# MAGIC     low,
# MAGIC     close,
# MAGIC     volume,
# MAGIC     LAG(close) OVER (PARTITION BY asset ORDER BY TO_DATE(date)) AS previous_close
# MAGIC   FROM silver.market_history_clean
# MAGIC )
# MAGIC SELECT
# MAGIC   *,
# MAGIC   ROUND(((close - previous_close) / previous_close) * 100, 4) AS daily_return_pct,
# MAGIC   CASE WHEN ABS(((close - previous_close) / previous_close) * 100) > 5 THEN TRUE ELSE FALSE END AS volatility_flag FROM base;

# COMMAND ----------

# MAGIC %md
# MAGIC # customer_positions
# MAGIC ## Compute portfolio value using latest price

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customer_positions AS 
# MAGIC WITH latest_price AS (
# MAGIC     SELECT
# MAGIC       asset,
# MAGIC       close,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY asset ORDER BY date DESC) AS rn
# MAGIC     FROM silver.market_history_clean
# MAGIC ), 
# MAGIC latest_per_asset AS (
# MAGIC     SELECT asset, close as latest_close_price
# MAGIC     FROM latest_price
# MAGIC     WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   cp.customer_id,
# MAGIC   cp.asset,
# MAGIC   cp.quantity,
# MAGIC   lp.latest_close_price,
# MAGIC   (cp.quantity * lp.latest_close_price) AS position_value_usd
# MAGIC FROM silver.customer_portfolios_clean cp
# MAGIC LEFT JOIN latest_per_asset lp
# MAGIC     ON LOWER(cp.asset) = LOWER(lp.asset);

# COMMAND ----------

# MAGIC %md
# MAGIC # customer_transactions_enriched
# MAGIC ## Add transaction value + type normalization

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customer_transactions_enriched AS
# MAGIC SELECT
# MAGIC     t.transaction_id,
# MAGIC     t.customer_id,
# MAGIC     t.asset,
# MAGIC     LOWER(t.type) AS transaction_type,
# MAGIC     t.quantity,
# MAGIC     t.price AS price_usd,
# MAGIC     (t.quantity * t.price) AS amount_usd,
# MAGIC     TO_DATE(t.date) AS transaction_date
# MAGIC FROM silver.transactions_clean t;
# MAGIC
