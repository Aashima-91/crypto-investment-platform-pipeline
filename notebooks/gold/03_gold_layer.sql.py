# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. DimCustomer (SCD2)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_customer (
# MAGIC     customer_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     customer_id INT,
# MAGIC     customer_name STRING,
# MAGIC     email STRING,
# MAGIC     country STRING,
# MAGIC     risk_profile STRING,
# MAGIC     effective_start_date DATE,
# MAGIC     effective_end_date DATE,
# MAGIC     is_current BOOLEAN
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # SCD2 MERGE

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_customer as tgt
# MAGIC USINg silver.customer_master as src
# MAGIC ON tgt.customer_id = src.customer_id
# MAGIC AND tgt.is_current = TRUE
# MAGIC
# MAGIC WHEN MATCHED AND (
# MAGIC     tgt.customer_name <> src.customer_name
# MAGIC     OR tgt.email <> src.email
# MAGIC     OR tgt.country <> src.country
# MAGIC     OR tgt.risk_profile <> src.risk_profile
# MAGIC
# MAGIC ) THEN UPDATE SET
# MAGIC     tgt.effective_end_date = CURRENT_DATE(),
# MAGIC     tgt.is_current = FALSE
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   customer_id,
# MAGIC   customer_name,
# MAGIC   email,
# MAGIC   country,
# MAGIC   risk_profile,
# MAGIC   effective_start_date,
# MAGIC   effective_end_date,
# MAGIC   is_current 
# MAGIC ) VALUES (
# MAGIC   src.customer_id,
# MAGIC   src.customer_name,
# MAGIC   src.email,
# MAGIC   src.country,
# MAGIC   src.risk_profile,
# MAGIC   CURRENT_DATE(), 
# MAGIC   DATE '9999-12-31', 
# MAGIC   TRUE
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. DimAsset (SCD2)
# MAGIC ## Uses both market history + market prices

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS gold.dim_asset (
# MAGIC     asset_sk BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC     asset STRING,
# MAGIC     effective_start_date DATE,
# MAGIC     effective_end_date DATE,
# MAGIC     is_current BOOLEAN
# MAGIC );
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO gold.dim_asset AS tgt
# MAGIC USING (
# MAGIC   SELECT DISTINCT LOWER(asset) AS asset
# MAGIC   FROM silver.market_history_clean
# MAGIC   UNION
# MAGIC   SELECT DISTINCT LOWER(asset) AS asset
# MAGIC   FROM silver.market_prices_clean
# MAGIC ) AS src
# MAGIC ON tgt.asset = src.asset
# MAGIC AND tgt.is_current = TRUE
# MAGIC
# MAGIC WHEN NOT MATCHED THEN INSERT (
# MAGIC   asset, 
# MAGIC   effective_start_date,
# MAGIC   effective_end_date,
# MAGIC   is_current
# MAGIC ) VALUES (
# MAGIC   src.asset,
# MAGIC   CURRENT_DATE(),
# MAGIC   DATE '9999-12-31',
# MAGIC   TRUE
# MAGIC )

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. DimDate

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date AS
# MAGIC WITH dates AS (
# MAGIC   SELECT explode(sequence(DATE '2023-01-01', DATE '2023-12-31', INTERVAL 1 DAY)) AS date 
# MAGIC )
# MAGIC
# MAGIC SELECT
# MAGIC   monotonically_increasing_id() AS date_sk,
# MAGIC   date,
# MAGIC   year(date) AS year,
# MAGIC   month(date) AS month,
# MAGIC   day(date) AS day,
# MAGIC   date_format(date, 'EEEE') AS day_name,
# MAGIC   quarter(date) AS quarter
# MAGIC FROM dates;

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. FactPortfolioValue

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_portfolio_value AS
# MAGIC SELECT
# MAGIC   dc.customer_sk,
# MAGIC   da.asset_sk,
# MAGIC   dd.date_sk,
# MAGIC   cp.quantity,
# MAGIC   cp.latest_close_price AS price_usd,
# MAGIC   (cp.quantity * cp.latest_close_price) AS position_value_usd
# MAGIC FROM silver.customer_positions cp
# MAGIC JOIN gold.dim_customer dc
# MAGIC ON cp.customer_id = dc.customer_id
# MAGIC AND dc.is_current = TRUE
# MAGIC JOIN gold.dim_asset da
# MAGIC ON LOWER(cp.asset) = da.asset
# MAGIC AND da.is_current = TRUE
# MAGIC JOIN gold.dim_date dd
# MAGIC ON dd.date = CURRENT_DATE();

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. FactTransactions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_transactions AS
# MAGIC SELECT
# MAGIC     dc.customer_sk,
# MAGIC     da.asset_sk,
# MAGIC     dd.date_sk,
# MAGIC     t.transaction_id,
# MAGIC     t.transaction_type,
# MAGIC     t.quantity,
# MAGIC     t.price_usd,
# MAGIC     t.amount_usd
# MAGIC FROM silver.customer_transactions_enriched t
# MAGIC JOIN gold.dim_customer dc
# MAGIC     ON t.customer_id = dc.customer_id AND dc.is_current = TRUE
# MAGIC JOIN gold.dim_asset da
# MAGIC     ON LOWER(t.asset) = da.asset AND da.is_current = TRUE
# MAGIC JOIN gold.dim_date dd
# MAGIC     ON t.transaction_date = dd.date;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. FactMarketPrices

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_market_prices AS
# MAGIC SELECT
# MAGIC     da.asset_sk,
# MAGIC     dd.date_sk,
# MAGIC     apd.open,
# MAGIC     apd.high,
# MAGIC     apd.low,
# MAGIC     apd.close,
# MAGIC     apd.volume,
# MAGIC     apd.daily_return_pct,
# MAGIC     apd.volatility_flag
# MAGIC FROM silver.asset_prices_daily apd
# MAGIC JOIN gold.dim_asset da
# MAGIC     ON LOWER(apd.asset) = da.asset AND da.is_current = TRUE
# MAGIC JOIN gold.dim_date dd
# MAGIC     ON apd.price_date = dd.date;
# MAGIC
