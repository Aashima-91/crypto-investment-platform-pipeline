# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;

# COMMAND ----------

# MAGIC %md
# MAGIC # customers_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customers_clean AS
# MAGIC SELECT DISTINCT
# MAGIC     customer_id,
# MAGIC     name,
# MAGIC     email,
# MAGIC     country,
# MAGIC     risk_profile
# MAGIC FROM bronze.customers;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # customer_portfolios_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.customer_portfolios_clean AS
# MAGIC SELECT DISTINCT
# MAGIC     customer_id,
# MAGIC     asset,
# MAGIC     quantity,
# MAGIC     acquisition_date
# MAGIC FROM bronze.customer_portfolios;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # transactions_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.transactions_clean AS
# MAGIC SELECT DISTINCT
# MAGIC     transaction_id,
# MAGIC     customer_id,
# MAGIC     asset,
# MAGIC     type,
# MAGIC     quantity,
# MAGIC     price,
# MAGIC     date
# MAGIC FROM bronze.transactions;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # market_history_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.market_history_clean AS
# MAGIC SELECT DISTINCT
# MAGIC     asset,
# MAGIC     date,
# MAGIC     open,
# MAGIC     high,
# MAGIC     low,
# MAGIC     close,
# MAGIC     volume
# MAGIC FROM bronze.crypto_history;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC # market_prices_clean

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE silver.market_prices_clean AS
# MAGIC SELECT DISTINCT
# MAGIC     asset,
# MAGIC     price_usd,
# MAGIC     price_aud,
# MAGIC     change_24h,
# MAGIC     market_cap_usd
# MAGIC FROM bronze.market_prices_snapshot;
# MAGIC
# MAGIC
