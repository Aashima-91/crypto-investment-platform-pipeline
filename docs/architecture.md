# 🏛️ Architecture Overview — Crypto Investment Platform

This document describes the end‑to‑end Medallion Architecture implemented on Databricks for the Crypto Investment Platform pipeline.

The architecture follows a clean, modular, production‑grade structure:

Bronze (raw)
↓
Silver Technical (clean)
↓
Silver Business (modeled)
↓
Gold (dim/fact)


---

## 🟫 Bronze Layer — Raw Ingestion

The Bronze layer captures raw CSV files directly from GitHub and loads them into Delta tables with no transformations applied.

**Purpose**
- Preserve original data
- Enable replay and auditability
- Maintain schema‑as‑received

**Bronze Tables**
- customers  
- customer_portfolios  
- transactions  
- crypto_history  
- market_prices_snapshot  
- assets  
- countries  
- risk_profiles  
- exchange_rates  
- audit_log  

---

## 🥈 Silver Technical Layer — Cleaned & Standardized

This layer applies technical cleaning to make data usable and consistent.

**Transformations**
- Deduplication  
- Type casting  
- Standardized column names  
- Removal of malformed rows  

**Silver Technical Tables**
- customers_clean  
- customer_portfolios_clean  
- transactions_clean  
- market_history_clean  
- market_prices_clean  

---

## 🥈 Silver Business Layer — Modeled & Enriched

This layer applies business logic and domain‑specific transformations.

**Transformations**
- Customer enrichment  
- Daily asset price modeling  
- Portfolio valuation  
- Transaction enrichment  
- Derived metrics (returns, volatility flags, position values)  

**Silver Business Tables**
- customer_master  
- asset_prices_daily  
- customer_positions  
- customer_transactions_enriched  

---

## 🥇 Gold Layer — Dimensional (Dim/Fact)

The Gold layer provides analytics‑ready dimensional models for BI dashboards and reporting.

**Dimensions**
- dim_customer (SCD2)
- dim_asset (SCD2)
- dim_date

**Facts**
- fact_portfolio_value  
- fact_transactions  
- fact_market_prices  

---

## 🎯 Outcome

This architecture delivers:
- A clean separation of ingestion, cleaning, modeling, and analytics layers  
- A star schema optimized for BI tools  
- A production‑grade pipeline orchestrated via Databricks Jobs  
