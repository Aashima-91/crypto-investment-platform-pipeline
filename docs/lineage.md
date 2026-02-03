# 🔗 Data Lineage — Crypto Investment Platform

This document describes the lineage from raw ingestion to analytical fact tables.

---

## 📐 Lineage Diagram (Mermaid)

```mermaid
flowchart LR

    subgraph Bronze
        B1[customers]
        B2[customer_portfolios]
        B3[transactions]
        B4[crypto_history]
        B5[market_prices_snapshot]
    end

    subgraph SilverTechnical
        S1[customers_clean]
        S2[customer_portfolios_clean]
        S3[transactions_clean]
        S4[market_history_clean]
        S5[market_prices_clean]
    end

    subgraph SilverBusiness
        SB1[customer_master]
        SB2[asset_prices_daily]
        SB3[customer_positions]
        SB4[customer_transactions_enriched]
    end

    subgraph Gold
        G1[dim_customer]
        G2[dim_asset]
        G3[dim_date]
        G4[fact_portfolio_value]
        G5[fact_transactions]
        G6[fact_market_prices]
    end

    B1 --> S1 --> SB1 --> G1
    B2 --> S2 --> SB3 --> G4
    B3 --> S3 --> SB4 --> G5
    B4 --> S4 --> SB2 --> G6
    B5 --> S5 --> SB3 --> G4
    SB2 --> G6
    SB3 --> G4
    SB4 --> G5
    G3 --> G4
    G3 --> G5
    G3 --> G6
