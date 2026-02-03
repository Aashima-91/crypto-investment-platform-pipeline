# Databricks notebook source
import requests
import pandas as pd
from io import StringIO
from pyspark.sql import functions as F

#bronze_path = "/Workspace/Users/aashima91.gupta@gmail.com/bronze/"
# Create the schema once 
spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

def load_csv(name):
    url = f"https://raw.githubusercontent.com/Aashima-91/crypto-investment-platform-pipeline/main/data/raw/{name}.csv"

    content = requests.get(url).content.decode("utf-8")
    pdf = pd.read_csv(StringIO(content))
    df = spark.createDataFrame(pdf)
    df = df.withColumn("ingestion_timestamp", F.current_timestamp())
    spark.sql(f"DROP TABLE IF EXISTS bronze.{name}")
    df.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{name}")

files = [
    "customers", "customer_portfolios", "transactions", "crypto_history",
    "market_prices_snapshot", "assets", "countries", "risk_profiles",
    "exchange_rates", "audit_log"
]

for f in files:
    load_csv(f)
