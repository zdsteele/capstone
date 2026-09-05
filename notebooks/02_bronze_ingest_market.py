# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 · Bronze — market data (yfinance)
# MAGIC
# MAGIC Daily OHLCV bars for each pilot ticker, fetched in a `mapInPandas` partition
# MAGIC function (one yfinance call per ticker). Lands `bronze_market_bars`, joined
# MAGIC onto the company/period grain in Silver for enrichment.

# COMMAND ----------

# MAGIC %pip install --quiet yfinance

# COMMAND ----------

# DBTITLE 1,Widgets & config
import json
import datetime as dt

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele")
dbutils.widgets.text("ciks_config", "../config/ciks.json")
dbutils.widgets.text("period", "10y")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
PERIOD = dbutils.widgets.get("period")

with open(dbutils.widgets.get("ciks_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

INGESTED_AT = dt.datetime.utcnow().isoformat()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"tickers: {[c['ticker'] for c in COMPANIES]}  period={PERIOD}")

# COMMAND ----------

# DBTITLE 1,Fetch bars (one yfinance call per ticker)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType, LongType,
)

seed = spark.createDataFrame(
    [(c["cik"].zfill(10), c["ticker"]) for c in COMPANIES if c.get("ticker")],
    schema=["cik", "ticker"],
).repartition(len(COMPANIES))

out_schema = StructType(
    [
        StructField("cik", StringType()),
        StructField("ticker", StringType()),
        StructField("bar_date", DateType()),
        StructField("open", DoubleType()),
        StructField("high", DoubleType()),
        StructField("low", DoubleType()),
        StructField("close", DoubleType()),
        StructField("adj_close", DoubleType()),
        StructField("volume", LongType()),
    ]
)

_period = PERIOD


def fetch_bars(iterator):
    import pandas as pd
    import yfinance as yf

    for pdf in iterator:
        frames = []
        for _, row in pdf.iterrows():
            try:
                hist = yf.Ticker(row["ticker"]).history(period=_period, interval="1d")
            except Exception:
                continue
            if hist is None or hist.empty:
                continue
            hist = hist.reset_index().rename(
                columns={
                    "Date": "bar_date", "Open": "open", "High": "high",
                    "Low": "low", "Close": "close", "Adj Close": "adj_close",
                    "Volume": "volume",
                }
            )
            hist["cik"] = row["cik"]
            hist["ticker"] = row["ticker"]
            if "adj_close" not in hist:
                hist["adj_close"] = hist["close"]
            hist["bar_date"] = pd.to_datetime(hist["bar_date"]).dt.date
            hist["volume"] = hist["volume"].fillna(0).astype("int64")
            frames.append(
                hist[
                    ["cik", "ticker", "bar_date", "open", "high", "low",
                     "close", "adj_close", "volume"]
                ]
            )
        if frames:
            yield pd.concat(frames, ignore_index=True)
        else:
            yield pd.DataFrame(
                columns=[f.name for f in out_schema.fields]
            )


bars = seed.mapInPandas(fetch_bars, schema=out_schema).withColumn(
    "ingested_at", F.lit(INGESTED_AT)
)

# COMMAND ----------

# DBTITLE 1,Write bronze_market_bars
n = bars.count()
(
    bars.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_market_bars")
)
print(f"wrote {n:,} rows -> bronze_market_bars")
dbutils.notebook.exit(json.dumps({"status": "ok", "rows": n, "ingested_at": INGESTED_AT}))
