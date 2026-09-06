# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 02 · Bronze — market data (yfinance)  (incremental)
# MAGIC
# MAGIC Daily OHLCV bars for each pilot ticker, fetched in a `mapInPandas` partition
# MAGIC function (one yfinance call per ticker). Lands `bronze_market_bars`, joined
# MAGIC onto the company/period grain in Silver for enrichment.
# MAGIC
# MAGIC **Incremental:** MERGE on `(cik, bar_date)`, refreshing matched rows (yfinance
# MAGIC revises recent closes for splits / dividends). First run — or `mode=full` —
# MAGIC pulls `full_period` (10y); later runs pull `incr_period` (5d) and only the
# MAGIC new/updated days are merged in. History is never dropped.

# COMMAND ----------

# MAGIC %pip install --quiet yfinance

# COMMAND ----------

# DBTITLE 1,Widgets & config
import json
import datetime as dt

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("ciks_config", "../config/ciks.json")
dbutils.widgets.dropdown("mode", "incremental", ["incremental", "full"])
dbutils.widgets.text("full_period", "10y")
dbutils.widgets.text("incr_period", "5d")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MODE = dbutils.widgets.get("mode")
TABLE = f"{CATALOG}.{SCHEMA}.bronze_market_bars"

with open(dbutils.widgets.get("ciks_config")) as fh:
    COMPANIES = json.load(fh)["companies"]

INGESTED_AT = dt.datetime.utcnow().isoformat()
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

FULL_PERIOD = dbutils.widgets.get("full_period")
INCR_PERIOD = dbutils.widgets.get("incr_period")
FORCE_FULL = MODE == "full"

# per-ticker, not per-table: a ticker with no history yet gets the full pull even
# when the table already exists (the scale bug — 470 new S&P 500 tickers were
# getting only a 5-day window because bronze_market_bars existed from the pilot).
if spark.catalog.tableExists(TABLE):
    _have = {r["ticker"] for r in spark.table(TABLE).select("ticker").distinct().collect()}
else:
    _have = set()
print(f"{len(COMPANIES)} tickers  ({len(_have)} already have history)  "
      f"mode={MODE}  full={FULL_PERIOD}  incr={INCR_PERIOD}")

# COMMAND ----------

# DBTITLE 1,Fetch bars (one yfinance call per ticker)
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DateType, DoubleType, LongType,
)

seed = spark.createDataFrame(
    [
        (c["cik"].zfill(10), c["ticker"],
         INCR_PERIOD if (c["ticker"] in _have and not FORCE_FULL) else FULL_PERIOD)
        for c in COMPANIES if c.get("ticker")
    ],
    schema=["cik", "ticker", "period"],
).repartition(max(8, min(256, len(COMPANIES))))

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


def fetch_bars(iterator):
    import pandas as pd
    import yfinance as yf

    for pdf in iterator:
        frames = []
        for _, row in pdf.iterrows():
            try:
                hist = yf.Ticker(row["ticker"]).history(
                    period=row["period"], interval="1d"
                )
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

# DBTITLE 1,Write bronze_market_bars (MERGE on cik, bar_date)
staged = bars.dropDuplicates(["cik", "bar_date"])

if not spark.catalog.tableExists(TABLE):
    staged.write.format("delta").option("mergeSchema", "true") \
        .partitionBy("cik").saveAsTable(TABLE)
    n_new = spark.table(TABLE).count()
    print(f"created {n_new:,} rows -> bronze_market_bars")
else:
    staged.createOrReplaceTempView("_stg_market_bars")
    # plain MERGE — fixed schema (the session-level autoMerge conf is rejected
    # on serverless anyway).
    spark.sql(
        f"""
        MERGE INTO {TABLE} t USING _stg_market_bars s
          ON t.cik = s.cik AND t.bar_date = s.bar_date
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
    )
    n_new = staged.count()
    print(f"merged {n_new:,} staged rows -> bronze_market_bars "
          f"(table now {spark.table(TABLE).count():,} rows)")

dbutils.notebook.exit(
    json.dumps({"status": "ok", "mode": MODE,
                "full_pull_tickers": len(COMPANIES) - len(_have),
                "staged_rows": int(n_new), "ingested_at": INGESTED_AT})
)
