# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · Reconcile against the Bloomberg sample
# MAGIC
# MAGIC Loads the three Bloomberg screens (`config/bloomberg_reference/*.csv`, as
# MAGIC pasted — fiscal period 2026 Q2, GTLS 2026 Q1) into `bronze_bloomberg_reference`
# MAGIC and compares them, metric by metric, with the gold row for the **same fiscal
# MAGIC period** (not the latest — later quarters have been filed since).
# MAGIC
# MAGIC `gold_bloomberg_reconciliation` status:
# MAGIC - `match` — within 5% relative, or a small absolute gap (0.5 pt / 0.5 USD mm / $0.02)
# MAGIC - `miss` — outside tolerance → check `silver_ltm_lineage`, then add a
# MAGIC   per-ticker override in `config/concept_map.json`
# MAGIC - `ours_missing` / `bloomberg_blank` — one side has no value
# MAGIC
# MAGIC TSR / Stock Return are skipped (no market data yet).

# COMMAND ----------

import json

import pandas as pd
from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("bronze_schema", "bronze_edgar")
dbutils.widgets.text("gold_schema", "gold_edgar")
dbutils.widgets.text("reference_dir", "../config/bloomberg_reference")
CATALOG = dbutils.widgets.get("catalog")
B = lambda n: f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}.{n}"
G = lambda n: f"{CATALOG}.{dbutils.widgets.get('gold_schema')}.{n}"
REF = dbutils.widgets.get("reference_dir")

SKIP = {"tsr_1y", "stock_return_pct"}
SCREENS = {"financials": "gold_peer_financials",
           "profitability": "gold_peer_profitability",
           "leverage": "gold_peer_leverage"}

# COMMAND ----------

# DBTITLE 1,bronze_bloomberg_reference (long)
fin = pd.read_csv(f"{REF}/financials.csv")
period_of = dict(zip(fin["bloomberg_ticker"], fin["fiscal_period"]))
long_frames = []
for screen in SCREENS:
    df = pd.read_csv(f"{REF}/{screen}.csv")
    if "fiscal_period" not in df:
        df["fiscal_period"] = df["bloomberg_ticker"].map(period_of)
    metrics = [c for c in df.columns if c not in ("bloomberg_ticker", "company_name", "fiscal_period")]
    melted = df.melt(id_vars=["bloomberg_ticker", "fiscal_period"], value_vars=metrics,
                     var_name="metric", value_name="bloomberg_value")
    melted.insert(0, "screen", screen)
    long_frames.append(melted)
ref = pd.concat(long_frames, ignore_index=True)
ref["bloomberg_value"] = pd.to_numeric(ref["bloomberg_value"], errors="coerce")
ref_df = spark.createDataFrame(ref.astype(object).where(ref.notna(), None))
ref_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(B("bronze_bloomberg_reference"))
ref_df = spark.table(B("bronze_bloomberg_reference")).filter(~F.col("metric").isin(*SKIP))

# COMMAND ----------

# DBTITLE 1,Ours (long), same fiscal period
ours = None
for screen, table in SCREENS.items():
    metrics = [m for m in ref[ref["screen"] == screen]["metric"].unique() if m not in SKIP]
    df = spark.table(G(table)).select(
        F.lit(screen).alias("screen"), "bloomberg_ticker", "fiscal_period", "as_of_date",
        F.explode(F.array(*[F.struct(F.lit(m).alias("metric"), F.col(m).cast("double").alias("our_value"))
                            for m in metrics])).alias("kv"))
    df = df.select("screen", "bloomberg_ticker", "fiscal_period", "as_of_date", "kv.metric", "kv.our_value")
    ours = df if ours is None else ours.unionByName(df)

# COMMAND ----------

# DBTITLE 1,gold_bloomberg_reconciliation
abs_tol = (F.when(F.col("metric") == "eps_diluted", F.lit(0.02))
           .when(F.col("metric") == "cash_flow_per_share", F.lit(0.02))
           .otherwise(F.lit(0.5)))
diff = F.col("our_value") - F.col("bloomberg_value")
recon = (
    ref_df.join(ours, ["screen", "bloomberg_ticker", "fiscal_period", "metric"], "left")
    .withColumn("diff", diff)
    .withColumn("diff_pct", F.try_divide(diff, F.abs("bloomberg_value")) * 100)
    .withColumn("status",
                F.when(F.col("bloomberg_value").isNull(), F.lit("bloomberg_blank"))
                 .when(F.col("our_value").isNull(), F.lit("ours_missing"))
                 .when((F.abs(diff) <= abs_tol) | (F.abs(F.col("diff_pct")) <= 5), F.lit("match"))
                 .otherwise(F.lit("miss")))
    .withColumn("ticker", F.split("bloomberg_ticker", " ").getItem(0))
    .select("screen", "ticker", "bloomberg_ticker", "fiscal_period", "as_of_date", "metric",
            "our_value", "bloomberg_value", "diff", "diff_pct", "status")
)
recon.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(G("gold_bloomberg_reconciliation"))
recon = spark.table(G("gold_bloomberg_reconciliation"))

by_status = {r["status"]: r["count"] for r in recon.groupBy("status").count().collect()}
compared = by_status.get("match", 0) + by_status.get("miss", 0)
print(by_status, f"match rate {by_status.get('match', 0) / max(compared, 1):.1%} of {compared} compared")
display(recon.groupBy("screen", "metric").pivot("status", ["match", "miss", "ours_missing"]).count()
        .orderBy("screen", "metric"))
display(recon.filter("status IN ('miss', 'ours_missing')").orderBy("screen", "metric", "ticker"))

dbutils.notebook.exit(json.dumps({"status": "ok", "by_status": by_status}))
