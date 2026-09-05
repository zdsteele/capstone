# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 07 · Tag the capstone tables in Unity Catalog
# MAGIC
# MAGIC The medallion tables share `bootcamp_students.zdsteele_capstone` with
# MAGIC other bootcamp work. UC has no table "colors", but tags + comments make the
# MAGIC capstone tables easy to spot and filter:
# MAGIC
# MAGIC - In **Catalog Explorer**, filter with `project:edgar_capstone` (or search
# MAGIC   the tag), and each table shows `project` / `capstone_layer` in its detail pane.
# MAGIC - The `COMMENT` renders in the description column of the table list.
# MAGIC
# MAGIC Idempotent — re-run any time (e.g. after adding tables). Also tags the
# MAGIC `bronze_edgar_raw` Volume.

# COMMAND ----------

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
FQ = f"{CATALOG}.{SCHEMA}"

PREFIXES = ("bronze_", "silver_", "gold_", "lb_", "analytics_")


def layer_of(name: str) -> str:
    if name.startswith("lb_"):
        return "cdc_landing"
    if name.startswith("analytics_"):
        return "control"
    if name.startswith(("gold_usage", "gold_agent")):
        return "gold_analytics"
    return name.split("_", 1)[0]  # bronze / silver / gold


tables = [
    r.tableName
    for r in spark.sql(f"SHOW TABLES IN {FQ}").collect()
    if r.tableName.startswith(PREFIXES)
]

for t in tables:
    layer = layer_of(t)
    spark.sql(
        f"ALTER TABLE {FQ}.{t} SET TAGS "
        f"('project' = 'edgar_capstone', 'capstone_layer' = '{layer}')"
    )
    spark.sql(
        f"COMMENT ON TABLE {FQ}.{t} IS "
        f"'EDGAR Intelligence Platform (capstone) - {layer} layer'"
    )
    print(f"  tagged {t:<34} [{layer}]")

# the raw-filings Volume
try:
    spark.sql(
        f"ALTER VOLUME {FQ}.bronze_edgar_raw SET TAGS ('project' = 'edgar_capstone', 'capstone_layer' = 'bronze')"
    )
    spark.sql(
        f"COMMENT ON VOLUME {FQ}.bronze_edgar_raw IS 'EDGAR Intelligence Platform (capstone) - raw filing HTML / XBRL / submission .txt'"
    )
    print("  tagged VOLUME bronze_edgar_raw")
except Exception as exc:
    print("  volume tag skipped:", exc)

print(f"\ntagged {len(tables)} tables. Filter in Catalog Explorer: project:edgar_capstone")
