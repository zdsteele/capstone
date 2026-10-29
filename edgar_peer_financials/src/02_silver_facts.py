# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Silver — companies, filings, financial facts
# MAGIC
# MAGIC | Table | Built from | Notes |
# MAGIC |---|---|---|
# MAGIC | `silver_companies` | `bronze_submissions` + `config/peers.json` | ticker, Bloomberg ticker, fiscal-year-end month |
# MAGIC | `silver_filings` | `bronze_filings` | typed dates, one row per accession |
# MAGIC | `silver_financial_facts` | XBRL instances (Volume) ∪ `bronze_companyfacts` | one row per reported number, 10-K/10-Q only |
# MAGIC
# MAGIC Two fact sources, tagged in `source`:
# MAGIC - **`instance`** — each 10-K/10-Q's own XBRL instance, parsed with
# MAGIC   `lib.edgar_parse.parse_xbrl_instance` inside `mapInPandas`. Includes
# MAGIC   company extension concepts (`whd:…`) and dimensioned facts (`dims`).
# MAGIC - **`companyfacts`** — SEC's pre-aggregated standard-taxonomy facts; covers
# MAGIC   history before `since_date` and acts as the fallback.
# MAGIC
# MAGIC `months` (3/6/9/12) on duration facts is what notebook 04 uses to tell a
# MAGIC quarter from a YTD from an annual figure.

# COMMAND ----------

# DBTITLE 1,Setup
import json
import os
import sys

_root = os.path.abspath(os.path.join(os.getcwd(), ".."))
if _root not in sys.path:
    sys.path.insert(0, _root)

# Ship the pure-Python parser to executors: serverless has no addPyFile, so
# register the module for pickle-by-value and cloudpickle carries the functions
# in the UDF. The UDFs reference the *functions*, never the module object —
# pickling the module by value drags its __loader__ (a dbruntime workspace-files
# import hook) along, and executors fail with "No module named 'dbruntime'".
from lib import edgar_parse
from lib.edgar_parse import flatten_company_facts, parse_xbrl_instance

try:
    from pyspark import cloudpickle as _cp
except Exception:
    import cloudpickle as _cp
_cp.register_pickle_by_value(edgar_parse)

from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

dbutils.widgets.text("catalog", "engineering_playground_dev")
dbutils.widgets.text("bronze_schema", "bronze_edgar")
dbutils.widgets.text("silver_schema", "silver_edgar")
dbutils.widgets.text("peers_config", "../config/peers.json")
CATALOG = dbutils.widgets.get("catalog")
B = lambda n: f"{CATALOG}.{dbutils.widgets.get('bronze_schema')}.{n}"
S = lambda n: f"{CATALOG}.{dbutils.widgets.get('silver_schema')}.{n}"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{dbutils.widgets.get('silver_schema')}")

with open(dbutils.widgets.get("peers_config")) as fh:
    PEERS = json.load(fh)["companies"]

PERIODIC_FORMS = ["10-K", "10-Q", "10-K/A", "10-Q/A"]


def _overwrite(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(S(table))
    print(f"{table}: {spark.table(S(table)).count():,}")

# COMMAND ----------

# DBTITLE 1,silver_companies
peers = spark.createDataFrame(
    [(p["cik"].zfill(10), p["ticker"], p["bloomberg_ticker"], p["name"]) for p in PEERS],
    ["cik", "ticker", "bloomberg_ticker", "name"],
)
companies = (
    peers.join(
        spark.table(B("bronze_submissions")).select(
            "cik", F.col("entity_name").alias("sec_name"), "fiscal_year_end", "sic", "sic_description"),
        "cik", "left")
    # fiscalYearEnd is MMDD, e.g. "1231"
    .withColumn("fye_month", F.substring("fiscal_year_end", 1, 2).cast("int"))
)
_overwrite(companies, "silver_companies")

# COMMAND ----------

# DBTITLE 1,silver_filings
filings = (
    spark.table(B("bronze_filings"))
    .withColumn("filing_date", F.to_date("filing_date"))
    .withColumn("report_date", F.to_date(F.expr("nullif(report_date, '')")))
    .withColumn("sec_url", F.concat(
        F.lit("https://www.sec.gov/Archives/edgar/data/"),
        F.expr("cast(cast(cik as bigint) as string)"), F.lit("/"),
        F.regexp_replace("accession", "-", ""), F.lit("/"), F.col("primary_document")))
    .dropDuplicates(["accession"])
    .drop("ingested_at")
)
_overwrite(filings, "silver_filings")

# COMMAND ----------

# DBTITLE 1,Instance facts (mapInPandas over the Volume files)
_INST_SCHEMA = StructType([
    StructField("cik", StringType()), StructField("accession", StringType()),
    StructField("concept", StringType()), StructField("unit", StringType()),
    StructField("period_start", StringType()), StructField("period_end", StringType()),
    StructField("value", DoubleType()), StructField("dims", StringType()),
])
_INST_COLS = [f.name for f in _INST_SCHEMA.fields]


def _parse_instances(iterator):
    # io.open, not open: the notebook's `open` is a dbruntime wrapper, and
    # cloudpickle would ship it to executors that have no dbruntime module.
    import io
    import pandas as pd
    for pdf in iterator:
        rows = []
        for _, r in pdf.iterrows():
            try:
                with io.open(r["volume_path"], encoding="utf-8", errors="replace") as fh:
                    facts = parse_xbrl_instance(fh.read())
            except Exception:
                continue
            for f in facts:
                rows.append({"cik": r["cik"], "accession": r["accession"], **{k: f[k] for k in _INST_COLS[2:]}})
        yield pd.DataFrame(rows, columns=_INST_COLS)


instance_docs = (
    spark.table(B("bronze_filing_documents")).filter(F.col("doc_type") == "XBRL_INSTANCE")
    .select("cik", "accession", "volume_path")
)
n_docs = instance_docs.count()
instance_facts = (
    instance_docs.repartition(max(4, min(64, n_docs // 8)))
    .mapInPandas(_parse_instances, schema=_INST_SCHEMA)
    .join(filings.select("accession", "form", F.col("filing_date").alias("filed")), "accession")
    .withColumn("source", F.lit("instance"))
)

# COMMAND ----------

# DBTITLE 1,Companyfacts facts
_CF_SCHEMA = StructType([
    StructField("cik", StringType()), StructField("accession", StringType()),
    StructField("form", StringType()), StructField("filed", StringType()),
    StructField("concept", StringType()), StructField("unit", StringType()),
    StructField("period_start", StringType()), StructField("period_end", StringType()),
    StructField("value", DoubleType()),
])
_CF_COLS = [f.name for f in _CF_SCHEMA.fields]


def _flatten_cf(iterator):
    import json as _json
    import pandas as pd
    for pdf in iterator:
        rows = []
        for _, r in pdf.iterrows():
            for f in flatten_company_facts(_json.loads(r["companyfacts_json"]), r["cik"]):
                rows.append({**{k: f[k] for k in ("cik", "accession", "form", "filed", "unit",
                                                    "period_start", "period_end", "value")},
                             "concept": f"{f['taxonomy']}:{f['concept']}"})
        yield pd.DataFrame(rows, columns=_CF_COLS)


cf_facts = (
    spark.table(B("bronze_companyfacts")).select("cik", "companyfacts_json")
    .repartition(8)
    .mapInPandas(_flatten_cf, schema=_CF_SCHEMA)
    .withColumn("filed", F.to_date("filed"))
    .withColumn("dims", F.lit(""))
    .withColumn("source", F.lit("companyfacts"))
)

# COMMAND ----------

# DBTITLE 1,silver_financial_facts
cols = ["cik", "accession", "form", "filed", "source", "concept", "unit", "dims",
        "period_start", "period_end", "value"]
facts = (
    instance_facts.select(*cols).unionByName(cf_facts.select(*cols))
    .filter(F.col("form").isin(PERIODIC_FORMS))
    .filter(F.col("value").isNotNull() & F.col("period_end").isNotNull())
    .withColumn("period_start", F.to_date("period_start"))
    .withColumn("period_end", F.to_date("period_end"))
    .withColumn("duration_days", F.datediff("period_end", "period_start"))
    # 3 / 6 / 9 / 12 for durations (52/53-week years round correctly), NULL for instants
    .withColumn("months", F.round(F.col("duration_days") / F.lit(30.44)).cast("int"))
    .dropDuplicates(["cik", "accession", "source", "concept", "unit", "dims", "period_start", "period_end"])
)
(facts.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .partitionBy("source").saveAsTable(S("silver_financial_facts")))

summary = spark.table(S("silver_financial_facts")).groupBy("source").count().collect()
print({r["source"]: r["count"] for r in summary}, f"from {n_docs} instance docs")

# COMMAND ----------

dbutils.notebook.exit(json.dumps({"status": "ok", "facts": {r["source"]: r["count"] for r in summary}}))
