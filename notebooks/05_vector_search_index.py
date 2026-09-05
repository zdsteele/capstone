# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 · Vector Search — semantic retrieval over filing text
# MAGIC
# MAGIC 1. Build `silver_filing_chunks_enriched`: each chunk prefixed with its
# MAGIC    company / form / period / section (context-enriched embedding — day-2
# MAGIC    "contextual retrieval" idea, done deterministically so it scales).
# MAGIC 2. Create a `DELTA_SYNC` Vector Search index on the `embed_text` column
# MAGIC    (`databricks-gte-large-en`), queried with **hybrid** (dense + BM25) in
# MAGIC    the agent's `search_filing_text` tool.
# MAGIC
# MAGIC Set `vs_endpoint` (e.g. `zachy_vs`). Then set `VS_ENDPOINT` / `VS_INDEX` in
# MAGIC `app.yaml` + `.env` so the app/agent use hybrid search instead of keyword.

# COMMAND ----------

import json
import time

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
dbutils.widgets.text("vs_endpoint", "zachy_vs")
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint").strip()
EMB = dbutils.widgets.get("embedding_endpoint")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

SOURCE_TABLE = T("silver_filing_chunks_enriched")
INDEX_NAME = T("filing_text_index")

# COMMAND ----------

# DBTITLE 1,Build the context-enriched chunk table
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SOURCE_TABLE}
    TBLPROPERTIES (delta.enableChangeDataFeed = true) AS
    SELECT
        k.chunk_id, k.cik, k.accession, k.section, k.heading, k.chunk_index,
        k.chunk_text,
        c.ticker, c.name AS company_name, f.form, f.filing_date, f.report_date,
        concat_ws(
            '\n',
            concat(coalesce(c.ticker, ''), ' ', coalesce(c.name, ''),
                   ' — ', coalesce(f.form, ''), ' filed ', coalesce(cast(f.filing_date as string), ''),
                   ' (period ', coalesce(cast(f.report_date as string), 'n/a'), ')'),
            concat('Section ', coalesce(k.section, ''),
                   CASE WHEN k.heading IS NOT NULL AND k.heading <> '' THEN concat(' — ', k.heading) ELSE '' END),
            k.chunk_text
        ) AS embed_text
    FROM {T('silver_filing_text_chunks')} k
    LEFT JOIN {T('silver_filings')}   f ON f.accession = k.accession
    LEFT JOIN {T('silver_companies')} c ON c.cik = k.cik
    """
)
n = spark.table(SOURCE_TABLE).count()
print(f"silver_filing_chunks_enriched: {n:,} rows")

if not VS_ENDPOINT:
    dbutils.notebook.exit(json.dumps({"status": "chunks_enriched_only", "rows": n}))

# COMMAND ----------

# DBTITLE 1,Create / refresh the Vector Search index
from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

endpoints = [e.name for e in w.vector_search_endpoints.list_endpoints()]
if VS_ENDPOINT not in endpoints:
    print(f"creating endpoint {VS_ENDPOINT} ...")
    w.vector_search_endpoints.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(VS_ENDPOINT)

existing = [
    i.get("name")
    for i in w.api_client.do(
        "GET", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}/indexes"
    ).get("vector_indexes", [])
]
if INDEX_NAME not in existing:
    w.api_client.do(
        "POST",
        "/api/2.0/vector-search/indexes",
        body={
            "name": INDEX_NAME,
            "endpoint_name": VS_ENDPOINT,
            "primary_key": "chunk_id",
            "index_type": "DELTA_SYNC",
            "delta_sync_index_spec": {
                "source_table": SOURCE_TABLE,
                "pipeline_type": "TRIGGERED",
                "embedding_source_columns": [
                    {"name": "embed_text", "embedding_model_endpoint_name": EMB}
                ],
            },
        },
    )
    print("index create requested")
else:
    w.api_client.do("POST", f"/api/2.0/vector-search/indexes/{INDEX_NAME}/sync")
    print("index sync triggered")

# COMMAND ----------

# DBTITLE 1,Wait for ready
deadline = time.time() + 1200
while time.time() < deadline:
    status = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{INDEX_NAME}").get("status", {})
    print("ready:", status.get("ready"), status.get("message", ""))
    if status.get("ready"):
        break
    time.sleep(20)

# COMMAND ----------

# DBTITLE 1,Smoke test — hybrid query
try:
    res = w.api_client.do(
        "POST",
        f"/api/2.0/vector-search/indexes/{INDEX_NAME}/query",
        body={
            "columns": ["chunk_id", "ticker", "form", "section", "chunk_text"],
            "query_text": "artificial intelligence capital expenditures and data center investment",
            "num_results": 3,
            "query_type": "hybrid",
        },
    )
    cols = [c["name"] for c in res.get("manifest", {}).get("columns", [])]
    for row in res.get("result", {}).get("data_array", []) or []:
        d = dict(zip(cols, row))
        print(f"  [{d.get('ticker')} {d.get('form')} {d.get('section')}] {str(d.get('chunk_text'))[:160]}")
except Exception as exc:
    print("smoke query failed (index may still be warming):", exc)

dbutils.notebook.exit(json.dumps({"status": "ok", "index": INDEX_NAME, "endpoint": VS_ENDPOINT, "rows": n}))
