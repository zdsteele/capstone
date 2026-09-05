# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 05 · Vector Search index on filing-text chunks
# MAGIC
# MAGIC Creates a `DELTA_SYNC` index over `silver_filing_text_chunks` so the agent's
# MAGIC `search_filing_text` tool can do hybrid (dense + BM25) retrieval. Pattern
# MAGIC lifted from `unstructured-data-2026/day2/03_vector_search.ipynb`.
# MAGIC
# MAGIC **Optional / guarded.** Default is keyword search — `search_filing_text`
# MAGIC runs `ILIKE` over `silver_filing_text_chunks` via the warehouse and this
# MAGIC notebook is skipped. To enable hybrid retrieval, set `vs_endpoint` to an
# MAGIC existing endpoint (e.g. `zachy_vs`) and run this, then set `VS_ENDPOINT` /
# MAGIC `VS_INDEX` in `app.yaml`.

# COMMAND ----------

import json
import time

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zachy_zacharysteele8")
dbutils.widgets.text("vs_endpoint", "")           # e.g. "zdsteele_vs" — blank to skip
dbutils.widgets.text("embedding_endpoint", "databricks-gte-large-en")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
VS_ENDPOINT = dbutils.widgets.get("vs_endpoint").strip()
EMB = dbutils.widgets.get("embedding_endpoint")

SOURCE_TABLE = f"{CATALOG}.{SCHEMA}.silver_filing_text_chunks"
INDEX_NAME = f"{CATALOG}.{SCHEMA}.filing_text_index"

if not VS_ENDPOINT:
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "no vs_endpoint; keyword fallback in app"}))

# COMMAND ----------

from databricks.sdk import WorkspaceClient

w = WorkspaceClient()

# Ensure the endpoint exists (STANDARD is fine for this scale)
endpoints = [e.name for e in w.vector_search_endpoints.list_endpoints()]
if VS_ENDPOINT not in endpoints:
    print(f"creating endpoint {VS_ENDPOINT} ...")
    w.vector_search_endpoints.create_endpoint(name=VS_ENDPOINT, endpoint_type="STANDARD")
    w.vector_search_endpoints.wait_get_endpoint_vector_search_endpoint_online(VS_ENDPOINT)

# COMMAND ----------

existing = [
    i["name"] for i in w.api_client.do("GET", f"/api/2.0/vector-search/endpoints/{VS_ENDPOINT}/indexes").get("vector_indexes", [])
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
                    {"name": "chunk_text", "embedding_model_endpoint_name": EMB}
                ],
            },
        },
    )
    print("index create requested")

# COMMAND ----------

# DBTITLE 1,Wait for ready
deadline = time.time() + 900
while time.time() < deadline:
    status = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{INDEX_NAME}")
    ready = status.get("status", {}).get("ready", False)
    print("ready:", ready, status.get("status", {}).get("message", ""))
    if ready:
        break
    time.sleep(15)

dbutils.notebook.exit(json.dumps({"status": "ok", "index": INDEX_NAME, "endpoint": VS_ENDPOINT}))
