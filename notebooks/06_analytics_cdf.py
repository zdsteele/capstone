# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 · Analytics — reverse Change Data Feed
# MAGIC
# MAGIC Triggered when the `lb_*_history` tables receive changes (Asset Bundle job
# MAGIC `databricks.yml`). Incremental watermark pattern lifted from
# MAGIC `ltap-cdc-day-2/notebooks/day1_ingest/generate_repo_scd.py`:
# MAGIC
# MAGIC 1. read `_sort_by > last_processed AND _pg_change_type != 'update_preimage'`
# MAGIC    from each `lb_*_history` table,
# MAGIC 2. normalize new rows into the append-only `gold_usage_events`,
# MAGIC 3. rebuild the usage marts,
# MAGIC 4. advance `analytics_watermarks`.

# COMMAND ----------

import json

dbutils.widgets.text("catalog", "bootcamp_students")
dbutils.widgets.text("schema", "zdsteele_capstone")
CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
T = lambda n: f"{CATALOG}.{SCHEMA}.{n}"

from pyspark.sql import functions as F, Window

WATERMARK = T("analytics_watermarks")
EVENTS = T("gold_usage_events")

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {WATERMARK} (
        source_table STRING, last_processed_sort_by LONG, last_processed_at TIMESTAMP
    ) USING DELTA"""
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {EVENTS} (
        source_table STRING, pg_change_type STRING, sort_by LONG, event_ts TIMESTAMP,
        user_id LONG, event_type STRING, entity STRING, detail STRING, ingested_at TIMESTAMP
    ) USING DELTA"""
)

# COMMAND ----------

# DBTITLE 1,Per-source: read new CDF rows -> gold_usage_events
# source table -> (event_type, entity column expr, detail columns)
SOURCES = {
    "lb_agent_actions_history": dict(
        event_type_expr="concat('tool:', tool_name)",
        entity_expr="tool_name",
        detail_cols=["tool_kind", "status", "latency_ms", "args_json", "confidence"],
        ts_col="created_at",
    ),
    "lb_saved_filings_history": dict(
        event_type_expr="'save_filing'",
        entity_expr="filing_id",
        detail_cols=["company_cik", "form", "note"],
        ts_col="created_at",
    ),
    "lb_saved_research_history": dict(
        event_type_expr="'research_note'",
        entity_expr="cast(research_id as string)",
        detail_cols=["company_cik", "filing_id", "title"],
        ts_col="created_at",
    ),
    "lb_watchlist_companies_history": dict(
        event_type_expr="'watchlist_add'",
        entity_expr="cik",
        detail_cols=["ticker", "watchlist_id"],
        ts_col="added_at",
    ),
    "lb_watchlists_history": dict(
        event_type_expr="'watchlist_create'",
        entity_expr="cast(watchlist_id as string)",
        detail_cols=["name"],
        ts_col="created_at",
    ),
    "lb_agent_conversations_history": dict(
        event_type_expr="'conversation'",
        entity_expr="cast(conversation_id as string)",
        detail_cols=["title", "message_count"],
        ts_col="created_at",
    ),
}

processed = {}
for src, cfg in SOURCES.items():
    fq = T(src)
    if not spark.catalog.tableExists(fq):
        print(f"  (skip {src} — not synced yet)")
        continue
    last = spark.sql(
        f"SELECT COALESCE(MAX(last_processed_sort_by), -1) AS s FROM {WATERMARK} WHERE source_table = '{src}'"
    ).collect()[0]["s"]

    detail_struct = ", ".join(f"'{c}', cast({c} as string)" for c in cfg["detail_cols"])
    new_rows = spark.sql(
        f"""
        SELECT
            '{src}' AS source_table,
            _pg_change_type AS pg_change_type,
            _sort_by AS sort_by,
            cast({cfg['ts_col']} as timestamp) AS event_ts,
            try_cast(user_id as long) AS user_id,
            {cfg['event_type_expr']} AS event_type,
            {cfg['entity_expr']} AS entity,
            to_json(named_struct({detail_struct})) AS detail,
            current_timestamp() AS ingested_at
        FROM {fq}
        WHERE _sort_by > {last} AND _pg_change_type NOT IN ('update_preimage', 'delete')
        """
    )
    n = new_rows.count()
    if n:
        new_rows.write.format("delta").mode("append").saveAsTable(EVENTS)
        max_sb = new_rows.agg(F.max("sort_by")).collect()[0][0]
        spark.sql(
            f"""
            MERGE INTO {WATERMARK} t
            USING (SELECT '{src}' AS source_table, {max_sb} AS last_processed_sort_by,
                          current_timestamp() AS last_processed_at) s
            ON t.source_table = s.source_table
            WHEN MATCHED THEN UPDATE SET last_processed_sort_by = s.last_processed_sort_by,
                                        last_processed_at = s.last_processed_at
            WHEN NOT MATCHED THEN INSERT *
            """
        )
    processed[src] = n
    print(f"  {src}: +{n} events")

# COMMAND ----------

# DBTITLE 1,gold_agent_tool_stats + gold_agent_confidence  (latest row per action_id)
ACTIONS_HIST = T("lb_agent_actions_history")
if spark.catalog.tableExists(ACTIONS_HIST):
    latest_actions = (
        spark.table(ACTIONS_HIST)
        .withColumn(
            "rn",
            F.row_number().over(
                Window.partitionBy("action_id").orderBy(F.col("_sort_by").desc())
            ),
        )
        .filter((F.col("rn") == 1) & (~F.col("_pg_change_type").isin("delete", "update_preimage")))
    )
    tool_stats = (
        latest_actions.filter(F.col("tool_kind") != "answer")
        .groupBy("tool_name")
        .agg(
            F.count("*").alias("call_count"),
            F.sum(F.when(F.col("status") == "SUCCESS", 1).otherwise(0)).alias("success_count"),
            F.sum(F.when(F.col("status") == "ERROR", 1).otherwise(0)).alias("error_count"),
            F.round(F.avg("latency_ms"), 1).alias("avg_latency_ms"),
        )
        .withColumn(
            "success_rate",
            F.round(F.col("success_count") / F.greatest(F.col("call_count"), F.lit(1)), 3),
        )
    )
    tool_stats.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(T("gold_agent_tool_stats"))
    print("gold_agent_tool_stats:", tool_stats.count())

    # confidence distribution across answered turns (the Discord suggestion)
    conf = (
        latest_actions.filter(F.col("tool_kind") == "answer")
        .withColumn("confidence", F.coalesce(F.col("confidence"), F.lit("unstated")))
        .groupBy("confidence")
        .agg(F.count("*").alias("answer_count"))
    )
    conf.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(T("gold_agent_confidence"))
    print("gold_agent_confidence:", conf.count())

# COMMAND ----------

# DBTITLE 1,gold_watchlist_activity / gold_filing_view_stats / gold_metric_view_stats / gold_usage_funnel
events = spark.table(EVENTS)

events.filter(F.col("event_type") == "watchlist_add").groupBy(
    "entity", F.to_date("event_ts").alias("day")
).agg(F.count("*").alias("adds")).write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(T("gold_watchlist_activity"))

events.filter(F.col("event_type") == "save_filing").groupBy("entity").agg(
    F.count("*").alias("save_count"), F.max("event_ts").alias("last_saved_at")
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_filing_view_stats")
)

events.filter(F.col("event_type") == "tool:get_financial_metric").withColumn(
    "metric", F.get_json_object("detail", "$.args_json")
).groupBy("metric").agg(F.count("*").alias("request_count")).write.format("delta").mode(
    "overwrite"
).option("overwriteSchema", "true").saveAsTable(T("gold_metric_view_stats"))

events.groupBy("event_type").agg(
    F.count("*").alias("event_count"), F.countDistinct("user_id").alias("users")
).write.format("delta").mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    T("gold_usage_funnel")
)
print("usage marts rebuilt")

# COMMAND ----------

dbutils.notebook.exit(
    json.dumps({"status": "ok", "new_events": processed, "total_events": events.count()})
)
