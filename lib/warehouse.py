"""
Read helper for Unity Catalog Delta tables via a SQL warehouse.

The app + the agent's retrieval tools read the Gold/Silver marts straight from
Delta (`bootcamp_students.zdsteele.*`) through the Serverless Starter Warehouse
(`b15d3d6f837ba428`) using ``databricks-sql-connector``. This replaces the
forward Synced Tables entirely — nothing to hand-create in the Lakebase UI.

Auth comes from the ambient Databricks identity (CLI profile locally; the app
service principal when deployed) via the SDK's ``Config.authenticate``.

Parameters use positional ``?`` placeholders (universally supported by the
connector); pass a tuple/list.
"""

from __future__ import annotations

import datetime as _dt
import json
import math
import os
import threading
from decimal import Decimal

CATALOG = os.environ.get("UC_CATALOG", "bootcamp_students")
SCHEMA = os.environ.get("UC_SCHEMA", "zdsteele_capstone")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "b15d3d6f837ba428")
HTTP_PATH = os.environ.get("DATABRICKS_HTTP_PATH", f"/sql/1.0/warehouses/{WAREHOUSE_ID}")

_local = threading.local()


def table(name: str) -> str:
    """Fully-qualified name for a table in the capstone UC schema."""
    return f"{CATALOG}.{SCHEMA}.{name}"


def _connection():
    conn = getattr(_local, "conn", None)
    if conn is not None:
        try:
            conn.cursor().close()
            return conn
        except Exception:
            conn = None

    from databricks import sql as dbsql
    from databricks.sdk.core import Config

    cfg = Config()  # picks up DATABRICKS_* env / CLI profile / app SP
    host = cfg.host.replace("https://", "").replace("http://", "").rstrip("/")

    conn = dbsql.connect(
        server_hostname=host,
        http_path=HTTP_PATH,
        credentials_provider=lambda: cfg.authenticate,
    )
    _local.conn = conn
    return conn


def _clean(v):
    """Make a warehouse value JSON-safe: NaN/Inf -> None, Decimal -> float,
    date/datetime -> isoformat, and ARRAY/STRUCT/MAP columns (which the connector
    hands back as JSON strings when pyarrow is absent) -> parsed Python objects."""
    if isinstance(v, float):
        return v if math.isfinite(v) else None
    if isinstance(v, Decimal):
        f = float(v)
        return f if math.isfinite(f) else None
    if isinstance(v, (_dt.date, _dt.datetime)):
        return v.isoformat()
    if isinstance(v, str) and len(v) >= 2 and v[0] in "[{" and v[-1] in "]}":
        try:
            return json.loads(v)
        except Exception:
            return v
    return v


def query(sql: str, params: tuple | list | None = None) -> list[dict]:
    """Run a read query against the warehouse; return rows as JSON-safe list[dict]."""
    conn = _connection()
    with conn.cursor() as cur:
        cur.execute(sql, params or [])
        cols = [c[0] for c in cur.description] if cur.description else []
        return [
            {c: _clean(v) for c, v in zip(cols, row)} for row in cur.fetchall()
        ]
