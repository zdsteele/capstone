"""
Lakebase (Databricks-managed Postgres) connection helper.

The 2026 Lakebase uses **short-lived OAuth tokens** as the Postgres password
(no static password, no base64 secret). This helper resolves a connection URL
in priority order:

1. ``LAKEBASE_URL`` env — a full ``postgresql://…`` string. For local dev: click
   "Connect" in the Lakebase UI, copy the connection string, put the OAuth token
   in as the password (valid ~1 hour per dev session).
2. ``PGHOST`` / ``PGUSER`` / ``PGDATABASE`` / ``PGPORT`` (+ optional
   ``PGPASSWORD``) env — injected by Databricks Apps when the Lakebase instance
   is bound as a resource. If ``PGPASSWORD`` is absent, a fresh credential is
   minted from the ambient Databricks identity.

All capstone Postgres objects live in schema ``edgar`` on the dedicated
``zdsteele-capstone`` Lakebase instance (its own project, so nothing else
replicates to Delta). Gold/Silver reads do **not** go here — see ``lib.warehouse``.
"""

from __future__ import annotations

import os
import uuid
from contextlib import contextmanager
from urllib.parse import quote

import psycopg2
from psycopg2.extras import RealDictCursor

SCHEMA = os.environ.get("LAKEBASE_SCHEMA", "edgar")
INSTANCE = os.environ.get("LAKEBASE_INSTANCE", "zdsteele-capstone")


def _mint_password(host: str, user: str) -> str:
    """Mint a short-lived Lakebase credential from the ambient Databricks identity."""
    from databricks.sdk import WorkspaceClient

    w = WorkspaceClient()
    # Newer SDKs: dedicated database-credential endpoint.
    try:
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()), instance_names=[INSTANCE]
        )
        if getattr(cred, "token", None):
            return cred.token
    except Exception:
        pass
    # Fallback: the workspace OAuth token doubles as the PG password.
    headers = w.config.authenticate()  # {"Authorization": "Bearer <token>"}
    return headers["Authorization"].split(" ", 1)[1]


def _url() -> str:
    direct = os.environ.get("LAKEBASE_URL")
    if direct:
        return direct

    host = os.environ.get("PGHOST")
    if not host:
        raise RuntimeError(
            "No Lakebase connection info. Set LAKEBASE_URL (local dev) or run "
            "inside a Databricks App with the Lakebase instance bound (PGHOST…)."
        )
    user = os.environ.get("PGUSER", "users")
    db = os.environ.get("PGDATABASE", "databricks_postgres")
    port = os.environ.get("PGPORT", "5432")
    pwd = os.environ.get("PGPASSWORD") or _mint_password(host, user)
    # The Lakebase role is an email on the zdsteele-capstone instance, so the
    # '@' (and any token special chars) must be percent-encoded in the URL.
    return (
        f"postgresql://{quote(user, safe='')}:{quote(pwd, safe='')}"
        f"@{host}:{port}/{db}?sslmode=require"
    )


@contextmanager
def get_connection():
    conn = psycopg2.connect(_url(), cursor_factory=RealDictCursor)
    try:
        with conn.cursor() as cur:
            cur.execute(f"SET search_path TO {SCHEMA}, public")
        conn.commit()
        yield conn
    finally:
        conn.close()


def run_query(sql: str, params=None) -> list[dict]:
    """Run a read query and return rows as list[dict]."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()


def run_write(sql: str, params=None) -> list[dict]:
    """Run INSERT/UPDATE/DELETE. Returns the RETURNING rows (or []). Commits."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows: list[dict] = []
            if cur.description is not None:
                rows = cur.fetchall()
            conn.commit()
            return rows
