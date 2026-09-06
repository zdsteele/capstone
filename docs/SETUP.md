# Setup — from zero

Linear path to a running platform. The **only** click-in-the-UI step is §4
(start Lakebase Change Data Feed); everything else is CLI / paste-and-run.

Workspace values (this deployment):

| Thing | Value |
|---|---|
| Databricks workspace | `https://dbc-7b106152-caf3.cloud.databricks.com` |
| Unity Catalog schema | `bootcamp_students.zdsteele_capstone` (Delta medallion + AI tables) |
| SQL warehouse | `Serverless Starter Warehouse`, id `b15d3d6f837ba428` |
| Lakebase (Postgres) | autoscaling project `zdsteele-capstone`, branch `production`, schema `edgar` |
| App Postgres role | `edgar_app` (native password — not the 1-hour OAuth token) |
| LLM | serving endpoint `databricks-meta-llama-3-3-70b-instruct` |
| Vector Search | index `…zdsteele_capstone.filing_text_index` on endpoint `zachy_vs` |

---

## 1. Databricks CLI auth

Add a profile to `~/.databrickscfg` (M2M service principal — PATs are disabled
in this org):

```ini
[edgar]
host          = https://dbc-7b106152-caf3.cloud.databricks.com
auth_type     = oauth-m2m
client_id     = <service-principal-app-id>
client_secret = <service-principal-secret>
```

`databricks.yml` pins `workspace.host`, so `databricks bundle …` auto-selects
this profile by host. For non-bundle commands: `-p edgar` or
`$env:DATABRICKS_CONFIG_PROFILE = "edgar"`.

Verify: `databricks auth describe` → `oauth-m2m`.

## 2. Lakebase — schema + tables

In the `zdsteele-capstone` Lakebase project → **SQL Editor**, run in order:

1. `sql/00_create_schema.sql` — `CREATE SCHEMA edgar`
2. `sql/10_operational_tables.sql` — 8 tables, each `ALTER TABLE … REPLICA IDENTITY FULL`
3. `sql/20_seed.sql` — demo user + pilot companies + a default watchlist

Then create the native-password app role (once):

```sql
CREATE ROLE edgar_app LOGIN PASSWORD '<pick-one>';
GRANT USAGE ON SCHEMA edgar TO edgar_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA edgar TO edgar_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA edgar TO edgar_app;
ALTER DEFAULT PRIVILEGES IN SCHEMA edgar
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO edgar_app;
```

## 3. `.env` for local dev

```bash
cd capstone
python -m venv venv && . venv/Scripts/activate   # or bin/activate
pip install -r requirements.txt
cp .env.example .env
```

Fill `.env`:

- `LAKEBASE_URL=postgresql://edgar_app:<password>@ep-lingering-recipe-d1fitihp.database.us-west-2.cloud.databricks.com/databricks_postgres?sslmode=require`
- `DATABRICKS_HOST` / `DATABRICKS_CLIENT_ID` / `DATABRICKS_CLIENT_SECRET` — the same SP as the CLI profile (the warehouse + LLM calls use these)
- `UC_CATALOG=bootcamp_students`, `UC_SCHEMA=zdsteele_capstone`
- the rest are pre-filled in `.env.example`

## 4. Deploy the bundle

```bash
databricks bundle deploy -t dev      # jobs + app to your own workspace folder
```

This registers `pipeline_daily_refresh`, `analytics_cdf_on_change`, and the
`edgar-intelligence` App. `-t dev` pauses the schedule; `-t prod` arms it and
binds Lakebase to the App (needs `CAN_MANAGE` on the Postgres project — run
`-t prod` as the project owner).

## 5. Run the pipeline

```bash
databricks bundle run pipeline_daily_refresh -t dev
```

Task order: `01 bronze_sec` → `03 silver` → `{04 gold, 05 vector_search,
08 filing_intelligence, 12 filing_diffs}` → `09 ratios` → `11 valuation` →
`10 company_health`. `02 bronze_market` runs alongside (best-effort). First
full-universe run is a few hours (rate-limited SEC HTTP); it's resumable —
notebook 01 flushes to Delta every 25 companies, so re-running picks up where
it left off. See [SCALING.md](SCALING.md).

Verify Volume: `SELECT COUNT(*) FROM bootcamp_students.zdsteele_capstone.silver_financial_facts` — well over 1,000,000.

## 6. Start Lakebase Change Data Feed  **(the one UI step)**

In the `zdsteele-capstone` Lakebase project → **Change Data Feed** → Start, with
source schema `edgar`, target `bootcamp_students.zdsteele_capstone`. This creates
the `lb_<table>_history` Delta tables the analytics job reads. `REPLICA IDENTITY
FULL` (§2) is the source-side prerequisite. Details + status in
[ARCHITECTURE.md](ARCHITECTURE.md).

## 7. Run the app locally

```bash
python app.py            # http://localhost:8000
```

Sign in with any lowercase username (creates a row in `edgar.users`). Smoke
test:

- **Companies** → grid ranked by health score; click one → detail + dashboard link
- **Dashboard** → enter `AAPL` → health verdict, financials, ratios, valuation panel
- **Filing** → open a 10-Q → AI briefing + "what changed" card; open an 8-K → event card
- **Research Assistant** → *"What are the 5 least healthy companies and why?"*,
  then *"Save Microsoft's latest 10-K and add a note comparing Azure to Google
  Cloud"* → check `edgar.saved_filings` / `edgar.saved_research` / `edgar.agent_actions`
- Use the agent a few times, then `databricks bundle run analytics_cdf_on_change -t dev`
  → the Dashboard **Platform activity** tab fills in

## 8. Deploy to prod

```bash
databricks bundle deploy -t prod        # as the Lakebase project owner
databricks apps deploy edgar-intelligence --source-code-path <bundle files path>
```

`-t prod` arms the 06:30 schedule and binds Lakebase to the App with a rotated
credential. The App itself must then be *started* (the `apps deploy` above, or
the Deploy button on the app page) to get a live URL.
