-- EDGAR Intelligence Platform — operational tables in Lakebase (Postgres).
--
-- Instance: zdsteele-capstone  ·  schema: edgar
--
-- Every table gets REPLICA IDENTITY FULL so Lakebase logical replication emits
-- full before/after row images for the reverse-CDF analytics pipeline (lands as
-- bootcamp_students.zdsteele_capstone.lb_<table>_history in Unity Catalog).
--
-- Run after 00_create_schema.sql, connected as your own identity (REPLICA
-- IDENTITY is owner-only DDL).

SET search_path TO edgar, public;

-- ---------------------------------------------------------------------------
-- users — lightweight classroom-style identity (no password).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS users (
    user_id     BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    username    TEXT NOT NULL UNIQUE,
    email       TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE users REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- companies — small operational company dimension so ticker <-> CIK resolution
-- works before the Delta pipeline has run. Seeded from config/ciks.json;
-- silver_companies (read via the warehouse) is the source of truth once populated.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS companies (
    cik          TEXT PRIMARY KEY,          -- 10-digit zero-padded
    ticker       TEXT,
    name         TEXT NOT NULL,
    sic          TEXT,
    fiscal_year_end TEXT,
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE companies REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- watchlists / watchlist_companies
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS watchlists (
    watchlist_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(user_id),
    name         TEXT NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, name)
);
ALTER TABLE watchlists REPLICA IDENTITY FULL;

CREATE TABLE IF NOT EXISTS watchlist_companies (
    watchlist_id BIGINT NOT NULL REFERENCES watchlists(watchlist_id) ON DELETE CASCADE,
    cik          TEXT NOT NULL,
    ticker       TEXT,
    added_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (watchlist_id, cik)
);
ALTER TABLE watchlist_companies REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- saved_filings — bookmarked filings. filing_id = SEC accession number
-- (dashed form, e.g. 0000320193-26-000064).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saved_filings (
    saved_filing_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(user_id),
    company_cik  TEXT NOT NULL,
    filing_id    TEXT NOT NULL,
    form         TEXT,
    filed_at     DATE,
    note         TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (user_id, filing_id)
);
ALTER TABLE saved_filings REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- saved_research — free-form research notes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS saved_research (
    research_id   BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id       BIGINT NOT NULL REFERENCES users(user_id),
    company_cik   TEXT,
    filing_id     TEXT,
    title         TEXT NOT NULL,
    notes         TEXT,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE saved_research REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- agent_conversations — one row per chat session.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_conversations (
    conversation_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id      BIGINT NOT NULL REFERENCES users(user_id),
    title        TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_message_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    message_count INTEGER NOT NULL DEFAULT 0
);
ALTER TABLE agent_conversations REPLICA IDENTITY FULL;

-- ---------------------------------------------------------------------------
-- agent_actions — one row per tool call. Retrieval tools log SUCCESS/ERROR;
-- write tools start PENDING then flip; one 'answer' row per turn carries the
-- agent's self-reported confidence. CDF sink for gold_agent_tool_stats.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS agent_actions (
    action_id       BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    conversation_id BIGINT REFERENCES agent_conversations(conversation_id),
    user_id      BIGINT REFERENCES users(user_id),
    tool_name    TEXT NOT NULL,
    tool_kind    TEXT NOT NULL DEFAULT 'retrieval',   -- 'retrieval' | 'write' | 'answer'
    args_json    JSONB,
    status       TEXT NOT NULL DEFAULT 'PENDING',      -- 'PENDING' | 'SUCCESS' | 'ERROR'
    result_json  JSONB,
    error        TEXT,
    confidence   TEXT,                                 -- 'high' | 'medium' | 'low' (answer rows)
    latency_ms   INTEGER,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
ALTER TABLE agent_actions REPLICA IDENTITY FULL;

CREATE INDEX IF NOT EXISTS ix_agent_actions_conv        ON agent_actions(conversation_id);
CREATE INDEX IF NOT EXISTS ix_saved_filings_user        ON saved_filings(user_id);
CREATE INDEX IF NOT EXISTS ix_saved_research_user       ON saved_research(user_id);
CREATE INDEX IF NOT EXISTS ix_watchlist_companies_cik   ON watchlist_companies(cik);
