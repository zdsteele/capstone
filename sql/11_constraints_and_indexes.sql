-- EDGAR Intelligence Platform — integrity constraints + indexes for the Lakebase
-- operational tables. Idempotent: safe to re-run.
--
-- Column additions are deliberately NOT done here: these tables replicate to
-- Delta through Lakebase CDF (lb_*_history), and changing their column set
-- would desync the history feed. Constraints and indexes don't change the
-- replicated row shape.

SET search_path TO edgar, public;

DO $$ BEGIN
    ALTER TABLE companies ADD CONSTRAINT ck_companies_cik
        CHECK (cik ~ '^[0-9]{10}$');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE users ADD CONSTRAINT ck_users_username
        CHECK (length(btrim(username)) > 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE watchlist_companies ADD CONSTRAINT ck_watchlist_companies_cik
        CHECK (cik ~ '^[0-9]{10}$');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE saved_filings ADD CONSTRAINT ck_saved_filings_cik
        CHECK (company_cik ~ '^[0-9]{10}$');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE saved_filings ADD CONSTRAINT ck_saved_filings_filing_id
        CHECK (filing_id ~ '^[0-9]{10}-[0-9]{2}-[0-9]{6}$');
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE agent_conversations ADD CONSTRAINT ck_agent_conversations_count
        CHECK (message_count >= 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE agent_actions ADD CONSTRAINT ck_agent_actions_status
        CHECK (status IN ('PENDING', 'SUCCESS', 'ERROR'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE agent_actions ADD CONSTRAINT ck_agent_actions_kind
        CHECK (tool_kind IN ('retrieval', 'write', 'answer'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE agent_actions ADD CONSTRAINT ck_agent_actions_confidence
        CHECK (confidence IS NULL OR confidence IN ('high', 'medium', 'low'));
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

DO $$ BEGIN
    ALTER TABLE agent_actions ADD CONSTRAINT ck_agent_actions_latency
        CHECK (latency_ms IS NULL OR latency_ms >= 0);
EXCEPTION WHEN duplicate_object THEN NULL; END $$;

CREATE INDEX IF NOT EXISTS ix_agent_actions_user_created
    ON agent_actions(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS ix_agent_actions_tool_status
    ON agent_actions(tool_name, status);
CREATE INDEX IF NOT EXISTS ix_agent_conversations_user
    ON agent_conversations(user_id, last_message_at DESC);
