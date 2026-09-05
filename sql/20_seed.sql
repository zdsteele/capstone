-- EDGAR Intelligence Platform — seed data for local dev / demo.
-- Run after 10_operational_tables.sql.

SET search_path TO bootcamp_students, public;

-- Demo user
INSERT INTO edgar_zdsteele_users (username, email)
VALUES ('zdsteele', 'zacharysteele8@gmail.com')
ON CONFLICT (username) DO NOTHING;

-- Pilot companies (kept in sync with config/ciks.json)
INSERT INTO edgar_zdsteele_companies (cik, ticker, name) VALUES
    ('0000320193', 'AAPL',  'Apple Inc.'),
    ('0000789019', 'MSFT',  'Microsoft Corporation'),
    ('0001018724', 'AMZN',  'Amazon.com, Inc.'),
    ('0001652044', 'GOOGL', 'Alphabet Inc.'),
    ('0001318605', 'TSLA',  'Tesla, Inc.')
ON CONFLICT (cik) DO UPDATE
    SET ticker = EXCLUDED.ticker,
        name   = EXCLUDED.name,
        updated_at = now();

-- A default watchlist for the demo user
INSERT INTO edgar_zdsteele_watchlists (user_id, name)
SELECT user_id, 'My Watchlist' FROM edgar_zdsteele_users WHERE username = 'zdsteele'
ON CONFLICT (user_id, name) DO NOTHING;
