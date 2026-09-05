-- EDGAR Intelligence Platform — Lakebase schema.
--
-- Runs on the dedicated Lakebase instance `zdsteele-capstone` (its own project,
-- scale-to-zero). Because the instance is private to this capstone, tables use
-- a clean `edgar` schema with unprefixed names — nothing else lives here to
-- collide with, and only these tables get replicated to Delta.

CREATE SCHEMA IF NOT EXISTS edgar;
