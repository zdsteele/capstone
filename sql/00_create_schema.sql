-- EDGAR Intelligence Platform — Lakebase schema.
--
-- Per the bootcamp TA, the capstone uses the SHARED `bootcamp_students` schema
-- that already exists in the summer-bootcamp-2026-v2 Lakebase (do not make a
-- personal schema). It almost certainly already exists; this is a harmless
-- no-op if so. Every capstone table is prefixed `edgar_` so it can't collide
-- with another student's tables in the shared schema.

CREATE SCHEMA IF NOT EXISTS bootcamp_students;
