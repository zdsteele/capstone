# RAG review — chunking, embedding, retrieval

Reviewed our filing-text search against the bootcamp modules that teach it:
`unstructured-data-2026/day2` (`02_chunking_strategies`, `03_vector_search`,
`05_embedding_strategies`) and `ai-agents-2026/day3/02b_retrieval_strategy_comparison`.

## Verdict

The **approach** was right; it was **under-tuned for SEC filings**. Fixed in the
commit that added this doc.

| Dimension | What the modules teach | Before | After |
|---|---|---|---|
| Split strategy | Recursive (Strategy 2) is the recommended default; **combine with structure-aware** (Strategy 3) — "headers give logical boundaries, recursive handles overflow" | ✅ Split on 10-K/10-Q/8-K **Item headings** first (`edgar_parse.parse_filing_html`), then recursive within each section | unchanged (already correct) |
| Chunk size | Examples: 300–600 chars prose, 600–800 code. "Chunking is the single highest-leverage decision." | ⚠️ **400 / 60** — ~65 tokens. Fragments a single risk-factor or MD&A point mid-thought | **1100 / 150** (~170 tokens) — one coherent point per chunk |
| Context enrichment | Strategy 9 (deterministic prefix) / Strategy 12 (LLM "Contextual Retrieval") — "~20–30% recall improvement" | ✅ `embed_text` = `"AAPL Apple Inc. — 10-K filed 2025-11-01 (period 2025-09-27)\nSection Item 1A — Risk Factors\n<chunk>"` — deterministic Strategy 9, scalable | unchanged (already correct) |
| Parent-child | Strategy 7 — "index small child chunks for precise retrieval, return large parent for rich context. For long documents where precise retrieval matters but context loss is a problem." | ❌ Agent got 5 × 600-char snippets, nothing more | ✅ `silver_filing_chunks_enriched.parent_text` = the **full section** (≤6k chars). `_vector_search` returns it; the agent quotes/reasons from the section, not the fragment |
| Read the whole thing | — (user: "the AI needs to be able to read all the information in these filings") | ❌ No tool returned full section text — `search_filing_text` = chunks, `get_filing` = headings only | ✅ New tool **`read_filing_section(accession, section)`** → full text of one Item (Item 1A / Item 7 / Item 8.01 …), up to 14k chars |
| Embedding model | `databricks-gte-large-en` | ✅ same | same |
| Retrieval mode | **Hybrid** (dense + BM25, RRF) — "GA-recommended default for most real-world RAG, especially technical docs" | ✅ `query_type: "hybrid"` | same |
| Index | `DELTA_SYNC`, `pipeline_type: TRIGGERED`, embed the enriched column | ✅ same | same, + a `_vector_search` retry that drops `parent_text` from the column list for an index built before the column existed |

## Have we done vector embedding?

Yes. `notebooks/05_vector_search_index.py` builds `silver_filing_chunks_enriched`
and a `DELTA_SYNC` Vector Search index `filing_text_index` on the shared
`zachy_vs` endpoint. Databricks embeds `embed_text` with `databricks-gte-large-en`
automatically and keeps the index synced from the Delta source. ~46k chunks
indexed today (will drop to ~20–25k after the 1100-char re-chunk, each carrying
more signal). The agent's `search_filing_text` queries it hybrid; it falls back
to a SQL `ILIKE` scan over `silver_filing_text_chunks` if the index is
unreachable.

## Strategies we deliberately did NOT use (and why)

- **Semantic / agentic chunking (Strategies 4, 10)** — an embedding or LLM call
  per sentence/boundary. 10-K sections already have hard structural boundaries
  (Items); the ROI over structure-aware + recursive is low at our scale and the
  cost scales badly to hundreds of CIKs.
- **LLM Contextual Retrieval (Strategy 12)** — one LLM call per chunk at index
  time. The deterministic prefix (Strategy 9) captures the same signal
  (company / form / period / section) for filings, at zero LLM cost. Worth
  revisiting if retrieval quality plateaus.
- **Late chunking (Strategy 11)** — needs an 8k-context embedding model;
  `gte-large-en` is 512 tokens.
- **Sentence-window (Strategy 6)** — parent-child (Strategy 7) covers the same
  "retrieve small, generate big" need and is simpler with the section as the
  natural parent.

## Follow-ups (not blocking)

- `notebooks/08_filing_intelligence.py` only feeds Items 1 / 1A / 2 / 7 / 7A to
  `ai_query` and truncates at 40k chars — it never sees Item 8 notes, Item 3
  legal proceedings, Item 5. Broaden `KEEP` / raise `max_chars` if the briefings
  feel thin.
- §16 of the analyst spec (filing-language diffing) is now *possible* via
  `read_filing_section` on two consecutive filings, but there's no automated
  period-over-period diff yet.
- Re-run `notebooks/03 → 05` is required for the new chunk size + `parent_text` +
  `section_index` column to take effect.
