"""
Pure-Python parsing helpers for SEC EDGAR content.

No Spark / Databricks imports — safe to call on the driver, inside a
``mapInPandas`` UDF, or in unit tests. Three jobs:

1. ``flatten_company_facts`` — explode the XBRL companyfacts JSON into typed
   fact rows. companyfacts is SEC's *pre-extracted* structured financial data,
   so we get ``silver_financial_facts`` without parsing raw XBRL instance docs.
2. ``parse_filing_html`` — strip a filing's primary HTML document to text and
   split it into sections on heading structure (10-K / 10-Q "Item" headings).
3. ``chunk_text`` — a dependency-free recursive character splitter matching the
   ``RecursiveCharacterTextSplitter(chunk_size=400, chunk_overlap=60)`` settings
   used in ``unstructured-data-2026/day2/03``.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# 1. XBRL companyfacts -> fact rows
# ---------------------------------------------------------------------------

def flatten_company_facts(facts: dict, cik: str) -> list[dict]:
    """Return one row per (concept, unit, period) observation.

    Shape of the input: ``facts["facts"][taxonomy][concept]["units"][unit]`` is
    a list of ``{start?, end, val, accn, fy, fp, form, frame?, filed}`` dicts.
    """
    out: list[dict] = []
    entity_name = facts.get("entityName")
    for taxonomy, concepts in (facts.get("facts") or {}).items():
        for concept, body in (concepts or {}).items():
            label = body.get("label")
            for unit, observations in (body.get("units") or {}).items():
                for ob in observations or []:
                    out.append(
                        {
                            "cik": str(cik).zfill(10),
                            "entity_name": entity_name,
                            "taxonomy": taxonomy,
                            "concept": concept,
                            "label": label,
                            "unit": unit,
                            "period_start": ob.get("start"),
                            "period_end": ob.get("end"),
                            "value": _to_float(ob.get("val")),
                            "accession": ob.get("accn"),
                            "fiscal_year": ob.get("fy"),
                            "fiscal_period": ob.get("fp"),
                            "form": ob.get("form"),
                            "frame": ob.get("frame"),
                            "filed": ob.get("filed"),
                        }
                    )
    return out


def _to_float(v):
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


# ---------------------------------------------------------------------------
# 2. Filing HTML -> sections
# ---------------------------------------------------------------------------

# "Item 1A.", "ITEM 7", "Item 8.01", "Item&nbsp;1B" etc. at the start of a line.
# Captures the sub-number for 8-K items (8.01) and the letter for 10-K items (1A).
_ITEM_RE = re.compile(
    r"^\s*item\s+(\d{1,2}(?:\.\d{1,2})?[a-z]?)[.:)\-–\s]*(.*)$",
    re.IGNORECASE,
)
# table-of-contents rows: "... Business .......... 4" or a trailing page number.
_TOC_RE = re.compile(r"\.{2,}\s*\d{1,4}\s*$|\t\d{1,4}\s*$")


def parse_filing_html(html: str, max_section_chars: int = 400_000) -> list[dict]:
    """Return ``[{"section": "Item 7", "heading": "...", "text": "..."}]``.

    Splits on 10-K / 10-Q / 8-K "Item" headings. Robust to the two things that
    wreck a naive split: the item name repeated as a running page header (many
    empty "Item 1" sections) and the table of contents (many "Item N" lines in
    quick succession). Fragments for the same item are merged; a repeated item
    keeps the occurrence with the most body.

    Uses BeautifulSoup with the stdlib ``html.parser`` backend; falls back to a
    regex strip if bs4 is absent.
    """
    text = _html_to_text(html)
    lines = [ln.rstrip() for ln in text.split("\n")]

    raw: list[dict] = []
    current = {"section": "PREAMBLE", "heading": "", "buf": []}

    def _flush():
        body = "\n".join(current["buf"]).strip()
        if body:
            raw.append(
                {"section": current["section"], "heading": current["heading"], "text": body}
            )

    for ln in lines:
        m = _ITEM_RE.match(ln)
        is_heading = bool(m) and len(ln) <= 120 and not _TOC_RE.search(ln)
        if is_heading:
            sec = f"Item {m.group(1).upper().rstrip('.')}"
            # running page header for the item we're already inside -> body line
            if sec == current["section"]:
                continue
            _flush()
            current = {"section": sec, "heading": m.group(2).strip()[:200], "buf": []}
        else:
            current["buf"].append(ln)
    _flush()

    # merge fragments per section id; for a repeated id keep the richest heading
    merged: dict[str, dict] = {}
    order: list[str] = []
    for s in raw:
        key = s["section"]
        if key not in merged:
            merged[key] = {"section": key, "heading": s["heading"], "text": s["text"]}
            order.append(key)
        else:
            cur = merged[key]
            cur["text"] = (cur["text"] + "\n\n" + s["text"])
            if len(s["text"]) > len(cur["text"]) - len(s["text"]) and s["heading"]:
                cur["heading"] = s["heading"]
    return [
        {**merged[k], "text": merged[k]["text"][:max_section_chars]} for k in order
    ]


def _html_to_text(html: str) -> str:
    try:
        from bs4 import BeautifulSoup

        soup = BeautifulSoup(html, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        raw = soup.get_text(separator="\n")
    except Exception:  # bs4 missing or parse blew up
        raw = re.sub(
            r"<(script|style|noscript)\b[^>]*>.*?</\1>", " ", html,
            flags=re.DOTALL | re.IGNORECASE,
        )
        raw = re.sub(r"<[^>]+>", "\n", raw)
        raw = (
            raw.replace("&nbsp;", " ")
            .replace("&amp;", "&")
            .replace("&#160;", " ")
        )
    # collapse runs of blank lines / trailing spaces
    raw = re.sub(r"[ \t]+\n", "\n", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


# ---------------------------------------------------------------------------
# 2b. Full-submission .txt -> per-document metadata (exhibits etc.)
# ---------------------------------------------------------------------------

_DOC_BLOCK_RE = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.DOTALL | re.IGNORECASE)


def _tag(block: str, name: str) -> str | None:
    m = re.search(rf"<{name}>([^\n<]*)", block, re.IGNORECASE)
    return m.group(1).strip() if m else None


def parse_submission_documents(submission_txt: str) -> list[dict]:
    """Parse the ``<DOCUMENT>`` blocks of a full submission .txt into a list of
    ``{sequence, doc_type, filename, description}`` — the filing's exhibit
    manifest. This is the semi-structured (SGML) slice of the Variety story."""
    out: list[dict] = []
    for block in _DOC_BLOCK_RE.findall(submission_txt or ""):
        out.append(
            {
                "sequence": _tag(block, "SEQUENCE"),
                "doc_type": _tag(block, "TYPE"),
                "filename": _tag(block, "FILENAME"),
                "description": _tag(block, "DESCRIPTION"),
            }
        )
    return out


# ---------------------------------------------------------------------------
# 3. Recursive character chunking (dependency-free)
# ---------------------------------------------------------------------------

_SEPARATORS = ["\n\n", "\n", ". ", " ", ""]


def chunk_text(
    text: str, chunk_size: int = 400, chunk_overlap: int = 60
) -> list[str]:
    """Split ``text`` into ~``chunk_size``-char chunks with ``chunk_overlap``
    char overlap, preferring paragraph > line > sentence > word boundaries."""
    text = (text or "").strip()
    if not text:
        return []
    if len(text) <= chunk_size:
        return [text]

    pieces = _split_recursive(text, chunk_size, _SEPARATORS)
    return _merge_with_overlap(pieces, chunk_size, chunk_overlap)


def _split_recursive(text: str, chunk_size: int, separators: list[str]) -> list[str]:
    sep = separators[0]
    rest = separators[1:]
    if sep == "":
        return [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
    parts = text.split(sep)
    out: list[str] = []
    for part in parts:
        piece = part + sep
        if len(piece) <= chunk_size:
            out.append(piece)
        elif rest:
            out.extend(_split_recursive(part, chunk_size, rest))
        else:
            out.extend(
                piece[i : i + chunk_size] for i in range(0, len(piece), chunk_size)
            )
    return [p for p in out if p.strip()]


def _merge_with_overlap(
    pieces: list[str], chunk_size: int, chunk_overlap: int
) -> list[str]:
    chunks: list[str] = []
    cur = ""
    for piece in pieces:
        if cur and len(cur) + len(piece) > chunk_size:
            chunks.append(cur.strip())
            cur = (cur[-chunk_overlap:] if chunk_overlap else "") + piece
        else:
            cur += piece
    if cur.strip():
        chunks.append(cur.strip())
    return chunks
