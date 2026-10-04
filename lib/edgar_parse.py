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
# 2c. Form 3 / 4 / 5 ownership XML -> insider transaction rows
# ---------------------------------------------------------------------------

# SEC transactionCode -> (label, is_open_market)
_TXN_CODES = {
    "P": ("open-market purchase", True),
    "S": ("open-market sale", True),
    "A": ("grant/award", False),
    "M": ("option exercise", False),
    "F": ("tax withholding", False),
    "G": ("gift", False),
    "C": ("conversion", False),
    "X": ("option exercise", False),
    "D": ("disposition to issuer", False),
}


def _xt(node, path: str) -> str | None:
    """Text at `path` (supports '/value' children the ownership schema uses)."""
    el = node.find(path) if node is not None else None
    if el is None:
        return None
    return (el.text or "").strip() or None


def parse_form4(xml: str) -> dict | None:
    """Parse a Form 3/4/5 `ownershipDocument` into
    ``{issuer_cik, issuer_ticker, owner_name, owner_cik, is_director, is_officer,
    officer_title, is_ten_pct_owner, transactions: [...]}``. Each transaction:
    ``{date, code, code_label, open_market, acquired_disposed, shares, price,
    value, shares_owned_after, security}``. Returns ``None`` if it isn't parseable
    ownership XML. Stdlib ElementTree only."""
    import xml.etree.ElementTree as ET

    try:
        # strip a stray leading XML declaration / SGML wrapper if present
        start = xml.find("<ownershipDocument")
        root = ET.fromstring(xml[start:] if start >= 0 else xml)
    except Exception:
        return None
    if root.tag != "ownershipDocument":
        return None

    rel = root.find("reportingOwner/reportingOwnerRelationship")
    out = {
        "issuer_cik": _xt(root, "issuer/issuerCik"),
        "issuer_ticker": _xt(root, "issuer/issuerTradingSymbol"),
        "owner_name": _xt(root, "reportingOwner/reportingOwnerId/rptOwnerName"),
        "owner_cik": _xt(root, "reportingOwner/reportingOwnerId/rptOwnerCik"),
        "is_director": _xt(rel, "isDirector") in ("1", "true"),
        "is_officer": _xt(rel, "isOfficer") in ("1", "true"),
        "is_ten_pct_owner": _xt(rel, "isTenPercentOwner") in ("1", "true"),
        "officer_title": _xt(rel, "officerTitle"),
        "transactions": [],
    }
    for tbl in ("nonDerivativeTable", "derivativeTable"):
        table = root.find(tbl)
        if table is None:
            continue
        for txn in list(table):
            if not txn.tag.endswith("Transaction"):
                continue
            code = _xt(txn, "transactionCoding/transactionCode")
            label, open_mkt = _TXN_CODES.get(code or "", ("other", False))
            shares = _to_float(_xt(txn, "transactionAmounts/transactionShares/value"))
            price = _to_float(_xt(txn, "transactionAmounts/transactionPricePerShare/value"))
            out["transactions"].append({
                "date": _xt(txn, "transactionDate/value"),
                "code": code,
                "code_label": label,
                "open_market": open_mkt,
                "acquired_disposed": _xt(
                    txn, "transactionAmounts/transactionAcquiredDisposedCode/value"),
                "shares": shares,
                "price": price,
                "value": (shares * price) if (shares is not None and price is not None) else None,
                "shares_owned_after": _to_float(
                    _xt(txn, "postTransactionAmounts/sharesOwnedFollowingTransaction/value")),
                "security": _xt(txn, "securityTitle/value"),
                "derivative": tbl == "derivativeTable",
            })
    return out


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
# 2d. Form 13F information table XML -> holding rows
# ---------------------------------------------------------------------------

def _local(tag: str) -> str:
    """Strip a namespace prefix: '{http://...}infoTable' -> 'infoTable'."""
    return tag.rsplit("}", 1)[-1]


def parse_13f_infotable(xml: str) -> list[dict] | None:
    """Parse a Form 13F **information table** XML (the holdings detail, not the
    cover-page primary_doc.xml) into one dict per ``infoTable`` entry:
    ``{name_of_issuer, title_of_class, cusip, value, shares_or_prn_amt,
    shares_type, investment_discretion, voting_sole, voting_shared,
    voting_none, put_call}``. ``value`` is already in whole USD, as filed in
    the modern XML information table (the legacy pre-XML text-format "report
    in thousands, omit trailing zeros" instruction does NOT apply here —
    confirmed against real data: Berkshire's filed AAPL ``value`` divided by
    its filed share count lands within a few dollars of the real AAPL price
    for that quarter; multiplying by 1000 first was tried and produced a
    position worth more than Apple's entire market cap). Namespace-agnostic (filer-
    generated XML varies in whether/how a default namespace is declared) —
    matches on the local tag name only. Returns ``None`` if it isn't parseable
    13F infotable XML. Stdlib ElementTree only, same convention as
    ``parse_form4``."""
    import xml.etree.ElementTree as ET

    try:
        start = xml.find("<")
        root = ET.fromstring(xml[start:] if start >= 0 else xml)
    except Exception:
        return None
    if _local(root.tag) != "informationTable":
        return None

    def find1(node, name: str):
        for child in node:
            if _local(child.tag) == name:
                return child
        return None

    def text1(node, name: str) -> str | None:
        el = find1(node, name) if node is not None else None
        if el is None or el.text is None:
            return None
        return el.text.strip() or None

    rows: list[dict] = []
    for entry in root:
        if _local(entry.tag) != "infoTable":
            continue
        shares_node = find1(entry, "shrsOrPrnAmt")
        voting_node = find1(entry, "votingAuthority")
        rows.append({
            "name_of_issuer": text1(entry, "nameOfIssuer"),
            "title_of_class": text1(entry, "titleOfClass"),
            "cusip": text1(entry, "cusip"),
            "value": _to_float(text1(entry, "value")),
            "shares_or_prn_amt": _to_float(text1(shares_node, "sshPrnamt")),
            "shares_type": text1(shares_node, "sshPrnamtType"),
            "investment_discretion": text1(entry, "investmentDiscretion"),
            "voting_sole": _to_float(text1(voting_node, "Sole")),
            "voting_shared": _to_float(text1(voting_node, "Shared")),
            "voting_none": _to_float(text1(voting_node, "None")),
            "put_call": text1(entry, "putCall"),
        })
    return rows or None


# ---------------------------------------------------------------------------
# 2e. Company-name normalization (13F issuer names -> tracked-universe join key)
# ---------------------------------------------------------------------------

_NAME_SUFFIXES = [
    "SPON ADR", "DEPOSITARY", "DEPOSITORY", "HOLDINGS", "HLDGS", "COMMON",
    "CORPORATION", "INCORPORATED", "COMPANY", "GROUP", "GRP", "LIMITED",
    "CLASS A", "CLASS B", "CLASS C", "CL A", "CL B", "CL C",
    "INC", "CORP", "CO", "LTD", "LLC", "PLC", "LP", "DEL", "NEW", "COM",
]
_NAME_PUNCT_RE = re.compile(r"[.,'’\-&/\\]")
_NAME_WS_RE = re.compile(r"\s+")


def normalize_company_name(name: str | None) -> str:
    """Normalize a company name for fuzzy matching: uppercase, strip
    punctuation, collapse whitespace, then strip trailing legal-entity /
    share-class / domicile suffixes repeatedly (13F issuer names commonly
    carry several at once, e.g. 'APPLE INC COM' or 'COMCAST CORP CLASS A').
    Not a general-purpose normalizer — tuned specifically for matching 13F
    ``nameOfIssuer`` strings against ``silver_companies.name``."""
    if not name:
        return ""
    s = _NAME_PUNCT_RE.sub(" ", name.upper())
    s = _NAME_WS_RE.sub(" ", s).strip()
    changed = True
    while changed:
        changed = False
        for suf in _NAME_SUFFIXES:
            if s.endswith(" " + suf):
                s = s[: -(len(suf) + 1)].strip()
                changed = True
    return s


# ---------------------------------------------------------------------------
# 2f. Schedule 13D / 13G cover page -> beneficial-ownership stake
# ---------------------------------------------------------------------------

# The SEC cover-page template's actual Item 1 heading is the single combined
# phrase "Names of Reporting Persons, I.R.S. Identification Nos. of Above
# Persons" — a regex anchored on just "Name(s) of Reporting Person(s)" matches
# that, then captures the *next line*, which in filings using this combined
# heading is the "I.R.S. Identification..." continuation text, not a name
# (confirmed live against a real Apple 13D/13G). Try the full combined-heading
# pattern first; only fall back to the short pattern for filings that split
# the heading differently.
_FILER_NAME_RES = [
    re.compile(
        r"Names?\s+of\s+Reporting\s+Persons?[,.]?\s*\n*\s*"
        r"(?:S\.?S\.?\s*(?:OR|/)?\s*)?I\.?R\.?S\.?\s*Identification\s*"
        r"Nos?\.?\s*of\s*Above\s*Persons?\.?\s*(?:\([^)]*\))?\.?\s*\n+\s*([^\n]{2,120})",
        re.IGNORECASE,
    ),
    re.compile(
        r"(?:1\.?\s*)?Name[s]?\s+of\s+Reporting\s+Person[s]?\.?\s*\n?\s*([^\n]{2,120})",
        re.IGNORECASE,
    ),
    # Legacy plain-text summary format (e.g. FMR LLC's "filing.txt" filings):
    # "Item 1: Reporting Person - FMR LLC" on a single line, no cover-page table.
    re.compile(
        r"Item\s*1\s*[:.]?\s*Reporting\s*Person\s*[-:]\s*([^\n]{2,120})",
        re.IGNORECASE,
    ),
]

_13DG_LABEL_RES = {
    "cusip": re.compile(r"CUSIP\s*No\.?\s*[:#]?\s*([0-9A-Z]{6,9})", re.IGNORECASE),
    "pct_of_class": re.compile(
        r"Percent of Class Represented by Amount in Row\s*\(?\d*\)?\.?\s*\n?\s*([\d.]+)\s*%",
        re.IGNORECASE,
    ),
    "shares_beneficially_owned": re.compile(
        r"Aggregate Amount Beneficially Owned by Each Reporting Person\.?\s*\n?\s*([\d,]+)",
        re.IGNORECASE,
    ),
    "sole_voting_power": re.compile(r"Sole Voting Power\.?\s*\n?\s*([\d,]+)", re.IGNORECASE),
    "shared_voting_power": re.compile(r"Shared Voting Power\.?\s*\n?\s*([\d,]+)", re.IGNORECASE),
    "sole_dispositive_power": re.compile(
        r"Sole Dispositive Power\.?\s*\n?\s*([\d,]+)", re.IGNORECASE),
    "shared_dispositive_power": re.compile(
        r"Shared Dispositive Power\.?\s*\n?\s*([\d,]+)", re.IGNORECASE),
}


def _num(s: str | None) -> float | None:
    if not s:
        return None
    try:
        return float(s.replace(",", ""))
    except ValueError:
        return None


def parse_13dg_cover(doc: str, is_xml: bool = False) -> dict | None:
    """Best-effort parse of a Schedule 13D/13G cover page into
    ``{cusip, filer_name, pct_of_class, shares_beneficially_owned,
    sole_voting_power, shared_voting_power, sole_dispositive_power,
    shared_dispositive_power, parse_method}``. 13D/13G filings don't share one
    universal clean XML schema the way Form 4 does — tries a structured XML
    cover page first (``is_xml`` hint or an auto-detected ``<edgarSubmission``/
    ``coverPage`` root), then falls back to regex over the plain text pulled
    through ``_html_to_text`` (or raw text if already plain), anchored on the
    cover-page labels (Items 1-11 / 1-10) that are consistent across nearly
    all filers regardless of exact formatting. Returns ``None`` if no anchor
    at all is found (narrative-only filing with no parseable cover page) —
    caller skips, matching the ``parse_form4`` convention."""
    probe = doc[:2000]
    looks_xml = is_xml or bool(re.search(r"<(edgarSubmission|coverPage)[\s>]", probe, re.I))
    if looks_xml:
        try:
            import xml.etree.ElementTree as ET

            root = ET.fromstring(doc)

            def find_local(node, name):
                for el in node.iter():
                    if _local(el.tag).lower() == name.lower():
                        return el
                return None

            cusip_el = find_local(root, "cusip")
            name_el = find_local(root, "filerName") or find_local(root, "nameOfReportingPerson")
            pct_el = find_local(root, "aggregateAmountOwnedPercent") or find_local(root, "percentOfClass")
            shares_el = find_local(root, "aggregateAmountOwned")
            if cusip_el is not None or name_el is not None:
                return {
                    "cusip": (cusip_el.text or "").strip() if cusip_el is not None else None,
                    "filer_name": (name_el.text or "").strip() if name_el is not None else None,
                    "pct_of_class": _num(pct_el.text if pct_el is not None else None),
                    "shares_beneficially_owned": _num(shares_el.text if shares_el is not None else None),
                    "sole_voting_power": None,
                    "shared_voting_power": None,
                    "sole_dispositive_power": None,
                    "shared_dispositive_power": None,
                    "parse_method": "xml_cover_page",
                }
        except Exception:
            pass  # fall through to text regex

    text = _html_to_text(doc) if "<" in probe and not looks_xml else doc
    found = {}
    for field, rx in _13DG_LABEL_RES.items():
        m = rx.search(text)
        if m:
            found[field] = m.group(1).strip()
    for rx in _FILER_NAME_RES:
        m = rx.search(text)
        if m:
            candidate = m.group(1).strip()
            # The combined-heading pattern can still land on the I.R.S.
            # continuation line in filings whose layout doesn't quite match
            # either pattern cleanly — reject an obviously-boilerplate
            # candidate instead of reporting it as a "name".
            cand_upper = candidate.upper()
            if ("IDENTIFICATION" not in cand_upper and "I.R.S" not in cand_upper
                    and cand_upper not in ("S.S.", "S.S", "SS.")
                    and re.search(r"[A-Za-z]{2,}", candidate)):
                # Some filed HTML hard-wraps a single name across two <P>
                # lines (e.g. "Jeffrey P.\nBezos", "Amazon.com,\nInc.") even
                # though it's one logical field — join a short, plausibly-a-
                # name-continuation next line rather than truncating.
                tail = text[m.end(1):].lstrip()
                nxt_line = tail.split("\n", 1)[0].strip()
                if (nxt_line and len(nxt_line) <= 40
                        and not nxt_line.isupper()
                        and not re.match(r"^Item\s*\d", nxt_line, re.IGNORECASE)
                        and re.match(r"^[A-Z][a-z]", nxt_line)):
                    candidate = f"{candidate} {nxt_line}"
                # The combined-heading pattern sometimes captures a trailing
                # EIN that shares the same line as the name (e.g. "The
                # Vanguard Group - 23-1945930") — strip it, it's not part of
                # the name.
                candidate = re.sub(r"\s*[-–]\s*\d{2}-\d{7}\s*$", "", candidate).strip()
                candidate = re.sub(r"\s+", " ", candidate.replace("\xa0", " ")).strip()
                found["filer_name"] = candidate
                break
    if not found:
        return None
    return {
        "cusip": found.get("cusip"),
        "filer_name": found.get("filer_name"),
        "pct_of_class": _num(found.get("pct_of_class")),
        "shares_beneficially_owned": _num(found.get("shares_beneficially_owned")),
        "sole_voting_power": _num(found.get("sole_voting_power")),
        "shared_voting_power": _num(found.get("shared_voting_power")),
        "sole_dispositive_power": _num(found.get("sole_dispositive_power")),
        "shared_dispositive_power": _num(found.get("shared_dispositive_power")),
        "parse_method": "text_regex",
    }


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
