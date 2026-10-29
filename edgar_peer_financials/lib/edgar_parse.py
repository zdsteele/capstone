"""
Pure-Python EDGAR parsers (stdlib + optional bs4). No Spark / Databricks imports,
so the same functions run on the driver, inside ``mapInPandas`` on executors,
and in local tests.

Trimmed from the capstone's ``lib/edgar_parse.py`` to what the peer-financials
pipeline needs, plus an XBRL-instance parser: companyfacts only carries
standard-taxonomy, non-dimensional facts, so company extension concepts
(``slb:…``) and share-class-dimensioned EPS (WHD, SEI) are only reachable from
the filing's own instance document.
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
# 2. Full-submission .txt -> per-document manifest + document bodies
# ---------------------------------------------------------------------------

_DOC_BLOCK_RE = re.compile(r"<DOCUMENT>(.*?)</DOCUMENT>", re.DOTALL | re.IGNORECASE)
_TEXT_RE = re.compile(r"<TEXT>\s*(.*?)\s*</TEXT>", re.DOTALL | re.IGNORECASE)


def _tag(block: str, name: str) -> str | None:
    m = re.search(rf"<{name}>([^\n<]*)", block, re.IGNORECASE)
    return m.group(1).strip() if m else None


def parse_submission_documents(submission_txt: str) -> list[dict]:
    """Parse the ``<DOCUMENT>`` blocks of a full submission .txt into a list of
    ``{sequence, doc_type, filename, description}`` — the filing's manifest."""
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


def extract_submission_document(
    submission_txt: str, doc_type: str | None = None, filename_suffix: str | None = None
) -> tuple[str, str] | None:
    """Return ``(filename, body)`` of the first ``<DOCUMENT>`` whose TYPE equals
    ``doc_type`` (case-insensitive) or whose FILENAME ends with
    ``filename_suffix``. Body is the content between ``<TEXT>`` tags, with any
    ``<XML>``/``<XBRL>`` wrapper left for the caller's parser to skip."""
    for block in _DOC_BLOCK_RE.findall(submission_txt or ""):
        typ = (_tag(block, "TYPE") or "").upper()
        fname = _tag(block, "FILENAME") or ""
        if (doc_type and typ == doc_type.upper()) or (
            filename_suffix and fname.lower().endswith(filename_suffix.lower())
        ):
            m = _TEXT_RE.search(block)
            return fname, (m.group(1) if m else "")
    return None


def extract_xbrl_instance(submission_txt: str) -> tuple[str, str] | None:
    """The XBRL instance of a 10-K/10-Q: ``*_htm.xml`` for inline-XBRL filings
    (EDGAR's extracted instance), ``EX-101.INS`` for older ones."""
    return extract_submission_document(
        submission_txt, filename_suffix="_htm.xml"
    ) or extract_submission_document(submission_txt, doc_type="EX-101.INS")


def html_to_text(html: str) -> str:
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
        import html as _html

        raw = _html.unescape(raw).replace("\xa0", " ")
    # collapse runs of blank lines / trailing spaces
    raw = re.sub(r"[ \t]+\n", "\n", raw)
    raw = re.sub(r"\n{3,}", "\n\n", raw)
    return raw.strip()


# ---------------------------------------------------------------------------
# 3. XBRL instance document -> numeric fact rows (incl. extensions + dimensions)
# ---------------------------------------------------------------------------

_XBRLI = "http://www.xbrl.org/2003/instance"
_XBRLDI = "http://xbrl.org/2006/xbrldi"


def _local(tag: str) -> str:
    return tag.rsplit("}", 1)[-1]


def _qname(name: str, uri_to_prefix: dict) -> str:
    """``{uri}Local`` -> ``prefix:Local`` using the document's own prefixes."""
    if name.startswith("{"):
        uri, local = name[1:].split("}", 1)
        return f"{uri_to_prefix.get(uri, uri)}:{local}"
    return name


def parse_xbrl_instance(xml: str) -> list[dict]:
    """Parse an XBRL instance into one row per numeric fact:
    ``{concept, unit, period_start, period_end, value, decimals, dims}``.

    - ``concept`` is ``prefix:LocalName`` (``us-gaap:Revenues``, ``slb:…``).
    - ``period_start`` is ``None`` for instant facts.
    - ``dims`` is ``""`` for the consolidated (no-segment) context, otherwise a
      sorted ``axis=member;axis=member`` string — callers keep ``dims == ""``
      unless a per-ticker override names a specific dimension.
    Non-numeric facts (text blocks, dates) and nil facts are dropped.
    Stdlib ElementTree only."""
    import io
    import xml.etree.ElementTree as ET

    # The submission .txt wraps the instance in <XML>…</XML> (or <XBRL>); strip it.
    xml = re.sub(r"^\s*<(XML|XBRL)>\s*|\s*</(XML|XBRL)>\s*$", "", xml or "")
    start = xml.find("<")
    if start < 0:
        return []
    xml = xml[start:]

    uri_to_prefix: dict[str, str] = {}
    try:
        for _event, (prefix, uri) in ET.iterparse(io.StringIO(xml), events=("start-ns",)):
            uri_to_prefix.setdefault(uri, prefix)
        root = ET.fromstring(xml)
    except ET.ParseError:
        return []

    contexts: dict[str, tuple] = {}
    for ctx in root.findall(f"{{{_XBRLI}}}context"):
        start_d = ctx.findtext(f"{{{_XBRLI}}}period/{{{_XBRLI}}}startDate")
        end_d = ctx.findtext(f"{{{_XBRLI}}}period/{{{_XBRLI}}}endDate")
        inst = ctx.findtext(f"{{{_XBRLI}}}period/{{{_XBRLI}}}instant")
        members = []
        for seg_path in (f"{{{_XBRLI}}}entity/{{{_XBRLI}}}segment", f"{{{_XBRLI}}}scenario"):
            seg = ctx.find(seg_path)
            if seg is None:
                continue
            for mem in seg:
                axis = mem.get("dimension") or ""
                val = (mem.text or "").strip()
                if not val and len(mem):
                    val = _local(mem[0].tag)
                members.append(f"{axis}={val}")
        contexts[ctx.get("id")] = (
            (start_d or "").strip() or None,
            (end_d or inst or "").strip() or None,
            ";".join(sorted(members)),
        )

    units: dict[str, str] = {}
    for u in root.findall(f"{{{_XBRLI}}}unit"):
        measures = [(m.text or "").split(":")[-1] for m in u.iter(f"{{{_XBRLI}}}measure")]
        units[u.get("id")] = "/".join(measures) if len(measures) > 1 else (measures[0] if measures else "")

    out: list[dict] = []
    seen = set()
    for el in root:
        ctx_ref = el.get("contextRef")
        unit_ref = el.get("unitRef")
        if ctx_ref is None or unit_ref is None:      # non-numeric / not a fact
            continue
        if el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            continue
        value = _to_float((el.text or "").strip())
        if value is None or ctx_ref not in contexts:
            continue
        p_start, p_end, dims = contexts[ctx_ref]
        concept = _qname(el.tag, uri_to_prefix)
        unit = units.get(unit_ref, unit_ref)
        key = (concept, unit, p_start, p_end, dims)
        if key in seen:                              # iXBRL repeats facts verbatim
            continue
        seen.add(key)
        out.append(
            {
                "concept": concept,
                "unit": unit,
                "period_start": p_start,
                "period_end": p_end,
                "value": value,
                "decimals": el.get("decimals"),
                "dims": dims,
            }
        )
    return out
