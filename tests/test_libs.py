"""Plain-assert tests for the pure-Python helpers (run: python tests/test_libs.py).

Mirrors the ltap-cdc-day-2/test_dependency_parsers.py style — no pytest needed,
though `pytest tests/` also works.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from lib import edgar_parse as ep


def test_chunk_text_sizes():
    text = ("A sentence here. Another one follows. " * 40).strip()
    chunks = ep.chunk_text(text, 400, 60)
    assert len(chunks) > 1
    assert all(len(c) <= 520 for c in chunks)  # size + a little slack
    assert ep.chunk_text("tiny", 400, 60) == ["tiny"]
    assert ep.chunk_text("", 400, 60) == []


def test_flatten_company_facts():
    facts = {
        "entityName": "Apple Inc.",
        "facts": {
            "us-gaap": {
                "Revenues": {
                    "label": "Revenues",
                    "units": {
                        "USD": [
                            {"start": "2025-01-01", "end": "2025-03-31", "val": 100,
                             "accn": "0000320193-25-000001", "fy": 2025, "fp": "Q1",
                             "form": "10-Q", "filed": "2025-05-01"},
                            {"end": "2025-03-31", "val": "bad", "accn": "x",
                             "fy": 2025, "fp": "Q1", "form": "10-Q"},
                        ]
                    },
                }
            }
        },
    }
    rows = ep.flatten_company_facts(facts, "320193")
    assert len(rows) == 2
    assert rows[0]["cik"] == "0000320193"
    assert rows[0]["value"] == 100.0
    assert rows[1]["value"] is None
    assert rows[0]["accession"] == "0000320193-25-000001"


def test_parse_filing_html_sections():
    html = (
        "<html><body>"
        "<p>Item 1. Business</p><p>We design phones.</p>"
        "<p>Item 1A. Risk Factors</p><p>Markets fluctuate.</p>"
        "<p>Item 7. Management Discussion</p><p>Revenue rose.</p>"
        "<script>ignored()</script></body></html>"
    )
    secs = ep.parse_filing_html(html)
    names = {s["section"] for s in secs}
    assert {"Item 1", "Item 1A", "Item 7"} <= names
    assert all("ignored" not in s["text"] for s in secs)


def test_parse_filing_html_dedups_page_headers_and_8k_items():
    # "Item 1" repeated as a running page header must not spawn extra sections;
    # 8-K sub-numbered items keep the dotted form.
    html = (
        "<p>Item 1. Business</p><p>Para one about the business here.</p>"
        "<p>Item 1</p><p>Para two, still the business section.</p>"
        "<p>Item 1</p><p>Para three.</p>"
        "<p>Item 8.01 Other Events</p><p>Something material happened.</p>"
        "<p>Item 9.01 Financial Statements and Exhibits</p><p>See exhibit 99.1.</p>"
    )
    secs = ep.parse_filing_html(html)
    names = [s["section"] for s in secs]
    assert names.count("Item 1") == 1
    assert "Item 8.01" in names and "Item 9.01" in names
    item1 = next(s for s in secs if s["section"] == "Item 1")
    assert "Para two" in item1["text"] and "Para three" in item1["text"]


def test_parse_form4():
    xml = """<?xml version="1.0"?>
    <ownershipDocument>
      <issuer><issuerCik>0000320193</issuerCik><issuerTradingSymbol>AAPL</issuerTradingSymbol></issuer>
      <reportingOwner>
        <reportingOwnerId><rptOwnerCik>0001214128</rptOwnerCik><rptOwnerName>COOK TIMOTHY D</rptOwnerName></reportingOwnerId>
        <reportingOwnerRelationship><isDirector>0</isDirector><isOfficer>1</isOfficer><officerTitle>CEO</officerTitle></reportingOwnerRelationship>
      </reportingOwner>
      <nonDerivativeTable>
        <nonDerivativeTransaction>
          <securityTitle><value>Common Stock</value></securityTitle>
          <transactionDate><value>2026-04-01</value></transactionDate>
          <transactionCoding><transactionCode>S</transactionCode></transactionCoding>
          <transactionAmounts>
            <transactionShares><value>511000</value></transactionShares>
            <transactionPricePerShare><value>170.5</value></transactionPricePerShare>
            <transactionAcquiredDisposedCode><value>D</value></transactionAcquiredDisposedCode>
          </transactionAmounts>
          <postTransactionAmounts><sharesOwnedFollowingTransaction><value>3280000</value></sharesOwnedFollowingTransaction></postTransactionAmounts>
        </nonDerivativeTransaction>
      </nonDerivativeTable>
    </ownershipDocument>"""
    d = ep.parse_form4(xml)
    assert d["issuer_ticker"] == "AAPL"
    assert d["owner_name"] == "COOK TIMOTHY D"
    assert d["is_officer"] is True and d["officer_title"] == "CEO"
    t = d["transactions"][0]
    assert t["code"] == "S" and t["open_market"] is True
    assert t["shares"] == 511000.0 and abs(t["value"] - 511000 * 170.5) < 1
    assert t["shares_owned_after"] == 3280000.0
    assert ep.parse_form4("<html>not ownership xml</html>") is None


def test_parse_submission_documents():
    txt = (
        "<DOCUMENT>\n<TYPE>10-Q\n<SEQUENCE>1\n<FILENAME>a.htm\n<DESCRIPTION>FORM 10-Q\n</DOCUMENT>\n"
        "<DOCUMENT>\n<TYPE>EX-31.1\n<SEQUENCE>2\n<FILENAME>b.htm\n</DOCUMENT>\n"
    )
    docs = ep.parse_submission_documents(txt)
    assert [d["doc_type"] for d in docs] == ["10-Q", "EX-31.1"]
    assert docs[0]["filename"] == "a.htm"


def _13dg(heading_block: str) -> str:
    return f"<html><body>{heading_block}\n\nCUSIP No. 037833100\n\n2. CHECK BOX\n</body></html>"


def test_parse_13dg_wrapped_person_heading():
    doc = _13dg("NAME OF\nREPORTING PERSON\n\nWarren E. Buffett\n\n2.")
    assert ep.parse_13dg_cover(doc)["filer_name"] == "Warren E. Buffett"


def test_parse_13dg_ss_or_irs_variant_skips_label():
    doc = _13dg(
        "NAMES OF REPORTING PERSONS\n\nS.S.\nOR I.R.S. IDENTIFICATION NO. OF ABOVE PERSON\n\n"
        "The\nVanguard Group - 23-1945930\n\n2."
    )
    assert ep.parse_13dg_cover(doc)["filer_name"] == "The Vanguard Group"


def test_parse_13dg_joins_hard_wrapped_name():
    doc = _13dg("NAME OF\nREPORTING PERSON\n\nJeffrey P.\nBezos\n\n2.")
    assert ep.parse_13dg_cover(doc)["filer_name"] == "Jeffrey P. Bezos"


def test_parse_13dg_legacy_item1_format_does_not_join_item2():
    txt = "#88160R101\nItem 1: Reporting Person - FMR LLC\nItem 2: (a)  [ ]\nItem 4: Delaware\n"
    assert ep.parse_13dg_cover(txt)["filer_name"] == "FMR LLC"


def test_parse_13dg_rejects_irs_boilerplate():
    doc = _13dg("NAME OF REPORTING PERSON\n\nI.R.S. IDENTIFICATION\nNO. OF ABOVE PERSONS\n\n2.")
    assert ep.parse_13dg_cover(doc)["filer_name"] is None


def test_sec_client_retries_on_429_then_succeeds():
    from lib.sec_client import SecClient

    class _Resp:
        def __init__(self, status, body=b""):
            self.status_code = status
            self.headers = {}
            self.content = body

        def raise_for_status(self):
            if self.status_code >= 400:
                raise RuntimeError(self.status_code)

    responses = [_Resp(429), _Resp(503), _Resp(200, b"ok")]
    client = SecClient(
        user_agent="Test Person test@example.com", requests_per_second=1000.0,
        backoff_base=0.001, backoff_cap=0.01,
    )
    client._session.get = lambda url, timeout: responses.pop(0)
    assert client.get_bytes("https://example.test/x") == b"ok"
    assert responses == []


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
