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


def test_parse_submission_documents():
    txt = (
        "<DOCUMENT>\n<TYPE>10-Q\n<SEQUENCE>1\n<FILENAME>a.htm\n<DESCRIPTION>FORM 10-Q\n</DOCUMENT>\n"
        "<DOCUMENT>\n<TYPE>EX-31.1\n<SEQUENCE>2\n<FILENAME>b.htm\n</DOCUMENT>\n"
    )
    docs = ep.parse_submission_documents(txt)
    assert [d["doc_type"] for d in docs] == ["10-Q", "EX-31.1"]
    assert docs[0]["filename"] == "a.htm"


if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    for fn in fns:
        fn()
        print(f"ok  {fn.__name__}")
    print(f"\n{len(fns)} passed")
