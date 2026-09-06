"""Generate config/ciks_full.json — the scaled company universe.

Resolves an S&P 500 ticker list against SEC's official ticker→CIK map
(`company_tickers.json`) and writes it in the same schema as `ciks.json`.
Merges in whatever is already in `ciks.json` so the pilot companies are kept.

    python config/build_universe.py                # writes config/ciks_full.json
    python config/build_universe.py --out foo.json # custom path

The medallion notebooks take `ciks_config` as a widget — point it at
`../config/ciks_full.json` for the scaled run. Volume is driven by XBRL
companyfacts (full history per CIK, one cheap API call each), NOT by how many
filing documents you download — so the >1M-row bar is cleared at this scale
regardless of `max_new_filings_per_cik`.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
UA = os.environ.get(
    "SEC_USER_AGENT",
    "EDGAR Intelligence Platform - Zach Steele zacharysteele8@gmail.com",
)

# Forms the pipeline can actually process: periodic reports (10-K/10-Q/8-K) plus
# foreign-filer equivalents (20-F annual, 40-F Canadian, 6-K interim). NOT the
# ownership/registration forms (3/4/5, 13D/G/F, S-1, 424B) — the section parser,
# XBRL flattener and AI briefing have nothing to do with those.
FORMS = ["10-K", "10-Q", "8-K", "20-F", "40-F", "6-K"]

# S&P 500 constituents (large operating companies — dense XBRL, clean filings).
# Trimmed of dual-class duplicates the SEC map can't disambiguate by ticker.
SP500 = """
MMM AOS ABT ABBV ACN ADBE AMD AES AFL A APD ABNB AKAM ALB ARE ALGN ALLE LNT ALL
GOOGL GOOG MO AMZN AMCR AEE AEP AXP AIG AMT AWK AMP AME AMGN APH ADI ANSS AON APA
AAPL AMAT APTV ACGL ADM ANET AJG AIZ T ATO ADSK ADP AZO AVB AVY AXON BKR BALL BAC
BAX BDX BRK.B BBY TECH BIIB BLK BX BK BA BKNG BWA BSX BMY AVGO BR BRO BF.B BLDR BG
CDNS CZR CPT CPB COF CAH KMX CCL CARR CAT CBOE CBRE CDW CE COR CNC CNP CF CHRW CRL
SCHW CHTR CVX CMG CB CHD CI CINF CTAS CSCO C CFG CLX CME CMS KO CTSH CL CMCSA CMA
CAG COP ED STZ CEG COO CPRT GLW CTVA CSGP COST CTRA CCI CSX CMI CVS DHR DRI DVA DAY
DE DAL XRAY DVN DXCM FANG DLR DFS DG DLTR D DPZ DOV DOW DHI DTE DUK DD EMN ETN EBAY
ECL EIX EW EA ELV EMR ENPH ETR EOG EPAM EQT EFX EQIX EQR ESS EL ETSY EG EVRG ES EXC
EXPE EXPD EXR XOM FFIV FDS FICO FAST FRT FDX FIS FITB FSLR FE FI FMC F FTNT FTV FOXA
FOX BEN FCX GRMN IT GEHC GEN GNRC GD GE GIS GM GPC GILD GPN GL GS HAL HIG HAS HCA
DOC HSIC HSY HES HPE HLT HOLX HD HON HRL HST HWM HPQ HUBB HUM HBAN HII IBM IEX IDXX
ITW ILMN INCY IR PODD INTC ICE IFF IP IPG INTU ISRG IVZ INVH IQV IRM JBHT JBL JKHY
J JNJ JCI JPM JNPR K KVUE KDP KEY KEYS KMB KIM KMI KLAC KHC KR LHX LH LRCX LW LVS
LDOS LEN LLY LIN LYV LKQ LMT L LOW LULU LYB MTB MRO MPC MKTX MAR MMC MLM MAS MA MTCH
MKC MCD MCK MDT MRK META MET MTD MGM MCHP MU MSFT MAA MRNA MHK MOH TAP MDLZ MPWR MNST
MCO MS MOS MSI MSCI NDAQ NTAP NFLX NEM NWSA NWS NEE NKE NI NDSN NSC NTRS NOC NCLH NRG
NUE NVDA NVR NXPI ORLY OXY ODFL OMC ON OKE ORCL OTIS PCAR PKG PANW PARA PH PAYX PAYC
PYPL PNR PEP PFE PCG PM PSX PNW PNC POOL PPG PPL PFG PG PGR PLD PRU PEG PTC PSA PHM
QRVO PWR QCOM DGX RL RJF RTX O REG REGN RF RSG RMD RVTY RHI ROK ROL ROP ROST RCL SPGI
CRM SBAC SLB STX SRE NOW SHW SPG SWKS SJM SNA SOLV SO LUV SWK SBUX STT STLD STE SYK
SMCI SYF SNPS SYY TMUS TROW TTWO TPR TRGP TGT TEL TDY TFX TER TSLA TXN TXT TMO TJX
TSCO TT TDG TRV TRMB TFC TYL TSN USB UBER UDR ULTA UNP UAL UPS URI UNH UHS VLO VTR
VLTO VRSN VRSK VZ VRTX VTRS VICI V VST VMC WRB GWW WAB WBA WMT DIS WBD WM WAT WEC WFC
WELL WST WDC WY WMB WTW WYNN XEL XYL YUM ZBRA ZBH ZTS
""".split()


def resolve(tickers, sec_map):
    """sec_map: {UPPER_TICKER: {cik_str, ticker, title}}. Returns rows + misses."""
    rows, misses = [], []
    for t in tickers:
        key = t.upper().replace(".", "-")  # SEC uses BRK-B for BRK.B
        hit = sec_map.get(key) or sec_map.get(t.upper())
        if not hit:
            misses.append(t)
            continue
        rows.append({
            "ticker": hit["ticker"],
            "cik": str(hit["cik_str"]).zfill(10),
            "name": hit["title"],
        })
    return rows, misses


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--out", default=os.path.join(HERE, "ciks_full.json"))
    args = ap.parse_args()

    print("fetching SEC company_tickers.json ...")
    req = urllib.request.Request(
        "https://www.sec.gov/files/company_tickers.json", headers={"User-Agent": UA}
    )
    raw = json.load(urllib.request.urlopen(req, timeout=60))
    sec_map = {}
    for v in raw.values():
        sec_map[str(v["ticker"]).upper()] = v
    print(f"  {len(sec_map):,} tickers in SEC map")

    # keep the current pilot companies
    pilot = []
    ck = os.path.join(HERE, "ciks.json")
    if os.path.exists(ck):
        pilot = json.load(open(ck)).get("companies", [])

    rows, misses = resolve(SP500, sec_map)
    by_cik = {r["cik"]: r for r in rows}
    for p in pilot:
        by_cik.setdefault(p["cik"].zfill(10), {**p, "cik": p["cik"].zfill(10)})

    companies = sorted(by_cik.values(), key=lambda r: r["ticker"])
    out = {
        "_comment": (
            "Scaled universe (S&P 500 + pilot), generated by config/build_universe.py. "
            "Point the notebooks' ciks_config widget here. Volume comes from XBRL "
            "companyfacts (full history per CIK), so >1M rows is cleared regardless "
            "of max_new_filings_per_cik."
        ),
        "forms": FORMS,
        "companies": companies,
    }
    json.dump(out, open(args.out, "w"), indent=2)
    print(f"\nwrote {len(companies)} companies -> {args.out}")
    if misses:
        print(f"unresolved ({len(misses)}): {' '.join(misses)}")


if __name__ == "__main__":
    sys.exit(main())
