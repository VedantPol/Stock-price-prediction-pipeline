# main.py
from pathlib import Path
from fetcher import fetch_yfinance_for_tickers
from dq import dq_checks
from storage import ensure_dirs, write_parquets_and_duckdb
from report import generate_html_report
import json
from datetime import datetime

# Example tickers (replace with real .NS tickers)
TICKERS = [
    "RELIANCE.NS", "TCS.NS", "HDFCBANK.NS", "INFY.NS", "ICICIBANK.NS"
    # add full Nifty50 + Next50 list here
]

def fetch_and_store_nifty(
    tickers,
    start=None,
    end=None,
    period="1y",
    out_base="nifty_data",
    overwrite_parquet=False
):
    paths = ensure_dirs(out_base)
    print("Fetching data from yfinance...")
    dfs = fetch_yfinance_for_tickers(tickers, start=start, end=end, period=period)

    print("Running DQ checks...")
    dq_results = {}
    for t, df in dfs.items():
        dq_results[t] = dq_checks(df)

    print("Writing parquet files and registering into DuckDB...")
    table_map = write_parquets_and_duckdb(dfs, paths, overwrite=overwrite_parquet, db_table_prefix="nifty")

    print("Generating HTML report...")
    html_out = paths['html_dir'] / f"nifty_report_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.html"
    generate_html_report(dq_results, dfs, html_out)

    return {
        "parquet_dir": str(paths['parquet_dir'].resolve()),
        "duckdb_path": str(paths['db_path'].resolve()),
        "html_report": str(html_out.resolve()),
        "table_map": table_map,
        "dq_results_summary": {k: {"rows": v.get('rows',0), "dq_pass": v.get('dq_pass', False)} for k,v in dq_results.items()}
    }

if __name__ == "__main__":
    out = fetch_and_store_nifty(TICKERS, period="1y", out_base="nifty_data", overwrite_parquet=True)
    print("Done.")
    print("HTML report at:", out['html_report'])
    print("DuckDB file:", out['duckdb_path'])
    print("Parquet dir:", out['parquet_dir'])
