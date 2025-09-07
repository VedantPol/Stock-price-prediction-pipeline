# fetcher.py
from typing import List, Dict, Optional
import pandas as pd
import yfinance as yf
import time

def fetch_yfinance_for_tickers(
    tickers: List[str],
    start: Optional[str] = None,
    end: Optional[str] = None,
    period: Optional[str] = None,
    batch_size: int = 5,
    retry: int = 2
) -> Dict[str, pd.DataFrame]:
    """
    Fetch historical OHLCV for a list of tickers using yfinance.
    Returns dict ticker -> DataFrame with columns [Open, High, Low, Close, Adj Close, Volume] and a datetime index.
    """
    results = {}
    total = len(tickers)
    for i in range(0, total, batch_size):
        batch = tickers[i:i+batch_size]
        attempt = 0
        while attempt <= retry:
            try:
                # Using download for the batch
                data = yf.download(batch, start=start, end=end, period=period, group_by='ticker', threads=True, progress=False, auto_adjust=False)
                break
            except Exception as e:
                attempt += 1
                if attempt > retry:
                    # mark empties for this batch
                    for t in batch:
                        results[t] = pd.DataFrame()
                    data = None
                else:
                    time.sleep(1 + attempt)
        if data is None:
            continue

        # normalize
        if len(batch) == 1:
            ticker = batch[0]
            df = data.copy()
            if df.empty:
                results[ticker] = pd.DataFrame()
            else:
                df.index = pd.to_datetime(df.index)
                results[ticker] = df
        else:
            # data has a top-level column per ticker
            for ticker in batch:
                try:
                    if ticker in data.columns.get_level_values(0):
                        sub = data[ticker].copy()
                        sub.index = pd.to_datetime(sub.index)
                        results[ticker] = sub
                    else:
                        results[ticker] = pd.DataFrame()
                except Exception:
                    results[ticker] = pd.DataFrame()

    # Final normalization: ensure expected columns and date-normalized index
    expected = ['Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume']
    for t, df in list(results.items()):
        if df is None or df.empty:
            results[t] = pd.DataFrame(columns=expected)
            continue
        df = df.sort_index()
        for col in expected:
            if col not in df.columns:
                df[col] = pd.NA
        # normalize index to date-only (midnight)
        df.index = pd.to_datetime(df.index).normalize()
        results[t] = df[expected].copy()
    return results
