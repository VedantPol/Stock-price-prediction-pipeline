# dq.py
from typing import Dict, Any
import pandas as pd
import numpy as np
from utils import sanitize_for_json

def dq_checks(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Returns a dictionary of DQ metrics, sanitized for JSON.
    """
    report = {}
    if df is None or df.empty:
        report['rows'] = 0
        report['start_date'] = None
        report['end_date'] = None
        report['missing_counts'] = {}
        report['missing_percent'] = {}
        report['duplicate_index_count'] = 0
        report['index_monotonic_increasing'] = True
        report['non_positive_price_counts'] = {}
        report['non_positive_volume_count'] = 0
        report['duplicate_rows'] = 0
        report['extreme_return_count'] = None
        report['extreme_dates'] = []
        report['large_calendar_gaps_count'] = 0
        report['large_gaps_sample'] = {}
        report['dq_pass'] = False
        report['dq_reasons'] = ['No rows returned']
        return sanitize_for_json(report)

    # ensure index is datetime
    df = df.copy()
    if not pd.api.types.is_datetime64_any_dtype(df.index):
        df.index = pd.to_datetime(df.index)

    report['rows'] = int(len(df))
    report['start_date'] = df.index.min().date().isoformat()
    report['end_date'] = df.index.max().date().isoformat()

    missing = df.isna().sum()
    report['missing_counts'] = {str(k): int(v) for k,v in missing.items()}
    report['missing_percent'] = {str(k): float(round(v/len(df), 6)) for k,v in missing.items()}

    report['duplicate_index_count'] = int(df.index.duplicated().sum())
    report['index_monotonic_increasing'] = bool(df.index.is_monotonic_increasing)
    price_cols = ['Open', 'High', 'Low', 'Close', 'Adj Close']
    neg_prices = {}
    for c in price_cols:
        if c in df.columns:
            neg_prices[c] = int((df[c] <= 0).sum())
        else:
            neg_prices[c] = 0
    report['non_positive_price_counts'] = neg_prices

    if 'Volume' in df.columns:
        report['non_positive_volume_count'] = int((df['Volume'] <= 0).sum())
    else:
        report['non_positive_volume_count'] = 0

    report['duplicate_rows'] = int(df.duplicated().sum())

    # returns based outlier detection
    if 'Close' in df.columns and len(df) >= 5:
        returns = df['Close'].pct_change().dropna()
        mean_r = float(returns.mean()) if len(returns) else 0.0
        std_r = float(returns.std(ddof=0)) if len(returns) else 0.0
        if std_r > 0:
            z = (returns - mean_r) / std_r
            extreme_idx = z[ z.abs() > 5 ].index
            report['extreme_return_count'] = int(len(extreme_idx))
            report['extreme_dates'] = [d.date().isoformat() for d in extreme_idx]
        else:
            report['extreme_return_count'] = 0
            report['extreme_dates'] = []
    else:
        report['extreme_return_count'] = None
        report['extreme_dates'] = []

    # calendar gaps (gaps > 3 days)
    if len(df) >= 2:
        diffs = df.index.to_series().diff().dt.days.dropna()
        large_gaps = diffs[ diffs > 3 ]
        report['large_calendar_gaps_count'] = int(len(large_gaps))
        # sample as simple dict of string->int
        report['large_gaps_sample'] = { str(idx.date().isoformat()): int(v) for idx,v in large_gaps.head(5).to_dict().items() }
    else:
        report['large_calendar_gaps_count'] = 0
        report['large_gaps_sample'] = {}

    # pass/fail heuristic
    pass_fail = True
    reasons = []
    if report['rows'] == 0:
        pass_fail = False
        reasons.append("No rows returned")
    if report['duplicate_index_count'] > 0:
        pass_fail = False
        reasons.append("Duplicate date index entries")
    if any(v > 0 for v in report['non_positive_price_counts'].values()):
        pass_fail = False
        reasons.append("Non-positive price values present")
    # zero volume days do not necessarily fail (flag only)
    if report['non_positive_volume_count'] > 0:
        reasons.append("Non-positive volume days present")

    report['dq_pass'] = bool(pass_fail)
    report['dq_reasons'] = reasons

    # sanitize before returning
    return sanitize_for_json(report)
