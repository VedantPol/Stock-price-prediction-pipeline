# utils.py
from datetime import datetime, date, timedelta
import numpy as np
import pandas as pd

def _convert_value(v):
    """Convert a single value to a JSON-serializable Python primitive."""
    if isinstance(v, (str, int, float, bool)) or v is None:
        return v
    if isinstance(v, (datetime, date)):
        return v.isoformat()
    if isinstance(v, timedelta):
        return str(v)
    if isinstance(v, pd.Timestamp):
        # Pandas Timestamp -> ISO string
        return v.isoformat()
    if isinstance(v, (np.integer,)):
        return int(v)
    if isinstance(v, (np.floating,)):
        return float(v)
    if isinstance(v, (np.bool_ ,)):
        return bool(v)
    if pd.isna(v):
        return None
    # Fallback: str
    return str(v)

def sanitize_for_json(obj):
    """
    Recursively convert obj into JSON-serializable structure:
      - convert pd.Timestamp / datetime / date to ISO strings
      - convert numpy scalar types to Python primitives
      - convert dict keys to strings
      - convert pandas Index/Series to lists
    """
    # primitives
    if isinstance(obj, (str, int, float, bool)) or obj is None:
        return obj

    # pandas objects
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, pd.Timedelta):
        return str(obj)
    if isinstance(obj, (pd.Series, pd.Index)):
        return [sanitize_for_json(x) for x in obj.tolist()]
    if isinstance(obj, pd.DataFrame):
        # convert to list of dicts (rows)
        return [ {str(k): sanitize_for_json(v) for k,v in row.items()} for _, row in obj.reset_index().iterrows() ]

    # numpy scalars
    if isinstance(obj, (np.integer, np.int64, np.int32)):
        return int(obj)
    if isinstance(obj, (np.floating, np.float64, np.float32)):
        return float(obj)
    if isinstance(obj, (np.bool_,)):
        return bool(obj)

    # lists / tuples / sets
    if isinstance(obj, (list, tuple, set)):
        return [sanitize_for_json(x) for x in obj]

    # dicts -> normalize keys to strings
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            # convert keys (e.g., Timestamp) to string
            k2 = k
            if not isinstance(k2, (str, int, float, bool, type(None))):
                # convert complex keys to string safely
                k2 = _convert_value(k2)
            new[str(k2)] = sanitize_for_json(v)
        return new

    # fallback for unknown types - try conversion
    return _convert_value(obj)
