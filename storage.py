# storage.py
from pathlib import Path
from typing import Dict
import duckdb
import pandas as pd

def ensure_dirs(base_dir: str = "data"):
    base = Path(base_dir)
    base.mkdir(parents=True, exist_ok=True)
    parquet_dir = base / "parquet"
    parquet_dir.mkdir(exist_ok=True)
    html_dir = base / "html"
    html_dir.mkdir(exist_ok=True)
    db_path = base / "nifty.duckdb"
    return {"base": base, "parquet_dir": parquet_dir, "html_dir": html_dir, "db_path": db_path}

def write_parquets_and_duckdb(
    dfs: Dict[str, pd.DataFrame],
    paths: Dict[str, Path],
    overwrite: bool = False,
    db_table_prefix: str = "nifty"
) -> Dict[str, str]:
    """
    Write per-ticker parquet files and create DuckDB tables using read_parquet.
    Returns mapping ticker -> table_name.
    """
    duck = duckdb.connect(str(paths['db_path']))
    table_map = {}
    for ticker, df in dfs.items():
        safe_name = ticker.replace('.', '_').replace('-', '_').upper()
        parquet_file = paths['parquet_dir'] / f"{safe_name}.parquet"
        # ensure index is a column
        to_write = df.reset_index().rename(columns={'index': 'Date'})
        if parquet_file.exists() and not overwrite:
            # keep existing file
            pass
        else:
            to_write.to_parquet(parquet_file, index=False)

        table_name = f"{db_table_prefix}_{safe_name}"
        try:
            if overwrite:
                duck.execute(f"DROP TABLE IF EXISTS {table_name}")
            # Create table by reading parquet (fast, leverages DuckDB read_parquet)
            duck.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM read_parquet('{parquet_file.as_posix()}')")
        except Exception as e:
            # fallback: register pandas DF and create table
            duck.register(safe_name, to_write)
            duck.execute(f"CREATE TABLE IF NOT EXISTS {table_name} AS SELECT * FROM {safe_name}")
        table_map[ticker] = table_name
    duck.close()
    return table_map
