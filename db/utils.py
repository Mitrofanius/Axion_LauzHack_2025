import pandas as pd
from sqlalchemy import create_engine, text

def load_csv(path: str, dtype=str) -> pd.DataFrame:
    return pd.read_csv(path, dtype=dtype).replace({"": None})

def to_date(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df

def to_datetime(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

def cast_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    for c in columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df

def create_engine_and_run_ddl(db_uri: str, ddl_statements: list[str]):
    engine = create_engine(db_uri)
    with engine.connect() as conn:
        for ddl in ddl_statements:
            for stmt in filter(None, map(str.strip, ddl.split(";"))):
                conn.execute(text(stmt))
    return engine

def run_select_query(engine, sql: str, row_limit: int = None) -> pd.DataFrame:
    sql_lower = sql.strip().lower()
    # if not sql_lower.startswith("select"):
    #     raise ValueError("Only SELECT statements allowed.")
    if row_limit is not None and "limit" not in sql_lower:
        sql = sql.rstrip(";") + f" LIMIT {row_limit}"
    with engine.connect() as conn:
        return pd.read_sql_query(sql, conn)
