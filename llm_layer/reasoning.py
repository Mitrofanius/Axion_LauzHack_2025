import json
import re
import pandas as pd
from typing import Dict, Any
from .llm_client import llm_call

MAX_RESULT_ROWS = 50
SQL_ROW_LIMIT = 1000
FORBIDDEN_SQL = [r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bDROP\b",
                 r"\bALTER\b", r";", r"--", r"\/\*", r"\bGRANT\b", r"\bREVOKE\b"]

with open("data/semantic_layer.json") as f:
    SEMANTIC_LAYER = json.load(f)
with open("data/schema.json") as f:
    SCHEMA_INFO = json.load(f)

def sanitize_sql(sql: str) -> str:
    if not sql or not sql.strip():
        raise ValueError("Empty SQL returned.")
    sql = re.sub(r"^```(sql)?\s*|\s*```$", "", sql, flags=re.IGNORECASE)
    for pattern in FORBIDDEN_SQL:
        if re.search(pattern, sql, flags=re.IGNORECASE):
            raise ValueError(f"Forbidden SQL pattern: {pattern}")
    if not re.match(r"^\s*SELECT\b", sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT statements allowed.")
    if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        sql = sql.rstrip() + f" LIMIT {SQL_ROW_LIMIT}"
    return sql

def summarize_df(df: pd.DataFrame, max_rows: int = MAX_RESULT_ROWS) -> str:
    head = df.head(max_rows)
    col_types = ", ".join([f"{c}({head[c].dtype})" for c in head.columns])
    sample_csv = head.to_csv(index=False)
    sample_csv = sample_csv if len(sample_csv) < 10000 else sample_csv[:10000] + "\n...TRUNCATED..."
    return f"ROWS_RETURNED: {len(df)}\nCOLUMNS: {col_types}\nSAMPLE_ROWS:\n{sample_csv}"

def build_sql_prompt(question: str) -> str:
    semantic_json = json.dumps(SEMANTIC_LAYER, indent=2)
    examples = """
# Example: List active accounts for BR-abc123
# Example: Show last 5 transactions for account X
"""
    instructions = """
Produce only one SELECT statement. No comments, no semicolons, use exact table/column names.
"""
    return f"""
### SCHEMA
{json.dumps(SCHEMA_INFO, indent=2)}

### SEMANTIC LAYER
{semantic_json}

### EXAMPLES
{examples}

### INSTRUCTIONS
{instructions}

### USER QUESTION
{question}
"""

def build_explanation_prompt(question: str, sql: str, df: pd.DataFrame) -> str:
    summary = summarize_df(df)
    semantic_json = json.dumps(SEMANTIC_LAYER, indent=2)
    return f"""
User Question:
{question}

SQL Executed:
{sql}

Result Summary:
{summary}

Semantic Layer:
{semantic_json}

Task: Provide a concise explanation (3-7 sentences) of what the SQL returned.
"""

def generate_sql(question: str) -> str:
    raw_sql = llm_call(build_sql_prompt(question))
    return sanitize_sql(raw_sql)

def explain_sql(question: str, sql: str, df: pd.DataFrame) -> str:
    return llm_call(build_explanation_prompt(question, sql, df)).strip()

def answer_question(question: str, run_sql) -> Dict[str, Any]:
    sql = generate_sql(question)
    df = run_sql(sql)
    explanation = explain_sql(question, sql, df)
    return {"sql": sql, "dataframe": df, "explanation": explanation}
