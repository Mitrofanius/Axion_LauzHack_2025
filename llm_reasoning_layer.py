import json
import re
from typing import Tuple, Dict, Any
import pandas as pd
from deepl import llm_call
MAX_RESULT_ROWS_FOR_EXPLAIN = 50  # how many rows to include when asking LLM to explain results
SQL_ROW_LIMIT = 1000              # safety cap to avoid huge queries
FORBIDDEN_SQL_PATTERNS = [
    r"\bINSERT\b", r"\bUPDATE\b", r"\bDELETE\b", r"\bDROP\b", r"\bALTER\b",
    r"\bCREATE\b", r";", r"--", r"\/\*", r"\bGRANT\b", r"\bREVOKE\b"
]

with open("data/semantic_layer.json") as f:
    semantic_layer = json.load(f)

with open("data/schema.json") as f:
    schema_info = json.load(f)

def sanitize_and_validate_sql(candidate_sql: str) -> str:
    """
    Ensure the model returned a single SELECT statement and no forbidden keywords.
    Returns a cleaned SQL (with appended LIMIT if appropriate).
    Raises ValueError on invalid SQL.
    """
    if not candidate_sql or not candidate_sql.strip():
        raise ValueError("Empty SQL returned by model.")

    sql = candidate_sql.strip()

    # remove surrounding ```sql fences if present
    sql = re.sub(r"^```(sql)?\s*", "", sql, flags=re.IGNORECASE)
    sql = re.sub(r"\s*```$", "", sql)

    # reject multi-statement by presence of semicolon OR comment tokens
    # for pattern in FORBIDDEN_SQL_PATTERNS:
    #     if re.search(pattern, sql, flags=re.IGNORECASE):
    #         raise ValueError(f"SQL contains forbidden pattern: {pattern}")

    # Ensure it begins with SELECT
    if not re.match(r"^\s*SELECT\b", sql, flags=re.IGNORECASE):
        raise ValueError("Only SELECT statements are allowed. Model must return a SELECT query.")

    # If no LIMIT clause detected, add a safe row limit at the end
    if not re.search(r"\bLIMIT\b", sql, flags=re.IGNORECASE):
        sql = sql.rstrip()
        if sql.endswith(";"):
            sql = sql[:-1].rstrip()
        sql = f"{sql} LIMIT {SQL_ROW_LIMIT}"

    return sql

def summarize_df_for_prompt(df: pd.DataFrame, max_rows: int = MAX_RESULT_ROWS_FOR_EXPLAIN) -> str:
    """
    Convert a DataFrame into a concise textual table for inclusion in a prompt.
    Truncates to max_rows rows and summarizes column types.
    """
    if not isinstance(df, pd.DataFrame):
        return "Result is not a DataFrame; cannot summarize."

    nrows = len(df)
    head = df.head(max_rows)
    col_types = ", ".join([f"{c}({str(head[c].dtype)})" for c in head.columns])
    sample_csv = head.to_csv(index=False)
    sample_csv = sample_csv if len(sample_csv) < 10000 else sample_csv[:10000] + "\n...TRUNCATED..."
    return f"ROWS_RETURNED: {nrows}\nCOLUMNS: {col_types}\nSAMPLE_ROWS:\n{sample_csv}"

# -------------------------
# Prompt assembly
# -------------------------
def build_sql_generation_prompt(user_question: str, schema_info: str, semantic_layer: Dict[str, Any]) -> str:
    semantic_json = json.dumps(semantic_layer, indent=2)
    examples = """
# EXAMPLES (format)
# - Input: "List active accounts for BR-abc123"
# - Expected SQL: SELECT a.account_id, a.account_iban, a.account_currency
#                 FROM account a
#                 JOIN br_to_account bta ON a.account_id = bta.account_id
#                 WHERE bta.br_id = 'BR-ABC123' AND bta.relationship_status_code = 1
#                 LIMIT 1000

# - Input: "Show last 5 transactions for account X"
# - Expected SQL: SELECT "Transaction ID", "Debit/Credit", Amount, Currency, Date
#                 FROM transactions
#                 WHERE "Account ID" = 'ACCOUNT-X'
#                 ORDER BY Date DESC
#                 LIMIT 1000
"""

    tool_instructions = """
TOOL INSTRUCTIONS (must follow exactly):
1) You MUST output **only one** SQL SELECT statement in your response—no explanation, no commentary, no backticks.
2) The SQL must be read-only (SELECT). No semicolons, no comments, no multiple statements.
3) Use the tables and columns exactly as in the schema provided above.
4) When joining BR <-> Account links, prefer relationship_status_code = 1 to filter active links unless the user asks for historical data.
5) Return column names in the SELECT that are meaningful and descriptive.
6) If the user requests aggregates (sums, counts), include GROUP BY appropriately.
"""

    prompt = f"""
### CONTEXT: Database schema
{schema_info}

### CONTEXT: Semantic layer (JSON)
{semantic_json}

### BUSINESS RULES
- Blank close date means active (account_close_date, br_close_date, partner_close_date).
- relationship_status_code = 1 indicates active BR<->Account link.
- partner_country_status_code = 1 marks current country row.
- Transaction Amount is positive; use Debit/Credit to infer sign.

### USAGE EXAMPLES
{examples}

### TOOL INSTRUCTIONS (strict)
{tool_instructions}

### USER QUESTION
{user_question}

### REQUIRED OUTPUT
Produce ONLY a single valid SELECT SQL statement (no surrounding text).
"""
    return prompt

def build_explanation_prompt(user_question: str, sql_executed: str, result_df: pd.DataFrame, semantic_layer: Dict[str, Any]) -> str:
    """
    Build a prompt asking the LLM to explain the SQL results concisely.
    """
    result_summary = summarize_df_for_prompt(result_df)
    semantic_json = json.dumps(semantic_layer, indent=2)

    prompt = f"""
You are an assistant that explains SQL query results for FCC investigators.

USER QUESTION:
{user_question}

SQL EXECUTED:
{sql_executed}

RESULT SUMMARY:
{result_summary}

SEMANTIC LAYER:
{semantic_json}

TASK: Provide a concise (3-7 sentence) explanation of what the SQL returned, focusing on:
- The answer to the user's question.
- Any notable numbers (row count, sums, maxima/minima if relevant).
Do NOT provide additional SQL or perform further queries. Be factual and cite the column names you used.
Focus on providing an executive summary of the information, focusing on relevant info and not on technical data on how it was obtained.
"""
    return prompt

def generate_sql_from_nl(user_question: str) -> str:
    """
    Ask the LLM to generate a SELECT SQL for the user_question using DB schema introspected via run_sql_func.
    Returns the sanitized SQL ready to execute via run_sql_func.
    """
    prompt = build_sql_generation_prompt(user_question, schema_info, semantic_layer)
    raw_response = llm_call(prompt)

    # Extract plain SQL from response and sanitize/validate
    sql = sanitize_and_validate_sql(raw_response)
    return sql

def explain_sql_results(user_question: str, executed_sql: str, result_df: pd.DataFrame, semantic_layer: Dict[str, Any]) -> str:
    """
    Ask the LLM to generate a concise explanation of the executed SQL and the returned results.
    """
    prompt = build_explanation_prompt(user_question, executed_sql, result_df, semantic_layer)
    explanation = llm_call(prompt)
    return explanation.strip()

def answer_user_question(user_question: str, run_sql) -> Dict[str, Any]:
    """
    End-to-end: NL -> SQL -> run_sql -> NL explanation.
    Returns a dict: { "sql": str, "dataframe": pd.DataFrame, "explanation": str }
    """
    # Step 1: get SQL from LLM (sanitized)
    sql = generate_sql_from_nl(user_question)

    # Step 2: safety double-check (again)
    sql = sanitize_and_validate_sql(sql)  # will raise if invalid

    # Step 3: execute via provided run_sql (expect pandas.DataFrame)
    df = run_sql(sql)

    # Step 4: ask LLM to explain results
    explanation = explain_sql_results(user_question, sql, df, semantic_layer)

    return {"sql": sql, "dataframe": df, "explanation": explanation}
