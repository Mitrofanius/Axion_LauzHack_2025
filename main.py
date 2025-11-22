import pandas as pd
from sqlalchemy import create_engine, text
from llm_reasoning_layer import answer_user_question

DB_URI = "sqlite:///bank_data.db"
DATA_DIR = "./data"

engine = create_engine(DB_URI)

def load_csv(name):
    path = f"{DATA_DIR}/{name}.csv"
    return pd.read_csv(path, dtype=str).replace({"": None})


def to_date(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df


def to_datetime(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce")
    return df

account = load_csv("account")
br_to_account = load_csv("br_to_account")
business_rel = load_csv("business_rel")
client_onboarding_notes = load_csv("client_onboarding_notes")
partner = load_csv("partner")
partner_country = load_csv("partner_country")
partner_role = load_csv("partner_role")
transactions = load_csv("transactions")


account = to_date(account, ["account_open_date", "account_close_date"])
business_rel = to_date(business_rel, ["br_open_date", "br_close_date"])
partner = to_date(partner, ["partner_open_date", "partner_close_date", "partner_birth_year"])
partner_role = to_date(partner_role, ["relationship_start_date", "relationship_end_date"])
transactions = to_datetime(transactions, ["Date"])

# Cast numeric-like fields
if "relationship_status_code" in br_to_account.columns:
    br_to_account["relationship_status_code"] = br_to_account["relationship_status_code"].astype(float)

if "partner_country_status_code" in partner_country.columns:
    partner_country["partner_country_status_code"] = partner_country["partner_country_status_code"].astype(float)

if "Amount" in transactions.columns:
    transactions["Amount"] = transactions["Amount"].astype(float)

if "Balance" in transactions.columns:
    transactions["Balance"] = transactions["Balance"].astype(float)


DDL_STATEMENTS = [
    """
    DROP TABLE IF EXISTS account;
    CREATE TABLE account (
        account_id TEXT PRIMARY KEY,
        account_iban TEXT,
        account_currency TEXT,
        account_open_date DATE,
        account_close_date DATE
    );
    """,
    """
    DROP TABLE IF EXISTS business_rel;
    CREATE TABLE business_rel (
        br_id TEXT PRIMARY KEY,
        br_open_date DATE,
        br_close_date DATE
    );
    """,
    """
    DROP TABLE IF EXISTS br_to_account;
    CREATE TABLE br_to_account (
        br_id TEXT,
        account_id TEXT,
        relationship_id TEXT,
        relationship_status_code INTEGER,
        PRIMARY KEY (br_id, account_id),
        FOREIGN KEY (br_id) REFERENCES business_rel(br_id),
        FOREIGN KEY (account_id) REFERENCES account(account_id)
    );
    """,
    """
    DROP TABLE IF EXISTS partner;
    CREATE TABLE partner (
        partner_id TEXT PRIMARY KEY,
        industry_gic2_code TEXT,
        partner_class_code TEXT,
        partner_gender TEXT,
        partner_name TEXT,
        partner_phone_number TEXT,
        partner_birth_year DATE,
        partner_address TEXT,
        partner_open_date DATE,
        partner_close_date DATE
    );
    """,
    """
    DROP TABLE IF EXISTS client_onboarding_notes;
    CREATE TABLE client_onboarding_notes (
        Partner_ID TEXT PRIMARY KEY,
        Onboarding_Note TEXT,
        FOREIGN KEY (Partner_ID) REFERENCES partner(partner_id)
    );
    """,
    """
    DROP TABLE IF EXISTS partner_country;
    CREATE TABLE partner_country (
        partner_id TEXT,
        country_type TEXT,
        country_name TEXT,
        partner_country_status_code INTEGER,
        PRIMARY KEY (partner_id, country_type),
        FOREIGN KEY (partner_id) REFERENCES partner(partner_id)
    );
    """,
    """
    DROP TABLE IF EXISTS partner_role;
    CREATE TABLE partner_role (
        partner_id TEXT,
        entity_type TEXT,
        entity_id TEXT,
        relationship_start_date DATE,
        relationship_end_date DATE,
        br_type_code TEXT,
        associated_partner_id TEXT,
        partner_class_code TEXT,
        PRIMARY KEY (partner_id, entity_id, br_type_code),
        FOREIGN KEY (partner_id) REFERENCES partner(partner_id),
        FOREIGN KEY (associated_partner_id) REFERENCES partner(partner_id)
    );
    """,
    """
    DROP TABLE IF EXISTS transactions;
    CREATE TABLE transactions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        "Transaction ID" TEXT,
        "Debit/Credit" TEXT,
        "Account ID" TEXT,
        Amount REAL,
        Balance REAL,
        Currency TEXT,
        Date TIMESTAMP,
        Transfer_Type TEXT,
        counterparty_Account_ID TEXT,
        ext_counterparty_Account_ID TEXT,
        ext_counterparty_country TEXT,
        FOREIGN KEY ("Account ID") REFERENCES account(account_id),
        FOREIGN KEY (counterparty_Account_ID) REFERENCES account(account_id)
    );
    """
]
with engine.connect() as conn:
    for ddl in DDL_STATEMENTS:
        # Split on semicolons and run each statement individually
        statements = [s.strip() for s in ddl.split(";") if s.strip()]
        for stmt in statements:
            conn.execute(text(stmt))

account.to_sql("account", engine, if_exists="append", index=False)
business_rel.to_sql("business_rel", engine, if_exists="append", index=False)
br_to_account.to_sql("br_to_account", engine, if_exists="append", index=False)
partner.to_sql("partner", engine, if_exists="append", index=False)
client_onboarding_notes.to_sql("client_onboarding_notes", engine, if_exists="append", index=False)
partner_country.to_sql("partner_country", engine, if_exists="append", index=False)
partner_role.to_sql("partner_role", engine, if_exists="append", index=False)
transactions.to_sql("transactions", engine, if_exists="append", index=False)


def run_sql(sql: str, row_limit: int = None):
    """
    Execute a read-only SELECT SQL query using sqlite3, log to audit_log, and return a pandas DataFrame.
    """
    sql_strip = sql.strip().lower()
    if not sql_strip.startswith("select"):
        raise ValueError("run_sql only allows SELECT statements for safety.")
    if row_limit is not None and "limit" not in sql_strip:
        sql_to_run = sql.rstrip().rstrip(";") + f" LIMIT {int(row_limit)}"
    else:
        sql_to_run = sql
    with engine.connect() as conn:
        df = pd.read_sql_query(sql_to_run, conn)
    return df

question = input("Question: ")
answer = answer_user_question(
    question,
    run_sql
)
sql, dataframe, explanation = answer["sql"], answer["dataframe"], answer["explanation"]
print("\nGenerated SQL:\n", sql)
print("\nDataFrame Result:\n", dataframe)
print("\nLLM Explanation of Results:\n", explanation)