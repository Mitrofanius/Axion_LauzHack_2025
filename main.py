from db.etl import load_all_data
from db.utils import create_engine_and_run_ddl, run_select_query
from llm_layer.reasoning import answer_question

DB_URI = "sqlite:///bank_data.db"

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
from analysis import transactions_stats, make_analysis_plot
if __name__ == "__main__":
    # Load CSVs and preprocess
    data = load_all_data()

    # Create DB and load tables
    engine = create_engine_and_run_ddl(DB_URI, DDL_STATEMENTS)
    for table, df in data.items():
        df.to_sql(table, engine, if_exists="append", index=False)

    # Function for SQL queries
    def run_sql(sql: str):
        return run_select_query(engine, sql)

    question = input("Question: ")
    if question.startswith("analysis"):
        question = question[len("analysis "):].strip()
        analysis = transactions_stats(question, run_sql)
        make_analysis_plot(analysis)
    else:
        answer = answer_question(question, run_sql)
        print("\nGenerated SQL:\n", answer["sql"])
        print("\nDataFrame Result:\n", answer["dataframe"])
        print("\nExplanation:\n", answer["explanation"])
