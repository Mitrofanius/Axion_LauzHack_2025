from pathlib import Path
from .utils import load_csv, to_date, to_datetime, cast_numeric

DATA_DIR = Path("./data")

def load_all_data() -> dict:
    data = {
        "account": load_csv(DATA_DIR / "account.csv"),
        "br_to_account": load_csv(DATA_DIR / "br_to_account.csv"),
        "business_rel": load_csv(DATA_DIR / "business_rel.csv"),
        "client_onboarding_notes": load_csv(DATA_DIR / "client_onboarding_notes.csv"),
        "partner": load_csv(DATA_DIR / "partner.csv"),
        "partner_country": load_csv(DATA_DIR / "partner_country.csv"),
        "partner_role": load_csv(DATA_DIR / "partner_role.csv"),
        "transactions": load_csv(DATA_DIR / "transactions.csv")
    }

    # Convert date columns
    data["account"] = to_date(data["account"], ["account_open_date", "account_close_date"])
    data["business_rel"] = to_date(data["business_rel"], ["br_open_date", "br_close_date"])
    data["partner"] = to_date(data["partner"], ["partner_open_date", "partner_close_date", "partner_birth_year"])
    data["partner_role"] = to_date(data["partner_role"], ["relationship_start_date", "relationship_end_date"])
    data["transactions"] = to_datetime(data["transactions"], ["Date"])

    # Cast numeric columns
    data["br_to_account"] = cast_numeric(data["br_to_account"], ["relationship_status_code"])
    data["partner_country"] = cast_numeric(data["partner_country"], ["partner_country_status_code"])
    data["transactions"] = cast_numeric(data["transactions"], ["Amount", "Balance"])

    return data
