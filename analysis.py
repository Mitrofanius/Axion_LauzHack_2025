import pandas as pd
from matplotlib.gridspec import GridSpec
import matplotlib.pyplot as plt


def get_partner_account_ids(partner: str, run_sql) -> list[str]:
	sql = f"""
		SELECT DISTINCT br_to_account.account_id
		FROM partner
		JOIN partner_role ON partner.partner_id = partner_role.partner_id AND partner_role.entity_type = 'BR'
		JOIN business_rel ON partner_role.entity_id = business_rel.br_id
		JOIN br_to_account ON business_rel.br_id = br_to_account.br_id
		WHERE partner.partner_name = '{partner}' LIMIT 1000

	"""
	df = run_sql(sql)
	return df['account_id'].tolist()

def get_account_transactions(account_id: str, run_sql) -> list[dict]:
	sql = f"""
	SELECT "Transaction ID","Debit/Credit","Account ID","Amount","Balance","Currency","Date","Transfer_Type","counterparty_Account_ID","ext_counterparty_Account_ID","ext_counterparty_country"
	FROM transactions
	WHERE "Account ID" = '{account_id}'
	"""
	df = run_sql(sql)
	return df

def analyse_transactions(transactions: list[pd.DataFrame]) -> dict:
	df = pd.concat(transactions, ignore_index=True)
	countries = df["ext_counterparty_country"]
	country_counts = countries.value_counts().to_dict()
	amounts = df["Amount"]
	IBAN_counts = df["ext_counterparty_Account_ID"].value_counts().to_dict()
	return {
		"countries": country_counts,
		"amounts": amounts,
		"IBANs": IBAN_counts
	}

def make_analysis_plot(analysis: dict):
	""" Figure with 3 subplots arranged as:
		- Row 1: Countries pie chart (left) and IBANs pie chart (right)
		- Row 2: Histogram of amounts (spans both columns)
	"""

	fig = plt.figure(figsize=(14, 10))
	gs = GridSpec(2, 2, figure=fig, height_ratios=[1, 1])

	# Countries pie chart (left, row 0 col 0)
	ax_countries = fig.add_subplot(gs[0, 0])
	countries = analysis.get("countries", {})
	labels_c = list(countries.keys())
	sizes_c = list(countries.values())
	if not sizes_c:
		ax_countries.text(0.5, 0.5, "No data", ha='center', va='center')
		ax_countries.set_title("Transactions by Country")
		ax_countries.axis('off')
	else:
		ax_countries.pie(sizes_c, labels=labels_c, autopct='%1.1f%%', startangle=90)
		ax_countries.set_title("Transactions by Country")
		ax_countries.axis('equal')

	# IBANs pie chart (right, row 0 col 1)
	ax_ibans = fig.add_subplot(gs[0, 1])
	IBANs = analysis.get("IBANs", {})
	# keep top 10 IBANs for readability
	top_IBANs_items = sorted(IBANs.items(), key=lambda item: item[1], reverse=True)[:10]
	labels_i = [item[0] for item in top_IBANs_items]
	sizes_i = [item[1] for item in top_IBANs_items]
	if not sizes_i:
		ax_ibans.text(0.5, 0.5, "No data", ha='center', va='center')
		ax_ibans.set_title("Top External IBANs")
		ax_ibans.axis('off')
	else:
		ax_ibans.pie(sizes_i, labels=labels_i, autopct='%1.1f%%', startangle=90)
		ax_ibans.set_title("Top External IBANs (Top 10)")
		ax_ibans.axis('equal')

	# Amounts histogram spanning both columns (row 1)
	ax_amounts = fig.add_subplot(gs[1, :])
	amounts = analysis.get("amounts", [])
	if getattr(amounts, "empty", False) or len(amounts) == 0:
		ax_amounts.text(0.5, 0.5, "No data", ha='center', va='center')
		ax_amounts.set_title("Transaction Amounts Distribution")
		ax_amounts.axis('off')
	else:
		# If amounts is a pandas Series, convert to numeric values
		try:
			vals = amounts.dropna().astype(float)
		except Exception:
			vals = [float(x) for x in amounts if x is not None]
		ax_amounts.hist(vals, bins=30, color='orange', edgecolor='black')
		ax_amounts.set_title("Transaction Amounts Distribution")
		ax_amounts.set_xlabel("Amount")
		ax_amounts.set_ylabel("Frequency")

	plt.tight_layout()
	plt.savefig("transaction_analysis.png")

def transactions_stats(partner: str, run_sql):
	account_ids = get_partner_account_ids(partner, run_sql)
	transactions = []
	for account_id in account_ids:
		transactions.append(get_account_transactions(account_id, run_sql))
	analysis = analyse_transactions(transactions)
	return analysis