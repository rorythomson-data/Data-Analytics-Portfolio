# metrics_pipeline.py
# 📊 FINAL METRICS PIPELINE – UPDATED JULY 2025
# --------------------------------------------
# Improved logic for churn calculation based on actual ChartMogul values.
# Fixes revenue churn rate negatives and includes better inline documentation.

# ============================================================================
# 📊 FINAL METRICS PIPELINE – METRIC CREATION EXPLAINED (FORMULAS + PROCESS)
# ============================================================================
# This script builds a monthly SaaS metrics dataset from cleaned data sources:
# ChartMogul (MRR, Customers) and Holded (Purchases, Invoices, Contacts).
# Below are the precise formulas and step-by-step data operations used for 
# each metric.

# --------------------------------------------------------------------------
# 1. 📈 MRR, ARR, MRR Components
#    ─────────────────────────────
#    FORMULAS:
#    • new_mrr, expansion_mrr, contraction_mrr, churned_mrr ➜ renamed from 
#      ChartMogul MRR columns
#    • mrr = monthly recurring revenue from ChartMogul
#    • arr = mrr * 12
#
#    STEPS:
#    • Source: df_mrr_components
#    • Columns used: 'date', 'mrr', 'mrr-new-business', 'mrr-expansion', 
#                    'mrr-contraction', 'mrr-churn'
#    • Rename component columns to standard names
#    • Extract month from 'date' → new 'month' column
#    • Group by 'month' and sum all relevant columns
#    • Create 'arr' column by multiplying 'mrr' by 12
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 2. 💸 OPEX, COGS, FINANCIAL COSTS
#    ───────────────────────────────
#    FORMULA:
#    • <metric> = sum of 'total' from purchases tagged accordingly
#
#    STEPS (same for each category):
#    • Source: df_purchases (purchase records), df_contacts (supplier tags)
#    • Join via: df_purchases['contact'] ∈ df_contacts['id']
#    • Tags:
#        - 'opex' in df_contacts['tags'] ➜ OPEX
#        - 'cogs' in df_contacts['tags'] ➜ COGS
#        - 'costes financieros' ➜ Financial Costs
#    • Filter purchases made to those contacts
#    • Extract 'month' from df_purchases['date']
#    • Group by 'month', sum df_purchases['total']
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 3. 🎯 CAC (Customer Acquisition Cost)
#    ───────────────────────────────────
#    FORMULA:
#    • cac_costs = sum of CAC-related purchases
#    • new_customers = count of first invoice per contact (per month)
#    • cac = cac_costs / new_customers
#
#    STEPS:
#    • df_contacts ➜ get contact IDs tagged with 'cac'
#    • df_purchases ➜ filter by those IDs ➜ sum 'total' per month
#    • df_invoices ➜ sort by ['contact', 'date'], drop duplicates on 'contact'
#                   ➜ count 'contact' per month to find new customers
#    • Merge cac_costs + new_customers on 'month'
#    • Calculate: cac = cac_costs / new_customers (0 if denominator = 0)
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 4. 💰 ARPA (Average Revenue Per Account)
#    ──────────────────────────────────────
#    FORMULA:
#    • arpa = mrr / active_customers
#
#    STEPS:
#    • df_customers ➜ extract 'month' from 'customer-since'
#    • Count unique 'uuid' per month ➜ active_customers
#    • Merge with df_mrr['mrr'] on 'month'
#    • Calculate: arpa = mrr / active_customers (0 if denominator = 0)
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 5. 📉 CUSTOMER CHURN RATE
#    ─────────────────────────
#    FORMULA:
#    • customer_churn_rate = (churned_customers / active_customers) * 100
#
#    STEPS:
#    • df_customers ➜ extract 'month' from 'customer-since'
#    • churned_customers = count of 'uuid' where status == 'Cancelled' per month
#    • active_customers = count of all 'uuid' per month
#    • Merge both by 'month' ➜ fill missing churns with 0
#    • Calculate rate: churned / active * 100
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 6. 📉 REVENUE CHURN RATE
#    ───────────────────────
#    FORMULA:
#    • revenue_churn_rate = (churned_mrr / mrr) * 100
#
#    STEPS:
#    • df_mrr ➜ use monthly 'churned_mrr' and 'mrr'
#    • Calculate: revenue_churn_rate = churned_mrr / mrr * 100
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 7. 🔁 LTV (Customer Lifetime Value)
#    ─────────────────────────────────
#    FORMULA:
#    • ltv = arpa / (customer_churn_rate / 100)
#
#    STEPS:
#    • Merge df_arpa with df_customer_churn on 'month'
#    • Calculate: arpa / (churn_rate / 100) ➜ 0 if churn_rate is 0
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 8. 🧾 EBITDA
#    ───────────
#    FORMULA:
#    • ebitda = mrr - (opex + cogs + financial_costs)
#
#    STEPS:
#    • Merge df_mrr with df_opex, df_cogs, df_financial_costs on 'month'
#    • Fill missing values with 0
#    • Calculate: ebitda = mrr - (opex + cogs + financial_costs)
# --------------------------------------------------------------------------

# --------------------------------------------------------------------------
# 9. 🔥 BURN RATE & RUNWAY
#    ───────────────────────
#    FORMULAS:
#    • burn_rate = |ebitda| if ebitda < 0, else 0
#    • runway = cash_balance / burn_rate  (∞ if burn = 0)
#
#    STEPS:
#    • From df_ebitda
#    • Apply logic row by row
#    • Uses a static `cash_balance` input (default = 10,000)
# --------------------------------------------------------------------------

# ============================================================================


import pandas as pd
import os
import argparse
from datetime import datetime

# ------------------- Debug Utility -------------------
def debug(message):
    print(f"🔍 DEBUG: {message}")

# ------------------- Helper Functions -------------------
def ensure_month_format(date_col):
    """Convert a date column to YYYY-MM format string."""
    return pd.to_datetime(date_col, errors='coerce').dt.to_period('M').astype(str)

def save_parquet(df, path):
    """Save a DataFrame to Parquet using pyarrow or fastparquet."""
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        debug("⚠️ pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

def validate_columns(df, required, df_name):
    """Ensure that required columns exist in the DataFrame."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing columns: {missing}")

# ------------------- Metric Calculations -------------------
def calculate_mrr_components(df):
    """Aggregate and rename MRR components by month."""
    df.rename(columns={
        'mrr-expansion': 'expansion_mrr',
        'mrr-contraction': 'contraction_mrr',
        'mrr-churn': 'churned_mrr',
        'mrr-new-business': 'new_mrr'
    }, inplace=True)
    df['month'] = ensure_month_format(df['date'])
    df['churned_mrr'] = df['churned_mrr'].abs()
    df = df.groupby('month', as_index=False)[
        ['mrr', 'expansion_mrr', 'contraction_mrr', 'churned_mrr', 'new_mrr']
    ].sum()
    df['arr'] = df['mrr'] * 12
    return df

def calculate_tagged_costs(df_purchases, df_contacts, tag, col_name):
    """Calculate costs by supplier tag (Opex, CoGs, etc)."""
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    tag_ids = df_contacts[df_contacts['tags'].str.contains(tag, case=False, na=False)]['id']
    df = df_purchases[df_purchases['contact'].isin(tag_ids)].copy()
    if df.empty:
        debug(f"No purchases found for tag '{tag}'. Defaulting to 0.")
        return pd.DataFrame({'month': [], col_name: []})
    df['month'] = ensure_month_format(df['date'])
    df = df.groupby('month', as_index=False)['total'].sum()
    df.rename(columns={'total': col_name}, inplace=True)
    return df

def calculate_cac(df_purchases, df_contacts, df_invoices):
    """Calculate Customer Acquisition Cost (CAC) and new customer count."""
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    cac_ids = df_contacts[df_contacts['tags'].str.contains('cac', case=False, na=False)]['id']
    df_cac = df_purchases[df_purchases['contact'].isin(cac_ids)].copy()
    if not df_cac.empty:
        df_cac['month'] = ensure_month_format(df_cac['date'])
        df_cac = df_cac.groupby('month', as_index=False)['total'].sum()
        df_cac.rename(columns={'total': 'cac_costs'}, inplace=True)
    else:
        debug("No CAC purchases found. Defaulting to 0.")
        df_cac = pd.DataFrame(columns=['month', 'cac_costs'])

    df_invoices['month'] = ensure_month_format(df_invoices['date'])
    first_invoices = df_invoices.sort_values(by=['contact', 'date']).drop_duplicates(subset='contact', keep='first')
    new_customers = first_invoices.groupby('month')['contact'].nunique().reset_index()
    new_customers.rename(columns={'contact': 'new_customers'}, inplace=True)

    df = pd.merge(new_customers, df_cac, on='month', how='outer').fillna(0)
    df['cac'] = df.apply(lambda row: row['cac_costs'] / row['new_customers'] if row['new_customers'] > 0 else 0, axis=1)
    return df

def calculate_arpa(df_mrr, df_customers):
    """Calculate ARPA (Average Revenue Per Account)."""
    df_customers['month'] = ensure_month_format(df_customers['customer-since'])
    active_customers = df_customers.groupby('month')['uuid'].nunique().reset_index()
    active_customers.rename(columns={'uuid': 'active_customers'}, inplace=True)

    df = pd.merge(df_mrr[['month', 'mrr']], active_customers, on='month', how='left').fillna(0)
    df['arpa'] = df.apply(lambda row: row['mrr'] / row['active_customers'] if row['active_customers'] > 0 else 0, axis=1)
    return df[['month', 'arpa', 'active_customers']]

def calculate_churn_rates(df_mrr, df_customers):
    """Calculate customer churn and revenue churn."""
    df_customers['month'] = ensure_month_format(df_customers['customer-since'])

    churned_customers = df_customers[df_customers['status'].str.lower() == 'cancelled'] \
                        .groupby('month')['uuid'].nunique().reset_index()
    churned_customers.rename(columns={'uuid': 'churned_customers'}, inplace=True)

    active_customers = df_customers.groupby('month')['uuid'].nunique().reset_index()
    active_customers.rename(columns={'uuid': 'active_customers'}, inplace=True)

    df_churn = pd.merge(active_customers, churned_customers, on='month', how='left').fillna(0)
    df_churn['customer_churn_rate'] = df_churn.apply(
        lambda row: (row['churned_customers'] / row['active_customers']) * 100 if row['active_customers'] > 0 else 0, axis=1)

    df_mrr['revenue_churn_rate'] = df_mrr.apply(
        lambda row: (row['churned_mrr'] / row['mrr']) * 100 if row['mrr'] > 0 else 0, axis=1)

    return df_churn[['month', 'churned_customers', 'customer_churn_rate']], df_mrr[['month', 'revenue_churn_rate']]

def calculate_ltv(df_arpa, df_customer_churn):
    """Calculate LTV as ARPA / Churn Rate."""
    df = pd.merge(df_arpa, df_customer_churn, on='month', how='left').fillna(0)
    df['ltv'] = df.apply(lambda row: row['arpa'] / (row['customer_churn_rate'] / 100) if row['customer_churn_rate'] > 0 else 0, axis=1)
    return df[['month', 'ltv']]

def calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs):
    """Calculate EBITDA from MRR - Expenses."""
    df = df_mrr.merge(df_opex, on='month', how='outer') \
               .merge(df_cogs, on='month', how='outer') \
               .merge(df_financial_costs, on='month', how='outer') \
               .fillna(0)
    df['ebitda'] = df['mrr'] - (df['opex'] + df['cogs'] + df['financial_costs'])
    return df[['month', 'ebitda']]

def calculate_burn_rate_and_runway(df_ebitda, cash_balance=10000):
    """Estimate burn rate and runway based on negative EBITDA and given cash reserve."""
    df = df_ebitda.copy()
    df['burn_rate'] = df['ebitda'].apply(lambda x: abs(x) if x < 0 else 0)
    df['runway'] = df['burn_rate'].apply(lambda x: (cash_balance / x) if x > 0 else float('inf'))
    return df[['month', 'burn_rate', 'runway']]

# ------------------- Main Pipeline -------------------
def run_pipeline(cash_balance):
    debug("Loading input datasets...")
    df_invoices = pd.read_csv('data/INPUT/holded_invoices/clean/holded_invoices_clean.csv')
    df_purchases = pd.read_csv('data/INPUT/holded_purchases/clean/holded_purchases_clean.csv')
    df_contacts = pd.read_csv('data/INPUT/holded_contacts/clean/holded_contacts_clean.csv')
    df_mrr_components = pd.read_csv('data/INPUT/chartmogul_mrr_components/clean/chartmogul_mrr_components_clean.csv')
    df_customers = pd.read_csv('data/INPUT/chartmogul_customers/clean/chartmogul_customers_clean.csv')

    validate_columns(df_contacts, ['id', 'tags'], "Contacts")
    validate_columns(df_purchases, ['date', 'contact', 'total'], "Purchases")

    # Compute metrics
    df_mrr = calculate_mrr_components(df_mrr_components)
    df_opex = calculate_tagged_costs(df_purchases, df_contacts, 'opex', 'opex')
    df_cogs = calculate_tagged_costs(df_purchases, df_contacts, 'cogs', 'cogs')
    df_financial_costs = calculate_tagged_costs(df_purchases, df_contacts, 'costes financieros', 'financial_costs')
    df_cac = calculate_cac(df_purchases, df_contacts, df_invoices)
    df_arpa = calculate_arpa(df_mrr, df_customers)
    df_customer_churn, df_revenue_churn = calculate_churn_rates(df_mrr, df_customers)
    df_ltv = calculate_ltv(df_arpa, df_customer_churn)
    df_ebitda = calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs)
    df_burn_runway = calculate_burn_rate_and_runway(df_ebitda, cash_balance)

    # Merge all
    dfs = [
        df_mrr, df_opex, df_cogs, df_financial_costs,
        df_cac, df_arpa, df_customer_churn, df_revenue_churn,
        df_ltv, df_ebitda, df_burn_runway
    ]

    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = pd.merge(df_final, df, on='month', how='outer')

    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    df_final = df_final.fillna(0)
    df_final['month'] = pd.to_datetime(df_final['month'], format="%Y-%m")
    df_final = df_final.sort_values(by='month')
    df_final['month'] = df_final['month'].dt.strftime("%Y-%m")

    # Reorder columns
    preferred_order = [
        'month', 'mrr', 'arr', 'new_mrr', 'expansion_mrr', 'contraction_mrr', 'churned_mrr',
        'opex', 'cogs', 'financial_costs',
        'cac_costs', 'new_customers', 'cac',
        'arpa', 'active_customers',
        'churned_customers', 'customer_churn_rate', 'revenue_churn_rate',
        'ltv', 'ebitda', 'burn_rate', 'runway'
    ]
    ordered_columns = [col for col in preferred_order if col in df_final.columns]
    df_final = df_final[ordered_columns]

    # Save to disk
    current_month = datetime.now().strftime("%Y-%m")
    output_dir = os.path.join("data", "OUTPUT", current_month)
    os.makedirs(output_dir, exist_ok=True)

    csv_path = os.path.join(output_dir, f"final_metrics_{current_month}.csv")
    parquet_path = os.path.join(output_dir, f"final_metrics_{current_month}.parquet")
    df_final.to_csv(csv_path, index=False)
    save_parquet(df_final, parquet_path)

    debug(f"Metrics saved at {csv_path} and {parquet_path}")

    summary_stats = df_final.describe().round(2)
    summary_path = os.path.join(output_dir, f"summary_stats_{current_month}.csv")
    summary_stats.to_csv(summary_path)
    debug(f"Summary saved at {summary_path}")

# ------------------- Entry Point -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cash", type=float, default=10000)
    args = parser.parse_args()
    run_pipeline(cash_balance=args.cash)
