import pandas as pd
import os
from datetime import datetime

# ================================================================
# 📊 FINAL METRICS PIPELINE – UPDATED (WITH ARPA, LTV & CHURN)
# ================================================================
# This script consolidates data from ChartMogul and Holded to create
# a monthly business metrics dataset. It merges key KPIs like MRR,
# ARR, CAC, LTV, ARPA, Churn, and operational costs into one CSV and
# Parquet file for analysis.
#
# ------------------------------------------------
# 🔍 **METRICS AND THEIR SOURCES**
# ------------------------------------------------
#
# 1) **MRR (Monthly Recurring Revenue)**
#    - **Formula:** sum(mrr)
#    - **Source:** chartmogul_mrr_components_clean.csv, `mrr`
#
# 2) **ARR (Annual Recurring Revenue)**
#    - **Formula:** MRR * 12
#
# 3) **New MRR**
#    - **Formula:** sum(mrr-new-business)
#    - **Source:** chartmogul_mrr_components_clean.csv
#
# 4) **Expansion MRR**
#    - **Formula:** sum(mrr-expansion)
#
# 5) **Contraction MRR**
#    - **Formula:** sum(mrr-contraction)
#
# 6) **Churned MRR**
#    - **Formula:** sum(mrr-churn)
#
# 7) **OPEX (Operational Expenses)**
#    - **Formula:** sum(total) for purchases tagged 'opex'
#    - **Source:** holded_purchases_clean.csv + holded_contacts_clean.csv
#
# 8) **COGS (Cost of Goods Sold)**
#    - **Formula:** sum(total) for purchases tagged 'cogs'
#
# 9) **Financial Costs**
#    - **Formula:** sum(total) for purchases tagged 'financial'
#
# 10) **CAC (Customer Acquisition Cost)**
#     - **Formula:** CAC costs / new_customers
#     - **Source:** holded_purchases_clean.csv (with 'cac' tag) + holded_invoices_clean.csv
#
# 11) **CAC Breakdown**
#     - **Details:** `new_customers` and `cac_costs` per month
#
# 12) **ARPA (Average Revenue Per Account)**
#     - **Formula:** MRR / active_customers
#     - **Source:** chartmogul_customers_clean.csv (active `uuid`)
#
# 13) **Customer Churn Rate**
#     - **Formula:** (Churned Customers / Active Customers) * 100
#     - **Source:** chartmogul_customers_clean.csv
#
# 14) **Revenue Churn Rate**
#     - **Formula:** (Churned MRR / MRR) * 100
#
# 15) **LTV (Customer Lifetime Value)**
#     - **Formula:** ARPA / churn_rate (approximation)
#
# 16) **EBITDA**
#     - **Formula:** MRR - (OPEX + COGS + Financial Costs)
#
# 17) **Burn Rate**
#     - **Formula:** |EBITDA| if EBITDA < 0 else 0
#
# 18) **Runway**
#     - **Formula:** cash_balance / burn_rate
#
# ------------------------------------------------
# 💾 OUTPUT:
#     - data/OUTPUT/YYYY-MM/final_metrics_YYYY-MM.csv
#     - data/OUTPUT/YYYY-MM/final_metrics_YYYY-MM.parquet
# ================================================================

import pandas as pd
import os
from datetime import datetime

# ------------------- Utility -------------------

def debug(message):
    print(f"🔍 DEBUG: {message}")

def ensure_month_format(date_col):
    """Ensures month column is formatted as 'YYYY-MM'."""
    return pd.to_datetime(date_col, errors='coerce').dt.to_period('M').astype(str)

def save_parquet(df, path):
    """Save DataFrame as Parquet, preferring pyarrow but falling back to fastparquet."""
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        debug("⚠️ pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

# ------------------- Metrics Calculations -------------------

def calculate_mrr_components(df):
    """
    Calculates MRR, Expansion, Contraction, Churned, New MRR, and ARR.
    """
    df.rename(columns={
        'mrr-expansion': 'expansion_mrr',
        'mrr-contraction': 'contraction_mrr',
        'mrr-churn': 'churned_mrr',
        'mrr-new-business': 'new_mrr'
    }, inplace=True)
    df['month'] = ensure_month_format(df['date'])
    df = df.groupby('month', as_index=False)[['mrr', 'expansion_mrr', 'contraction_mrr', 'churned_mrr', 'new_mrr']].sum()
    df['arr'] = df['mrr'] * 12
    return df

def calculate_tagged_costs(df_purchases, df_contacts, tag, col_name):
    """Filters purchases where contacts have a tag (e.g., 'opex') and sums totals per month."""
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
    """
    Calculates CAC (Customer Acquisition Cost).
    """
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
    """
    ARPA = MRR / Active Customers.
    """
    df_customers['month'] = ensure_month_format(df_customers['customer-since'])
    active_customers = df_customers.groupby('month')['uuid'].nunique().reset_index()
    active_customers.rename(columns={'uuid': 'active_customers'}, inplace=True)

    df = pd.merge(df_mrr[['month', 'mrr']], active_customers, on='month', how='left').fillna(0)
    df['arpa'] = df.apply(lambda row: row['mrr'] / row['active_customers'] if row['active_customers'] > 0 else 0, axis=1)
    return df[['month', 'arpa']]

def calculate_churn_rates(df_mrr, df_customers):
    """
    Calculates Customer Churn Rate and Revenue Churn Rate.
    """
    df_customers['month'] = ensure_month_format(df_customers['customer-since'])
    churned_customers = df_customers[df_customers['status'].str.lower() == 'churned'] \
                        .groupby('month')['uuid'].nunique().reset_index()
    churned_customers.rename(columns={'uuid': 'churned_customers'}, inplace=True)

    active_customers = df_customers.groupby('month')['uuid'].nunique().reset_index()
    active_customers.rename(columns={'uuid': 'active_customers'}, inplace=True)

    df_churn = pd.merge(active_customers, churned_customers, on='month', how='left').fillna(0)
    df_churn['customer_churn_rate'] = df_churn.apply(
        lambda row: (row['churned_customers'] / row['active_customers']) * 100 if row['active_customers'] > 0 else 0, axis=1)

    df_mrr['revenue_churn_rate'] = df_mrr.apply(
        lambda row: (row['churned_mrr'] / row['mrr']) * 100 if row['mrr'] > 0 else 0, axis=1)

    return df_churn[['month', 'customer_churn_rate']], df_mrr[['month', 'revenue_churn_rate']]

def calculate_ltv(df_arpa, df_customer_churn):
    """
    LTV = ARPA / Churn Rate (monthly churn).
    """
    df = pd.merge(df_arpa, df_customer_churn, on='month', how='left').fillna(0)
    df['ltv'] = df.apply(lambda row: row['arpa'] / (row['customer_churn_rate'] / 100) if row['customer_churn_rate'] > 0 else 0, axis=1)
    return df[['month', 'ltv']]

def calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs):
    df = df_mrr.merge(df_opex, on='month', how='outer') \
               .merge(df_cogs, on='month', how='outer') \
               .merge(df_financial_costs, on='month', how='outer') \
               .fillna(0)
    df['ebitda'] = df['mrr'] - (df['opex'] + df['cogs'] + df['financial_costs'])
    return df[['month', 'ebitda']]

def calculate_burn_rate_and_runway(df_ebitda, cash_balance=10000):
    df = df_ebitda.copy()
    df['burn_rate'] = df['ebitda'].apply(lambda x: abs(x) if x < 0 else 0)
    df['runway'] = df['burn_rate'].apply(lambda x: (cash_balance / x) if x > 0 else float('inf'))
    return df[['month', 'burn_rate', 'runway']]

# ------------------- Pipeline -------------------

def run_pipeline():
    debug("Loading input datasets...")
    df_invoices = pd.read_csv('data/INPUT/holded_invoices/clean/holded_invoices_clean.csv')
    df_purchases = pd.read_csv('data/INPUT/holded_purchases/clean/holded_purchases_clean.csv')
    df_contacts = pd.read_csv('data/INPUT/holded_contacts/clean/holded_contacts_clean.csv')
    df_mrr_components = pd.read_csv('data/INPUT/chartmogul_mrr_components/clean/chartmogul_mrr_components_clean.csv')
    df_customers = pd.read_csv('data/INPUT/chartmogul_customers/clean/chartmogul_customers_clean.csv')

    debug(f"Loaded datasets -> Invoices: {df_invoices.shape}, Purchases: {df_purchases.shape}, Contacts: {df_contacts.shape}, MRR: {df_mrr_components.shape}, Customers: {df_customers.shape}")

    # Core metrics
    df_mrr = calculate_mrr_components(df_mrr_components)
    df_opex = calculate_tagged_costs(df_purchases, df_contacts, 'opex', 'opex')
    df_cogs = calculate_tagged_costs(df_purchases, df_contacts, 'cogs', 'cogs')
    df_financial_costs = calculate_tagged_costs(df_purchases, df_contacts, 'financial', 'financial_costs')
    df_cac = calculate_cac(df_purchases, df_contacts, df_invoices)

    # New metrics
    df_arpa = calculate_arpa(df_mrr, df_customers)
    df_customer_churn, df_revenue_churn = calculate_churn_rates(df_mrr, df_customers)
    df_ltv = calculate_ltv(df_arpa, df_customer_churn)

    df_ebitda = calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs)
    df_burn_runway = calculate_burn_rate_and_runway(df_ebitda)

    # Merge all metrics
    dfs = [df_mrr, df_opex, df_cogs, df_financial_costs, df_cac, df_arpa, df_customer_churn, df_revenue_churn, df_ltv, df_ebitda, df_burn_runway]
    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = pd.merge(df_final, df, on='month', how='outer')
    df_final = df_final.fillna(0)

    # Save output
    current_month = datetime.now().strftime("%Y-%m")
    output_dir = os.path.join("data", "OUTPUT", current_month)
    os.makedirs(output_dir, exist_ok=True)

    output_file_csv = os.path.join(output_dir, f"final_metrics_{current_month}.csv")
    output_file_parquet = os.path.join(output_dir, f"final_metrics_{current_month}.parquet")

    df_final.to_csv(output_file_csv, index=False)
    save_parquet(df_final, output_file_parquet)

    debug(f"Metrics saved at {output_file_csv} and {output_file_parquet}")

if __name__ == "__main__":
    run_pipeline()
