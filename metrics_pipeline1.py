import pandas as pd
import os
from datetime import datetime

# ------------------- Utility -------------------
def debug(message):
    print(f"🔍 DEBUG: {message}")

def ensure_month_format(date_col):
    """
    Ensures month column is formatted as 'YYYY-MM'.
    """
    return pd.to_datetime(date_col, errors='coerce').dt.to_period('M').astype(str)

def save_parquet(df, path):
    """
    Save DataFrame as Parquet, preferring pyarrow but falling back to fastparquet.
    """
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        debug("⚠️ pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

# ------------------- Metrics Calculations -------------------
def calculate_mrr_components(df):
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

def calculate_ebitda(df_mrr, df_opex, df_cogs):
    df = df_mrr.merge(df_opex, on='month', how='outer').merge(df_cogs, on='month', how='outer').fillna(0)
    df['ebitda'] = df['mrr'] - (df['opex'] + df['cogs'])
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

    debug(f"Loaded datasets -> Invoices: {df_invoices.shape}, Purchases: {df_purchases.shape}, Contacts: {df_contacts.shape}, ChartMogul: {df_mrr_components.shape}")

    df_mrr = calculate_mrr_components(df_mrr_components)
    df_opex = calculate_tagged_costs(df_purchases, df_contacts, 'opex', 'opex')
    df_cogs = calculate_tagged_costs(df_purchases, df_contacts, 'cogs', 'cogs')
    df_cac = calculate_cac(df_purchases, df_contacts, df_invoices)
    df_ebitda = calculate_ebitda(df_mrr, df_opex, df_cogs)
    df_burn_runway = calculate_burn_rate_and_runway(df_ebitda)

    # Merge all metrics
    dfs = [df_mrr, df_opex, df_cogs, df_cac, df_ebitda, df_burn_runway]
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
