import pandas as pd
import os
import argparse
from datetime import datetime


# ------------------- Debug Utility -------------------
def debug(message):
    print(f"DEBUG: {message}")

# ------------------- Helper Functions -------------------
def ensure_month_format(date_col):
    """Convert a date column to YYYY-MM format string."""
    return pd.to_datetime(date_col, errors='coerce').dt.to_period('M').astype(str)

def save_parquet(df, path):
    """Save a DataFrame to Parquet using pyarrow or fastparquet."""
    try:
        df.to_parquet(path, index=False, engine="pyarrow")
    except ImportError:
        debug("pyarrow not found, falling back to fastparquet.")
        df.to_parquet(path, index=False, engine="fastparquet")

def validate_columns(df, required, df_name):
    """Ensure that required columns exist in the DataFrame."""
    missing = [col for col in required if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing columns: {missing}")

# --------------------------------------------------------------------------
#                   SUMMARY OF METRIC CREATION PROCESS:
# --------------------------------------------------------------------------
# 1. MRR - Monthly Recurring Revenue (mrr)            * VALID: Taken directly from CM_Metrics
#    ───────────────────────────────────
#
# * Formula:
#     mrr = sum of recurring revenue (monthly) from all customers, aggregated from MRR components per month
#   Where:
#     mrr = total monthly recurring revenue from all active subscriptions at month-end
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date' → monthly timestamp
#         • 'mrr' → monthly recurring revenue
#
# * Calculation Steps:
#     1. Extract month from 'date' column → 'month'
#     2. Group by 'month' and sum 'mrr'
#
# * Assumptions / Filters:
#     - Assumes ChartMogul already filters for active subscriptions
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Extract 'month' from 'date'
#             └── Group by 'month' and sum 'mrr'
#
# * Notes for Verification:
#     - Optionally cross-validate against df_CM_metrics_clean['mrr']
#     - Check for missing or anomalous dates in source file
# --------------------------------------------------------------------------
def calculate_mrr(df):
    """
    Calculate Monthly Recurring Revenue (MRR) from ChartMogul MRR components.

    Parameters:
        df (pd.DataFrame): ChartMogul MRR components table in cleaned format

    Returns:
        pd.DataFrame: Monthly aggregated table containing:
            - 'month' (YYYY-MM)
            - 'mrr'
    """

    # ----------------------------------------------------------------------
    # STEP 1: Extract 'month' from 'date'
    # ----------------------------------------------------------------------
    # Converts the ChartMogul date column into a YYYY-MM format string.
    # This ensures consistent grouping and merging with other metrics.
    df_copy = df.copy()
    df_copy['month'] = ensure_month_format(df_copy['date'])

    # ----------------------------------------------------------------------
    # STEP 2: Aggregate total MRR by month
    # ----------------------------------------------------------------------
    # Groups data by 'month' and sums the 'mrr' column to produce a
    # single row per month representing total recurring revenue.
    df_out = df_copy.groupby('month', as_index=False)['mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 3: Return the aggregated table
    # ----------------------------------------------------------------------
    return df_out

# --------------------------------------------------------------------------
# 2. Expansion MRR            * VALID: Taken directly from CM_Metrics
#    ───────────────────────────────
#
# * Formula:
#     expansion_mrr = sum of positive revenue changes from existing customers
#   Where:
#     expansion_mrr = total monthly recurring revenue gained from upgrades, plan changes, or cross-sells to existing customers
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date'
#         • 'mrr-expansion'
#
# * Calculation Steps:
#     1. Rename 'mrr-expansion' to 'expansion_mrr'
#     2. Extract 'month' from 'date'
#     3. Convert to absolute value if negative (ChartMogul should only report positive values)
#     4. Group by 'month' and sum
#
# * Assumptions / Filters:
#     - Includes upgrades, plan changes, and cross-sells to existing customers
#     - Excludes new business, reactivations, or contractions
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Rename 'mrr-expansion' → 'expansion_mrr'
#             └── Extract 'month'
#                 └── abs(expansion_mrr) [if needed]
#                     └── Group and sum
#
# * Notes for Verification:
#     - Should match ChartMogul's "Expansion" component in the MRR movements report
#     - Check for months with unexpected zeros or negatives
# --------------------------------------------------------------------------
def calculate_expansion_mrr(df_mrr_components):
    """
    Calculate Expansion Monthly Recurring Revenue (expansion_mrr) 
    from existing customer upgrades and add-ons.
    """

    # ----------------------------------------------------------------------
    # STEP 1: Standardize the MRR expansion column name
    # ----------------------------------------------------------------------
    # ChartMogul exports this metric as 'mrr-expansion'.
    # We rename it to 'expansion_mrr' for:
    #   - Consistency across all functions
    #   - Easier referencing in downstream merges
    # Note:
    #   If df_mrr_components has already been processed by calculate_mrr_components,
    #   this column may already be named 'expansion_mrr'.
    #   In that case, this rename will have no effect (pandas will ignore it).
    df = df_mrr_components.rename(columns={'mrr-expansion': 'expansion_mrr'}).copy()

    # ----------------------------------------------------------------------
    # STEP 2: Extract 'month' in YYYY-MM format from 'date'
    # ----------------------------------------------------------------------
    # Converts the raw 'date' into a 'month' column for grouping.
    # This ensures that all expansion MRR values are aggregated monthly.
    df['month'] = ensure_month_format(df['date'])

    # ----------------------------------------------------------------------
    # STEP 3: Ensure expansion_mrr is positive
    # ----------------------------------------------------------------------
    # ChartMogul normally reports only positive values for expansion MRR,
    # but we enforce abs() here as a safeguard in case of data anomalies.
    df['expansion_mrr'] = df['expansion_mrr'].abs()

    # ----------------------------------------------------------------------
    # STEP 4: Aggregate total expansion MRR per month
    # ----------------------------------------------------------------------
    # Group by 'month' and sum all expansion MRR values to get:
    #   - Total monthly recurring revenue gained from upsells,
    #     plan upgrades, or cross-sells to existing customers.
    df = df.groupby('month', as_index=False)['expansion_mrr'].sum()

    # Return the monthly aggregated expansion MRR
    return df

# --------------------------------------------------------------------------
# 3. Contraction MRR            * VALID: Taken directly from CM_Metrics
#    ─────────────────────────────────
#
# * Formula:
#     contraction_mrr = sum of negative revenue changes from existing customers
#   Where:
#     contraction_mrr = total monthly recurring revenue lost from downgrades or partial cancellations by existing customers
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date'
#         • 'mrr-contraction'
#
# * Calculation Steps:
#     1. Rename 'mrr-contraction' to 'contraction_mrr'
#     2. Extract 'month' from 'date'
#     3. Convert to absolute value (ChartMogul reports as negative)
#     4. Group by 'month' and sum
#
# * Assumptions / Filters:
#     - Includes downgrades and partial cancellations by existing customers
#     - Excludes churned MRR (full cancellations) and new business
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Rename 'mrr-contraction' → 'contraction_mrr'
#             └── Extract 'month'
#                 └── abs(contraction_mrr)
#                     └── Group and sum
#
# * Notes for Verification:
#     - Should match ChartMogul's "Contraction" component in the MRR movements report
#     - Compare with churned_mrr to ensure no overlaps
# --------------------------------------------------------------------------
def calculate_contraction_mrr(df_mrr_components):
    """
    Calculate Contraction Monthly Recurring Revenue (contraction_mrr)
    from existing customer downgrades or partial cancellations.

    Parameters:
        df_mrr_components (pd.DataFrame): Cleaned ChartMogul MRR components data.

    Returns:
        pd.DataFrame: DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'contraction_mrr' (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Rename the ChartMogul contraction column to 'contraction_mrr'
    # ----------------------------------------------------------------------
    # This aligns with the naming conventions used across the pipeline.
    # If the column is already renamed, pandas will silently ignore it.
    df = df_mrr_components.rename(columns={'mrr-contraction': 'contraction_mrr'}).copy()

    # ----------------------------------------------------------------------
    # STEP 2: Extract 'month' in YYYY-MM format from 'date'
    # ----------------------------------------------------------------------
    # Ensures monthly granularity for all time-series metric aggregation.
    df['month'] = ensure_month_format(df['date'])

    # ----------------------------------------------------------------------
    # STEP 3: Convert contraction_mrr values to positive
    # ----------------------------------------------------------------------
    # ChartMogul exports these values as negatives (losses), but we
    # want to store them as positive numbers for reporting clarity.
    df['contraction_mrr'] = df['contraction_mrr'].abs()

    # ----------------------------------------------------------------------
    # STEP 4: Group by 'month' and sum the contraction values
    # ----------------------------------------------------------------------
    # This results in one row per month with the total amount of MRR
    # lost to partial downgrades or cancellations.
    df = df.groupby('month', as_index=False)['contraction_mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 5: Return the final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 4. New MRR (New Monthly Recurring Revenue)            * VALID: Taken directly from CM_Metrics
#    ───────────────────────────────────────────
#
# * Formula:
#     new_mrr = sum of MRR added in the month from first-time paying customers
#   Where:
#     MRR added = monthly recurring revenue from customers who have never had an active subscription before
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date' → monthly timestamp
#         • 'mrr-new-business' → new revenue from brand new customers
#
# * Calculation Steps:
#     1. Rename 'mrr-new-business' to 'new_mrr' for consistency
#     2. Extract 'month' from 'date' column → 'month'
#     3. Group by 'month' and sum 'new_mrr'
#
# * Assumptions / Filters:
#     - 'mrr-new-business' includes only revenue from customers who have never subscribed before
#     - ChartMogul has already filtered out reactivations or expansions
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Rename 'mrr-new-business' → 'new_mrr'
#             └── Extract 'month' from 'date'
#                 └── Group by 'month' and sum
#                     └── Final: new_mrr per month
#
# * Notes for Verification:
#     - Should match values shown in ChartMogul UI under "New Business"
#     - No customer-level breakdown available from current schema
# --------------------------------------------------------------------------

def calculate_new_mrr(df_mrr_components):
    """
    Calculate New Monthly Recurring Revenue (new_mrr) from first-time customers.
    
    Parameters:
        df_mrr_components (pd.DataFrame): Cleaned ChartMogul MRR components data

    Returns:
        pd.DataFrame: DataFrame with 'month' and 'new_mrr' columns
    """
    # ----------------------------------------------------------------------
    # STEP 1: Rename the raw ChartMogul column for clarity and consistency
    # ----------------------------------------------------------------------
    # ChartMogul names the column 'mrr-new-business' for first-time customer MRR.
    # We rename it to 'new_mrr' to make it easier to reference consistently across the pipeline.
    # NOTE: This assumes df_mrr_components is in raw format; if it already has 'new_mrr',
    # the rename will simply have no effect.
    df = df_mrr_components.rename(columns={'mrr-new-business': 'new_mrr'})

    # ----------------------------------------------------------------------
    # STEP 2: Convert the 'date' column to YYYY-MM month format
    # ----------------------------------------------------------------------
    # This standardises the month representation for grouping, regardless of the original date format.
    df['month'] = ensure_month_format(df['date'])

    # ----------------------------------------------------------------------
    # STEP 3: Aggregate total new MRR per month
    # ----------------------------------------------------------------------
    # Group the data by 'month' and sum all 'new_mrr' values.
    # If there are multiple rows for the same month (e.g., multiple customers),
    # this will aggregate them into a single monthly total.
    df = df.groupby('month', as_index=False)['new_mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 4: Return the monthly new MRR DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 5. Churned MRR            * VALID: Taken directly from CM_Metrics
#    ─────────────────────────────────
#
# * Formula:
#     churned_mrr = sum of MRR lost from customers who fully cancelled
#   Where:
#     churned_mrr = total monthly recurring revenue lost from customers who ended all active subscriptions
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date'
#         • 'mrr-churn'
#
# * Calculation Steps:
#     1. Rename 'mrr-churn' to 'churned_mrr'
#     2. Extract 'month' from 'date'
#     3. Convert to absolute value (ChartMogul reports as negative)
#     4. Group by 'month' and sum
#
# * Assumptions / Filters:
#     - Includes only **full cancellations** of subscriptions
#     - Excludes partial downgrades (contraction_mrr)
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Rename 'mrr-churn' → 'churned_mrr'
#             └── Extract 'month'
#                 └── abs(churned_mrr)
#                     └── Group and sum
#
# * Notes for Verification:
#     - Should match ChartMogul's "Churn" component in the MRR movements report
#     - Important for calculating revenue churn rate
# --------------------------------------------------------------------------

def calculate_churned_mrr(df_mrr_components):
    """
    Calculate Churned Monthly Recurring Revenue (churned_mrr)
    from customers who fully cancelled their subscriptions.

    Parameters:
        df_mrr_components (pd.DataFrame): Cleaned ChartMogul MRR components data.

    Returns:
        pd.DataFrame: 'month' and 'churned_mrr' columns
    """

    # ----------------------------------------------------------------------
    # STEP 1: Rename ChartMogul's churn column to 'churned_mrr'
    # ----------------------------------------------------------------------
    # - The raw ChartMogul export uses 'mrr-churn'
    # - We rename to 'churned_mrr' for consistency with naming conventions
    df = df_mrr_components.rename(columns={'mrr-churn': 'churned_mrr'}).copy()

    # ----------------------------------------------------------------------
    # STEP 2: Extract 'month' from the 'date' column
    # ----------------------------------------------------------------------
    # - Convert ChartMogul's full date to YYYY-MM format
    # - This ensures aggregation is done at a monthly level
    df['month'] = ensure_month_format(df['date'])

    # ----------------------------------------------------------------------
    # STEP 3: Ensure churned MRR is stored as a positive number
    # ----------------------------------------------------------------------
    # - ChartMogul reports churned MRR as negative values
    # - Taking the absolute value avoids negative metrics in reporting
    df['churned_mrr'] = df['churned_mrr'].abs()

    # ----------------------------------------------------------------------
    # STEP 4: Aggregate churned MRR by month
    # ----------------------------------------------------------------------
    # - Group data by 'month'
    # - Sum churned MRR for each month
    df = df.groupby('month', as_index=False)['churned_mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 6. Net New MRR (Net Monthly Recurring Revenue)            * VALID: Calculated using metrics taken directly from CM_Metrics
#    ───────────────────────────────────────────────
#
# * Formula:
#     net_new_mrr = new_mrr + expansion_mrr - contraction_mrr - churned_mrr
#   Where:
#     new_mrr = MRR from brand new customers in the month
#     expansion_mrr = additional MRR from upgrades by existing customers
#     contraction_mrr = MRR lost from downgrades by existing customers
#     churned_mrr = MRR lost from customers who cancelled all subscriptions
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date'
#         • 'mrr-new-business'
#         • 'mrr-expansion'
#         • 'mrr-contraction'
#         • 'mrr-churn'
#
# * Calculation Steps:
#     1. Rename relevant columns:
#        • 'mrr-new-business' → 'new_mrr'
#        • 'mrr-expansion' → 'expansion_mrr'
#        • 'mrr-contraction' → 'contraction_mrr'
#        • 'mrr-churn' → 'churned_mrr'
#     2. Extract 'month' from 'date'
#     3. Convert churned_mrr and contraction_mrr to absolute values (positive)
#     4. Group by 'month' and sum each component
#     5. Compute: net_new_mrr = new_mrr + expansion_mrr - contraction_mrr - churned_mrr
#
# * Assumptions / Filters:
#     - Assumes negative churn/contraction values from ChartMogul are flipped to positive
#     - Excludes reactivation MRR by definition (see note below)
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Rename MRR components
#             └── Extract 'month'
#                 └── abs(churned + contraction)
#                     └── Group and sum
#                         └── Apply net new MRR formula
#
# * Notes for Verification:
#     - Net New MRR may not exactly match: MRR[month] - MRR[month−1]
#       → Reason: ChartMogul includes "mrr-reactivation" in total MRR but **not** in Net New MRR
#     - This metric reflects pure growth from **new** and **existing** active customers,
#       excluding returning/cancelled users
# --------------------------------------------------------------------------

def calculate_net_new_mrr(df_mrr_components):
    """
    Calculate Net New Monthly Recurring Revenue:
    new_mrr + expansion_mrr - contraction_mrr - churned_mrr
    """

    # Step 1: Rename columns for consistency
    df = df_mrr_components.rename(columns={
        'mrr-new-business': 'new_mrr',
        'mrr-expansion': 'expansion_mrr',
        'mrr-contraction': 'contraction_mrr',
        'mrr-churn': 'churned_mrr'
    })

    # Step 2: Extract 'month' in YYYY-MM format
    df['month'] = ensure_month_format(df['date'])

    # Step 3: Convert churn-related values to positive
    df['churned_mrr'] = df['churned_mrr'].abs()
    df['contraction_mrr'] = df['contraction_mrr'].abs()

    # Step 4: Group and sum all relevant columns
    grouped = df.groupby('month', as_index=False)[
        ['new_mrr', 'expansion_mrr', 'contraction_mrr', 'churned_mrr']
    ].sum()

    # Step 5: Compute Net New MRR
    grouped['net_new_mrr'] = (
        grouped['new_mrr']
        + grouped['expansion_mrr']
        - grouped['contraction_mrr']
        - grouped['churned_mrr']
    )

    return grouped[['month', 'net_new_mrr']]

# --------------------------------------------------------------------------
# 7. ARR (Annual Recurring Revenue)            * VALID: Taken directly from CM_Metrics
#    ─────────────────────────────────
#
# * Formula:
#     arr = mrr * 12
#     Where:
#       mrr = Monthly Recurring Revenue for the month
#
# * Source Table(s) and Columns:
#     - df_CM_mrr_components_clean:
#         • 'date'
#         • 'mrr'
#
# * Calculation Steps:
#     1. Extract 'month' from 'date'
#     2. Group by 'month' and sum 'mrr'
#     3. Multiply monthly MRR by 12 to get ARR
#
# * Assumptions / Filters:
#     - Assumes MRR is already aggregated per customer at month-end
#     - Assumes no seasonality or expected churn is factored in (straight projection)
#
# * Flowchart:
#     df_CM_mrr_components_clean
#         └── Extract 'month'
#             └── Group and sum 'mrr'
#                 └── Multiply by 12 → arr
#
# * Notes for Verification:
#     - ARR should exactly equal MRR × 12 per month
#     - Matches reported ARR in df_CM_metrics_clean['arr']
# --------------------------------------------------------------------------

def calculate_arr(df_mrr_components):
    """
    Calculate Annual Recurring Revenue (ARR) from MRR values per month.

    ARR is calculated as:
        arr = mrr × 12
    where:
        mrr = Monthly Recurring Revenue for the month.

    Parameters:
        df_mrr_components (pd.DataFrame):
            Cleaned ChartMogul MRR components data containing:
            - 'date' (str/date): Date associated with MRR value
            - 'mrr' (float): Monthly Recurring Revenue

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'arr' (float): Annual Recurring Revenue
    """
    # ----------------------------------------------------------------------
    # STEP 1: Extract 'month' from the 'date' column
    # ----------------------------------------------------------------------
    # Converts the full date into a YYYY-MM string to allow
    # grouping at the monthly level.
    df = df_mrr_components.copy()
    df['month'] = ensure_month_format(df['date'])

    # ----------------------------------------------------------------------
    # STEP 2: Group by 'month' and sum MRR
    # ----------------------------------------------------------------------
    # Aggregates all MRR entries in the same month to get
    # the total monthly recurring revenue.
    monthly_mrr = df.groupby('month', as_index=False)['mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 3: Calculate ARR as MRR × 12
    # ----------------------------------------------------------------------
    # Projects the monthly MRR to a yearly value assuming no change
    # over the next 12 months.
    monthly_mrr['arr'] = monthly_mrr['mrr'] * 12

    # ----------------------------------------------------------------------
    # STEP 4: Return the 'month' and 'arr' columns only
    # ----------------------------------------------------------------------
    return monthly_mrr[['month', 'arr']]

# --------------------------------------------------------------------------
# 8. ARPA from Chartmogul           * VALID: Taken directly from CM_Metrics
#    ─────────────────────────────────────
#
# * Formula:
#     arpa = mrr / active_customers
#   Where:
#     mrr = Monthly Recurring Revenue for the month
#     active_customers = number of active paying customers in that month
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_metrics_clean:
#         • 'month_start' → first day of the month
#         • 'arpa' → average revenue per active account
#
# * Calculation Steps:
#     1. Extract 'month' from 'month_start' column
#     2. Select 'arpa' column
#     3. Rename 'arpa' to 'arpa'
#
# * Assumptions / Filters:
#     - Follows ChartMogul’s default logic:
#       arpa = MRR / Number of active paying customers in that month
#     - Accepts ChartMogul’s definition of “active customer” as authoritative
#
# * Flowchart:
#     df_CM_metrics_clean
#         └── Extract 'month' from 'month_start'
#             └── Select 'arpa' column
#                 └── Rename to arpa
#
# * Notes for Verification:
#     - Useful for high-level overview and benchmarking
#     - No customer-level granularity available
# --------------------------------------------------------------------------

def calculate_arpa(df_cm_metrics):
    """
    Extract ChartMogul-calculated Average Revenue Per Account (ARPA) per month.

    ARPA (arpa) is defined as:
        arpa = mrr / active_customers
    where:
        mrr = Monthly Recurring Revenue for the month
        active_customers = number of active paying customers in that month

    Parameters:
        df_cm_metrics (pd.DataFrame):
            Cleaned ChartMogul metrics table containing:
            - 'month_start' (str/date): First day of the month
            - 'arpa' (float): Average revenue per account for the month

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'arpa' (float): ChartMogul-calculated ARPA
    """

    # ----------------------------------------------------------------------
    # STEP 1: Validate required columns exist
    # ----------------------------------------------------------------------
    # Ensures 'month_start' and 'arpa' columns are present in df_cm_metrics
    validate_columns(df_cm_metrics, ['month_start', 'arpa'], "ChartMogul Metrics")

    # ----------------------------------------------------------------------
    # STEP 2: Work on a copy of the DataFrame
    # ----------------------------------------------------------------------
    # Prevents accidental modifications to the original DataFrame.
    df = df_cm_metrics.copy()

    # ----------------------------------------------------------------------
    # STEP 3: Extract 'month' from 'month_start'
    # ----------------------------------------------------------------------
    # Converts the first day of the month into YYYY-MM format for grouping
    # and merging with other metrics.
    df['month'] = ensure_month_format(df['month_start'])

    # ----------------------------------------------------------------------
    # STEP 4: Select relevant columns and rename 'arpa' to 'arpa'
    # ----------------------------------------------------------------------
    # Matches the naming convention used across the pipeline for ChartMogul ARPA.
    df = df[['month', 'arpa']]

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 9. Customer Count (customers)                        * VALID: Taken directly from CM_Metrics
#     ─────────────────────────────────────────────
#
# * Formula:
#     customers = total number of customers (monthly)
#
# * Source Table(s) and Columns:
#     - df_CM_metrics_clean:
#         • 'month_start' → first day of the month
#         • 'customers'   → total number of customers
#
# * Calculation Steps:
#     1. Extract 'month' from 'month_start' column
#     2. Select 'customers' column
#
# * Assumptions / Filters:
#     - Assumes ChartMogul's customer counts are correct for the given month
#
# * Flowchart:
#     df_CM_metrics_clean
#         └── Extract 'month' from 'month_start'
#             └── Select 'customers'
# --------------------------------------------------------------------------

def calculate_customers(df_cm_metrics):
    """
    Extract the number of customers per month as reported by ChartMogul.

    Parameters:
        df_cm_metrics (pd.DataFrame):
            Cleaned ChartMogul metrics data containing:
            - 'month_start' (str/date): First day of the month
            - 'customers' (int): Total number of active customers

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'customers' (int)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Validate required columns exist
    # ----------------------------------------------------------------------
    # Ensures that the input contains the required fields
    validate_columns(df_cm_metrics, ['month_start', 'customers'], "ChartMogul Metrics")

    # ----------------------------------------------------------------------
    # STEP 2: Work on a copy and extract 'month'
    # ----------------------------------------------------------------------
    # Avoids modifying the original DataFrame
    df = df_cm_metrics.copy()
    df['month'] = ensure_month_format(df['month_start'])

    # ----------------------------------------------------------------------
    # STEP 3: Select columns
    # ----------------------------------------------------------------------
    # Keeps only the customer count per month 
    df = df[['month', 'customers']]

    # ----------------------------------------------------------------------
    # STEP 4: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 10. Customer Churn Rate           * VALID: Taken directly from CM_Metrics
#    ───────────────────────────────────────────────────────────
#
# * Formula:
#     customer_churn_rate = (lost_customers / starting_customers) × 100
#   Where:
#     lost_customers = number of customers who cancelled all active subscriptions in the month
#     starting_customers = number of active customers at the start of the month
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_metrics_clean:
#         • 'month_start' → first day of the month
#         • 'customer-churn-rate' → churn % as calculated by ChartMogul
#
# * Calculation Steps:
#     1. Extract 'month' from 'month_start' column
#     2. Select 'customer-churn-rate' column
#     3. Rename it to 'customer_churn_rate
#
# * Assumptions / Filters:
#     - Follows ChartMogul’s default churn calculation:
#       (Lost Customers in Month / Customers at Start of Month) × 100
#     - Accepts ChartMogul’s definition of a “lost customer” as authoritative
#
# * Flowchart:
#     df_CM_metrics_clean
#         └── Extract 'month' from 'month_start'
#             └── Select 'customer-churn-rate'
#                 └── Rename to 'customer_churn_rate'
#
# * Notes for Verification:
#     - Compare with a local churn calculation for data validation if needed
# --------------------------------------------------------------------------

def calculate_customer_churn_rate(df_cm_metrics):
    """
    Calculate the monthly Customer Churn Rate (customer_churn_rate)
    as reported by ChartMogul.

    Customer Churn Rate is defined as:
        (lost_customers / starting_customers) × 100
    where:
        lost_customers     = number of customers who cancelled all active subscriptions in the month
        starting_customers = number of active customers at the start of the month

    Parameters:
        df_cm_metrics (pd.DataFrame):
            Cleaned ChartMogul metrics table containing:
            - 'month_start' (str/date): First day of the month
            - 'customer-churn-rate' (float): Monthly churn rate percentage

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'customer_churn_rate' (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Validate required columns exist
    # ----------------------------------------------------------------------
    # Ensures 'month_start' and 'customer-churn-rate' are present before processing
    validate_columns(df_cm_metrics, ['month_start', 'customer-churn-rate'], "ChartMogul Metrics")

    # ----------------------------------------------------------------------
    # STEP 2: Work on a copy of the DataFrame
    # ----------------------------------------------------------------------
    # Prevents accidental modification of the original DataFrame
    df = df_cm_metrics.copy()

    # ----------------------------------------------------------------------
    # STEP 3: Extract 'month' from 'month_start'
    # ----------------------------------------------------------------------
    # Converts the first day of the month into YYYY-MM format
    # for grouping/merging with other metrics
    df['month'] = ensure_month_format(df['month_start'])

    # ----------------------------------------------------------------------
    # STEP 4: Select relevant columns
    # ----------------------------------------------------------------------
    df = df[['month', 'customer-churn-rate']]

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 11. Revenue Churn Rate                * VALID: Taken directly from CM_Metrics
#    ──────────────────────────────────────────────────────────
#
# * Formula:
#     revenue_churn_rate = (churned_mrr / starting_mrr) × 100
#   Where:
#     churned_mrr = total MRR lost in the month due to cancellations or downgrades
#     starting_mrr = MRR at the beginning of the month
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_metrics_clean:
#         • 'month_start' → first day of the month
#         • 'mrr-churn-rate' → MRR churn % as calculated by ChartMogul
#
# * Calculation Steps:
#     1. Extract 'month' from 'month_start' column
#     2. Select 'mrr-churn-rate' column
#     3. Rename it to 'revenue_churn_rate'
#
# * Assumptions / Filters:
#     - Follows ChartMogul’s default MRR churn rate calculation:
#       (Churned MRR in Month / MRR at Start of Month) × 100
#     - Accepts ChartMogul’s definition of “churned MRR” as authoritative
#
# * Flowchart:
#     df_CM_metrics_clean
#         └── Extract 'month' from 'month_start'
#             └── Select 'mrr-churn-rate'
#                 └── Rename to 'revenue_churn_rate'
# --------------------------------------------------------------------------

def calculate_revenue_churn_rate(df_cm_metrics):
    """
    Calculate the monthly Revenue Churn Rate (revenue_churn_rate)
    as reported by ChartMogul.

    Revenue Churn Rate is defined as:
        (churned_mrr / starting_mrr) × 100
    where:
        churned_mrr  = total MRR lost in the month due to cancellations or downgrades
        starting_mrr = MRR at the beginning of the month

    Parameters:
        df_cm_metrics (pd.DataFrame):
            Cleaned ChartMogul metrics table containing:
            - 'month_start' (str/date): First day of the month
            - 'mrr-churn-rate' (float): Monthly MRR churn rate percentage

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'revenue_churn_rate' (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Validate required columns exist
    # ----------------------------------------------------------------------
    # Ensures 'month_start' and 'mrr-churn-rate' are present before processing
    validate_columns(df_cm_metrics, ['month_start', 'mrr-churn-rate'], "ChartMogul Metrics")

    # ----------------------------------------------------------------------
    # STEP 2: Work on a copy of the DataFrame
    # ----------------------------------------------------------------------
    # Prevents accidental modification of the original DataFrame
    df = df_cm_metrics.copy()

    # ----------------------------------------------------------------------
    # STEP 3: Extract 'month' from 'month_start'
    # ----------------------------------------------------------------------
    # Converts the first day of the month into YYYY-MM format
    # for grouping/merging with other metrics
    df['month'] = ensure_month_format(df['month_start'])

    # ----------------------------------------------------------------------
    # STEP 4: Select relevant columns
    # ----------------------------------------------------------------------
    df = df[['month', 'mrr-churn-rate']]

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 12. Customer Lifetime Value               * VALID: Taken directly from CM_Metrics
#    ───────────────────────────────────────────────
#
# * Formula:
#     ltv = arpa / (customer_churn_rate / 100)
#   Where:
#     arpa = Average Revenue Per Account (monthly)
#     customer_churn_rate = % of customers lost in the month
#   (Reported directly by ChartMogul)
#
# * Source Table(s) and Columns:
#     - df_CM_metrics_clean:
#         • 'month_start' → first day of the month
#         • 'ltv' → average lifetime value per customer
#
# * Calculation Steps:
#     1. Extract 'month' from 'month_start' column
#     2. Select 'ltv' column
#     3. Rename it to 'ltv'
#
# * Assumptions / Filters:
#     - Follows ChartMogul’s default formula:
#       LTV = ARPA / Customer Churn Rate
#     - Accepts ChartMogul’s definitions of ARPA and churn as authoritative
#
# * Flowchart:
#     df_CM_metrics_clean
#         └── Extract 'month' from 'month_start'
#             └── Select 'ltv'
#                 └── Rename to 'ltv'
# --------------------------------------------------------------------------

def calculate_ltv(df_cm_metrics):
    """
    Calculate the monthly Customer Lifetime Value (ltv)
    as reported by ChartMogul.

    LTV is defined as:
        ARPA / (customer_churn_rate / 100)
    where:
        ARPA                = Average Revenue Per Account (monthly)
        customer_churn_rate = % of customers lost in the month

    Parameters:
        df_cm_metrics (pd.DataFrame):
            Cleaned ChartMogul metrics table containing:
            - 'month_start' (str/date): First day of the month
            - 'ltv' (float): Average lifetime value per customer

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month' (str, YYYY-MM)
            - 'ltv' (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Validate required columns exist
    # ----------------------------------------------------------------------
    # Ensures 'month_start' and 'ltv' are present before processing
    validate_columns(df_cm_metrics, ['month_start', 'ltv'], "ChartMogul Metrics")

    # ----------------------------------------------------------------------
    # STEP 2: Work on a copy of the DataFrame
    # ----------------------------------------------------------------------
    # Prevents accidental modification of the original DataFrame
    df = df_cm_metrics.copy()

    # ----------------------------------------------------------------------
    # STEP 3: Extract 'month' from 'month_start'
    # ----------------------------------------------------------------------
    # Converts the first day of the month into YYYY-MM format
    # for grouping/merging with other metrics
    df['month'] = ensure_month_format(df['month_start'])

    # ----------------------------------------------------------------------
    # STEP 4: Select relevant columns 
    # ----------------------------------------------------------------------
    df = df[['month', 'ltv']]

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df

# --------------------------------------------------------------------------
# 13. Customer Acquisition Cost (CAC)           * VALID: 
#                                                 CAC costs taken from HD Purchases using 'total_eur'
#                                                 (Only confirmed purchases, status == 1)
#                                                 CAC suppliers identified in HD Contacts ('tags' contains "cac")
#                                                 New Customers taken from CM_Customers
#    ─────────────────────────────────────────────────────────────────────
#
# Tecnicamente el CAC como esta calculado aqui es correcto. 
# Pero cuando se añadieron los tags era para dividir los costes entre tipos. 
#
# * Formula:
#     cac = CAC Costs / New Customers
#   Where:
#     CAC Costs = sum of confirmed purchase totals in EUR from suppliers tagged as "cac"
#     New Customers = number of customers whose `customer-since` date falls in the month
#
# * Source Table(s) and Columns:
#     - df_HD_contacts_clean (Holded Contacts):
#         • 'id' → supplier ID
#         • 'tags' → list of classification tags
#         • 'type' → contact type (supplier/client)
#     - df_HD_purchases_clean (Holded Purchases):
#         • 'contact' → supplier ID (matches 'id' in contacts)
#         • 'status' → purchase status (1 = confirmed, 0 = draft/other)
#         • 'date' → purchase date
#         • 'total_eur' → purchase total in EUR
#     - df_CM_customers_clean (ChartMogul Customers):
#         • 'uuid' → customer ID
#         • 'customer-since' → date the customer first generated subscription revenue
#
# * Calculation Steps:
#     1. From contacts: filter for rows where:
#         • 'tags' contains "cac"
#         • 'type' is "supplier"
#     2. Get supplier IDs from this filtered list.
#     3. From purchases: filter for rows where:
#         • 'contact' is in the supplier IDs
#         • 'status' == 1 (confirmed purchase)
#     4. Extract 'month' from purchase 'date' and sum 'total_eur' per month (CAC costs).
#     5. From ChartMogul Customers: filter rows where 'customer-since' is not null.
#     6. Extract 'month' from 'customer-since' and count unique customers.
#     7. Merge CAC costs and new customers by 'month'.
#     8. Compute: cac = cac_costs / new_customers (0 if no new customers).
#
# * Assumptions / Filters:
#     - Only suppliers tagged with "cac" are included in CAC costs.
#     - Only confirmed purchases (status == 1) are counted towards CAC.
#     - New customers are identified based on the authoritative `customer-since` date from ChartMogul.
#     - CAC costs and new customers are aligned to the same month granularity.
#     - All costs are normalized to EUR.
#
# * Flowchart:
#     df_HD_contacts_clean
#         └── filter tags contains "cac" AND type == "supplier"
#             └── get supplier IDs
#                 └── filter purchases by supplier IDs AND status == 1
#                     └── extract month → sum total_eur (CAC costs)
#     df_CM_customers_clean
#         └── drop null customer-since
#             └── extract month → count unique customers (new customers)
#     merge both → divide CAC costs / new customers
#
# * Notes for Verification:
#     - Verify that 'customer-since' in ChartMogul excludes leads and test accounts.
#     - Check that month alignment between CAC costs and new customers matches fiscal reporting.
#     - Ensure 'total_eur' is used consistently across all EUR-normalized metrics.
# --------------------------------------------------------------------------

def calculate_cac(df_purchases, df_contacts, df_cm_customers):
    """
    Calculate Customer Acquisition Cost (CAC) using EUR-normalized purchase totals
    and the number of new customers per month.

    Formula:
        CAC = CAC Costs / New Customers

    Parameters:
        df_purchases (pd.DataFrame):
            Cleaned Holded purchases data with:
            - 'contact'      (supplier ID)
            - 'status'       (1 = confirmed)
            - 'total_eur'    (purchase amount in EUR)
            - 'date'         (purchase date)
        df_contacts (pd.DataFrame):
            Cleaned Holded contacts data with:
            - 'id'           (contact ID)
            - 'tags'         (stringified classification tags)
            - 'type'         (contact type, e.g., supplier)
        df_cm_customers (pd.DataFrame):
            Cleaned ChartMogul customers data with:
            - 'uuid'             (unique customer ID)
            - 'customer-since'   (date of first paid subscription)

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month'          (str, YYYY-MM)
            - 'cac_costs'      (float): total EUR CAC supplier spend
            - 'new_customers'  (int): new customers in the month
            - 'cac'            (float): cost per acquired customer (0 if no new customers)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Preprocessing – Clean tags and type fields
    # ----------------------------------------------------------------------
    # We ensure the filtering columns in df_contacts are usable:
    # - Replace null values in 'tags' and 'type' with empty strings
    # - Convert them to lowercase-safe strings to avoid filter errors
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    df_contacts['type'] = df_contacts['type'].fillna('').astype(str)

    # ----------------------------------------------------------------------
    # STEP 2: Identify CAC-related suppliers from the Contacts table
    # ----------------------------------------------------------------------
    # Goal: Find all suppliers explicitly tagged for Customer Acquisition Cost (CAC)
    # - Filter contacts where:
    #     • The 'tags' column contains the substring "cac" (case-insensitive)
    #     • The 'type' column equals "supplier"
    # - Extract and store the 'id' values for these matching contacts
    # - These IDs will be used to match purchases in df_purchases
    cac_ids = df_contacts[
        (df_contacts['tags'].str.contains('cac', case=False, na=False)) &
        (df_contacts['type'].str.lower() == 'supplier')
    ]['id']

    # ----------------------------------------------------------------------
    # STEP 3: Filter Purchases table to retain only CAC-related supplier spend
    # ----------------------------------------------------------------------
    # - Match df_purchases['contact'] to the CAC supplier IDs extracted in Step 1
    # - Keep only confirmed purchases ('status' == 1)
    # - We work on a filtered copy called df_cac_purchases
    df_cac_purchases = df_purchases[
        (df_purchases['contact'].isin(cac_ids)) &
        (df_purchases['status'] == 1)
    ].copy()

    # ----------------------------------------------------------------------
    # STEP 4: Aggregate CAC-related purchases by month
    # ----------------------------------------------------------------------
    if not df_cac_purchases.empty:
        # 1. Convert purchase dates to 'YYYY-MM' format using ensure_month_format()
        # 2. Group by month and sum the EUR-normalized 'total_eur' purchase amounts
        # 3. Rename the resulting column to 'cac_costs' for clarity
        df_cac_purchases['month'] = ensure_month_format(df_cac_purchases['date'])
        df_cac_costs = (
            df_cac_purchases
            .groupby('month', as_index=False)['total_eur']
            .sum()
            .rename(columns={'total_eur': 'cac_costs'})
        )
    else:
        # If there were no confirmed CAC-related purchases, return an empty result
        debug("No confirmed CAC purchases found. Defaulting to 0.")
        df_cac_costs = pd.DataFrame(columns=['month', 'cac_costs'])

    # ----------------------------------------------------------------------
    # STEP 5: Count New Customers using ChartMogul 'customer-since'
    # ----------------------------------------------------------------------
    # - Work on a copy of df_cm_customers to avoid modifying the original
    # - Drop any rows with missing 'customer-since' (leads, test accounts, etc.)
    # - Extract the acquisition month using ensure_month_format()
    # - Group by month and count the number of new unique customer UUIDs
    df_new_customers = df_cm_customers.copy()
    df_new_customers = df_new_customers[df_new_customers['customer-since'].notna()]
    df_new_customers['month'] = ensure_month_format(df_new_customers['customer-since'])
    df_new_customers = (
        df_new_customers
        .groupby('month')['uuid']
        .nunique()
        .reset_index()
        .rename(columns={'uuid': 'new_customers'})
    )

    # ----------------------------------------------------------------------
    # STEP 6: Merge the monthly CAC costs and New Customer counts
    # ----------------------------------------------------------------------
    # - Perform an outer join on 'month' to ensure no time periods are lost
    # - Replace NaNs with 0 (some months may have spend but no new customers, or vice versa)
    df_cac = pd.merge(df_new_customers, df_cac_costs, on='month', how='outer').fillna(0)

    # ----------------------------------------------------------------------
    # STEP 7: Compute CAC = CAC Costs / New Customers
    # ----------------------------------------------------------------------
    # - Row-wise division: only compute CAC if there are new customers that month
    # - If no new customers, set CAC to 0 to avoid division by zero
    df_cac['cac'] = df_cac.apply(
        lambda row: row['cac_costs'] / row['new_customers'] if row['new_customers'] > 0 else 0,
        axis=1
    )

    # ----------------------------------------------------------------------
    # STEP 8: Return final DataFrame
    # ----------------------------------------------------------------------
    # - Columns: ['month', 'cac_costs', 'new_customers', 'cac']
    return df_cac[['month', 'cac_costs', 'new_customers', 'cac']]

# --------------------------------------------------------------------------
# 14. CAC:LTV Ratio (CAC_LTV)
#    ─────────────────────────────────
#
# * Formula:
#     cac_ltv_ratio = LTV / CAC
#   Where:
#     LTV = Customer Lifetime Value (calculated locally from ARPA and churn rate)
#     CAC = Customer Acquisition Cost (from calculate_cac)
#
# * Source Table(s) and Columns:
#     - Output of calculate_ltv():
#         • 'month'
#         • 'ltv'
#     - Output of calculate_cac():
#         • 'month'
#         • 'cac'
#
# * Calculation Steps:
#     1. Merge LTV and CAC dataframes on 'month'.
#     2. For each month:
#         • If CAC > 0 → cac_ltv_ratio = ltv / cac
#         • If CAC = 0 → cac_ltv_ratio = 0
#
# * Assumptions / Filters:
#     - LTV and CAC must be aligned by the same 'month' definition.
#     - CAC values are based on suppliers tagged "cac" and type == "supplier".
#
# * Flowchart:
#     calculate_ltv() → df_ltv
#     calculate_cac() → df_cac
#         └── merge on 'month'
#             └── divide LTV by CAC
#
# * Notes for Verification:
#     - If CAC is extremely small, ratio can become very large.
#     - Check for months with high LTV and 0 CAC — may indicate insufficient CAC tagging.
# --------------------------------------------------------------------------

def calculate_cac_ltv_ratio(df_ltv, df_cac):
    """
    Calculate the CAC:LTV ratio for each month.

    Formula:
        cac_ltv_ratio = ltv / cac
        if cac == 0 → cac_ltv_ratio = 0

    Parameters:
        df_ltv (pd.DataFrame):
            Output of calculate_ltv() containing:
            - 'month' (str, YYYY-MM)
            - 'ltv'   (float): Customer Lifetime Value
        df_cac (pd.DataFrame):
            Output of calculate_cac() containing:
            - 'month' (str, YYYY-MM)
            - 'cac'   (float): Customer Acquisition Cost

    Returns:
        pd.DataFrame:
            DataFrame with:
            - 'month'          (str, YYYY-MM)
            - 'cac_ltv_ratio'  (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Merge LTV and CAC data on 'month'
    # ----------------------------------------------------------------------
    # Outer join ensures months present in either table are preserved.
    # Missing values are filled with 0 for both LTV and CAC.
    df = pd.merge(
        df_ltv[['month', 'ltv']],
        df_cac[['month', 'cac']],
        on='month',
        how='outer'
    ).fillna(0)

    # ----------------------------------------------------------------------
    # STEP 2: Calculate CAC:LTV ratio
    # ----------------------------------------------------------------------
    # If CAC > 0, divide LTV by CAC.
    # If CAC == 0, set ratio to 0 to avoid division-by-zero errors.
    df['cac_ltv_ratio'] = df.apply(
        lambda row: row['ltv'] / row['cac'] if row['cac'] > 0 else 0,
        axis=1
    )

    # ----------------------------------------------------------------------
    # STEP 3: Return final DataFrame
    # ----------------------------------------------------------------------
    return df[['month', 'cac_ltv_ratio']]

# --------------------------------------------------------------------------
# 15. Operating Expenses (OPEX)         * VALID: Taken from HD Contacts & Purchases
#                                        Only confirmed purchases (status == 1)
#                                        EUR-normalized using 'total_eur'
#    ─────────────────────────────────
#
# * Formula:
#     opex = sum of confirmed purchase totals in EUR from suppliers tagged as "opex"
#   Where:
#     OPEX purchases = all confirmed purchases from suppliers whose tags include "opex"
#
# * Source Table(s) and Columns:
#     - df_HD_contacts_clean:
#         • 'id' → supplier ID
#         • 'tags' → list of classification tags
#         • 'type' → contact type (supplier/client)
#     - df_HD_purchases_clean:
#         • 'contact' → supplier ID (matches 'id' in contacts)
#         • 'status' → purchase status (1 = confirmed, 0 = draft/other)
#         • 'date' → purchase date
#         • 'total_eur' → purchase total amount in EUR
#
# * Calculation Steps:
#     1. From contacts: filter for rows where:
#         • 'tags' contains "opex"
#         • 'type' is "supplier"
#     2. Get supplier IDs from this filtered list.
#     3. From purchases: filter for rows where:
#         • 'contact' is in the supplier IDs
#         • 'status' == 1 (confirmed purchase)
#     4. Extract 'month' from purchase 'date'.
#     5. Group by 'month' and sum 'total_eur' (opex).
#
# * Assumptions / Filters:
#     - Only suppliers tagged with "opex" are included.
#     - Only confirmed purchases (status == 1) are counted towards OPEX.
#     - Purchases are normalized to EUR.
#
# * Flowchart:
#     df_HD_contacts_clean
#         └── filter tags contains "opex" AND type == "supplier"
#             └── get supplier IDs
#                 └── filter purchases by supplier IDs AND status == 1
#                     └── extract month → sum total_eur (opex)
#
# * Notes for Verification:
#     - Verify that 'type' is populated correctly to avoid excluding valid suppliers.
#     - Check that months with zero opex actually have no relevant confirmed purchases.
# --------------------------------------------------------------------------

def calculate_opex(df_purchases, df_contacts):
    """
    Calculate monthly Operating Expenses (OPEX) from tagged supplier purchases in EUR.

    Parameters:
        df_purchases (pd.DataFrame): Holded purchases data with:
            - 'contact'     (supplier ID)
            - 'status'      (1 = confirmed)
            - 'total_eur'   (purchase total in EUR)
            - 'date'        (purchase date)
        df_contacts (pd.DataFrame): Holded contacts data with:
            - 'id'          (supplier ID)
            - 'tags'        (classification tags)
            - 'type'        (contact type)

    Returns:
        pd.DataFrame: DataFrame with:
            - 'month' (YYYY-MM)
            - 'opex'  (float): Sum of confirmed EUR-denominated purchases from opex suppliers
    """

    # --- Ensure the columns used for filtering are in the correct format ---
    # Fill missing tags/types with empty strings to avoid NaN issues when filtering
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    df_contacts['type'] = df_contacts['type'].fillna('').astype(str)

    # ----------------------------------------------------------------------
    # STEP 1: Identify OPEX-related suppliers from the Contacts table
    # ----------------------------------------------------------------------
    # 1. Filter df_contacts for rows where:
    #    - The 'tags' column contains the keyword "opex" (case-insensitive).
    #    - The 'type' column equals "supplier" (ensuring we only include vendors, not clients).
    # 2. Extract their 'id' values into a list/Series.
    opex_ids = df_contacts[
        (df_contacts['tags'].str.contains('opex', case=False, na=False)) &
        (df_contacts['type'].str.lower() == 'supplier')
    ]['id']

    # ----------------------------------------------------------------------
    # STEP 2: Filter Purchases table to keep only relevant confirmed OPEX purchases
    # ----------------------------------------------------------------------
    # This step "links" Contacts → Purchases by matching:
    # - df_purchases['contact'] (supplier ID in the purchase record)
    #   against the `opex_ids` extracted from df_contacts above.
    # We also require status == 1 to ensure the purchase is confirmed/approved.
    df_opex_purchases = df_purchases[
        (df_purchases['contact'].isin(opex_ids)) &   # Supplier is OPEX-tagged
        (df_purchases['status'] == 1)                # Purchase is confirmed
    ].copy()

    # ----------------------------------------------------------------------
    # STEP 3: Aggregate total OPEX per month
    # ----------------------------------------------------------------------
    if not df_opex_purchases.empty:
        # Convert purchase date into YYYY-MM format for monthly grouping
        df_opex_purchases['month'] = ensure_month_format(df_opex_purchases['date'])

        # Sum the EUR purchase amounts ('total_eur') for each month
        df_opex = df_opex_purchases.groupby('month', as_index=False)['total_eur'].sum()

        # Rename the column to 'opex' to indicate metric meaning
        df_opex.rename(columns={'total_eur': 'opex'}, inplace=True)
    else:
        # If no matching confirmed OPEX purchases exist, return an empty DataFrame
        debug("No confirmed OPEX purchases found. Defaulting to 0.")
        df_opex = pd.DataFrame(columns=['month', 'opex'])

    # ----------------------------------------------------------------------
    # STEP 4: Return the aggregated monthly OPEX
    # ----------------------------------------------------------------------
    return df_opex

# --------------------------------------------------------------------------
# 16. Cost of Goods Sold (COGS)         * VALID: Taken from HD Contacts & Purchases
#                                        Only confirmed purchases (status == 1)
#                                        EUR-normalized using 'total_eur'
#    ─────────────────────────────────
#
# * Formula:
#     cogs = sum of confirmed purchase totals in EUR from suppliers tagged as "cogs"
#   Where:
#     COGS purchases = all confirmed purchases from suppliers whose tags include "cogs"
#
# * Source Table(s) and Columns:
#     - df_HD_contacts_clean:
#         • 'id' → supplier ID
#         • 'tags' → list of classification tags
#         • 'type' → contact type (supplier/client)
#     - df_HD_purchases_clean:
#         • 'contact' → supplier ID (matches 'id' in contacts)
#         • 'status' → purchase status (1 = confirmed, 0 = draft/other)
#         • 'date' → purchase date
#         • 'total_eur' → purchase total amount in EUR
#
# * Calculation Steps:
#     1. From contacts: filter for rows where:
#         • 'tags' contains "cogs"
#         • 'type' is "supplier"
#     2. Get supplier IDs from this filtered list.
#     3. From purchases: filter for rows where:
#         • 'contact' is in the supplier IDs
#         • 'status' == 1 (confirmed purchase)
#     4. Extract 'month' from purchase 'date'.
#     5. Group by 'month' and sum 'total_eur' (cogs).
#
# * Assumptions / Filters:
#     - Only suppliers tagged with "cogs" are included.
#     - Only confirmed purchases (status == 1) are counted towards COGS.
#     - Purchases are normalized to EUR.
#
# * Flowchart:
#     df_HD_contacts_clean
#         └── filter tags contains "cogs" AND type == "supplier"
#             └── get supplier IDs
#                 └── filter purchases by supplier IDs AND status == 1
#                     └── extract month → sum total_eur (cogs)
#
# * Notes for Verification:
#     - Verify that 'type' is populated correctly to avoid excluding valid suppliers.
#     - Check that months with zero COGS actually have no relevant confirmed purchases.
# --------------------------------------------------------------------------

def calculate_cogs(df_purchases, df_contacts):
    """
    Calculate monthly Cost of Goods Sold (COGS) from tagged supplier purchases in EUR.

    Parameters:
        df_purchases (pd.DataFrame): Holded purchases data with:
            - 'contact'     (supplier ID)
            - 'status'      (1 = confirmed)
            - 'total_eur'   (purchase total in EUR)
            - 'date'        (purchase date)
        df_contacts (pd.DataFrame): Holded contacts data with:
            - 'id'          (supplier ID)
            - 'tags'        (classification tags)
            - 'type'        (contact type)

    Returns:
        pd.DataFrame: DataFrame with:
            - 'month' (YYYY-MM)
            - 'cogs'  (float): Sum of confirmed EUR-denominated purchases from cogs suppliers
    """

    # --- Ensure the columns used for filtering are in the correct format ---
    # Fill missing tags/types with empty strings to avoid NaN issues when filtering
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    df_contacts['type'] = df_contacts['type'].fillna('').astype(str)

    # ----------------------------------------------------------------------
    # STEP 1: Identify COGS-related suppliers from the Contacts table
    # ----------------------------------------------------------------------
    # 1. Filter df_contacts for rows where:
    #    - The 'tags' column contains the keyword "cogs" (case-insensitive).
    #    - The 'type' column equals "supplier".
    # 2. Extract their 'id' values for purchase filtering.
    cogs_ids = df_contacts[
        (df_contacts['tags'].str.contains('cogs', case=False, na=False)) &
        (df_contacts['type'].str.lower() == 'supplier')
    ]['id']

    # ----------------------------------------------------------------------
    # STEP 2: Filter Purchases table for matching confirmed COGS purchases
    # ----------------------------------------------------------------------
    # Filter df_purchases to only:
    #   - Purchases linked to COGS-tagged suppliers
    #   - Purchases marked as confirmed (status == 1)
    df_cogs_purchases = df_purchases[
        (df_purchases['contact'].isin(cogs_ids)) &
        (df_purchases['status'] == 1)
    ].copy()

    # ----------------------------------------------------------------------
    # STEP 3: Aggregate COGS totals by month
    # ----------------------------------------------------------------------
    if not df_cogs_purchases.empty:
        df_cogs_purchases['month'] = ensure_month_format(df_cogs_purchases['date'])

        df_cogs = df_cogs_purchases.groupby('month', as_index=False)['total_eur'].sum()
        df_cogs.rename(columns={'total_eur': 'cogs'}, inplace=True)
    else:
        debug("No confirmed COGS purchases found. Defaulting to 0.")
        df_cogs = pd.DataFrame(columns=['month', 'cogs'])

    # ----------------------------------------------------------------------
    # STEP 4: Return final monthly COGS DataFrame
    # ----------------------------------------------------------------------
    return df_cogs

# --------------------------------------------------------------------------
# 17. Financial Costs (financial_costs)   * VALID: Taken from HD Contacts & Purchases
#                                          Only confirmed purchases (status == 1)
#                                          EUR-normalized using 'total_eur'
#    ─────────────────────────────────
#
# * Formula:
#     financial_costs = sum of confirmed purchase totals in EUR from suppliers tagged as "costes financieros"
#   Where:
#     Financial Costs purchases = all confirmed purchases from suppliers whose tags include "costes financieros"
#
# * Source Table(s) and Columns:
#     - df_HD_contacts_clean:
#         • 'id' → supplier ID
#         • 'tags' → list of classification tags
#         • 'type' → contact type (supplier/client)
#     - df_HD_purchases_clean:
#         • 'contact' → supplier ID (matches 'id' in contacts)
#         • 'status' → purchase status (1 = confirmed, 0 = draft/other)
#         • 'date' → purchase date
#         • 'total_eur' → purchase total amount in EUR
#
# * Calculation Steps:
#     1. From contacts: filter for rows where:
#         • 'tags' contains "costes financieros"
#         • 'type' is "supplier"
#     2. Get supplier IDs from this filtered list.
#     3. From purchases: filter for rows where:
#         • 'contact' is in the supplier IDs
#         • 'status' == 1 (confirmed purchase)
#     4. Extract 'month' from purchase 'date'.
#     5. Group by 'month' and sum 'total_eur' (financial_costs).
#
# * Assumptions / Filters:
#     - Only suppliers tagged with "costes financieros" are included.
#     - Only confirmed purchases (status == 1) are counted.
#     - Purchase totals are normalized to EUR.
#
# * Flowchart:
#     df_HD_contacts_clean
#         └── filter tags contains "costes financieros" AND type == "supplier"
#             └── get supplier IDs
#                 └── filter purchases by supplier IDs AND status == 1
#                     └── extract month → sum total_eur (financial_costs)
#
# * Notes for Verification:
#     - Verify that 'type' is populated correctly to avoid excluding valid suppliers.
#     - Ensure consistent formatting of "costes financieros" in supplier tags.
# --------------------------------------------------------------------------

def calculate_financial_costs(df_purchases, df_contacts):
    """
    Calculate monthly Financial Costs from tagged supplier purchases in EUR.

    Parameters:
        df_purchases (pd.DataFrame): Holded purchases data with:
            - 'contact'     (supplier ID)
            - 'status'      (1 = confirmed)
            - 'total_eur'   (purchase total in EUR)
            - 'date'        (purchase date)
        df_contacts (pd.DataFrame): Holded contacts data with:
            - 'id'          (supplier ID)
            - 'tags'        (classification tags)
            - 'type'        (contact type)

    Returns:
        pd.DataFrame: DataFrame with:
            - 'month' (YYYY-MM)
            - 'financial_costs' (float): Sum of confirmed EUR-denominated purchases from financial suppliers
    """

    # --- Prepare contacts data for filtering ---
    # Ensure 'tags' and 'type' columns are strings and not NaN
    df_contacts['tags'] = df_contacts['tags'].fillna('').astype(str)
    df_contacts['type'] = df_contacts['type'].fillna('').astype(str)

    # ----------------------------------------------------------------------
    # STEP 1: Identify Financial Costs suppliers from the Contacts table
    # ----------------------------------------------------------------------
    # Filter to suppliers whose 'tags' contain "costes financieros"
    # (case-insensitive match) and whose 'type' is "supplier".
    fin_cost_ids = df_contacts[
        (df_contacts['tags'].str.contains('costes financieros', case=False, na=False)) &
        (df_contacts['type'].str.lower() == 'supplier')
    ]['id']

    # ----------------------------------------------------------------------
    # STEP 2: Filter Purchases for these suppliers (confirmed only)
    # ----------------------------------------------------------------------
    df_fin_cost_purchases = df_purchases[
        (df_purchases['contact'].isin(fin_cost_ids)) &
        (df_purchases['status'] == 1)
    ].copy()

    # ----------------------------------------------------------------------
    # STEP 3: Aggregate Financial Costs by month
    # ----------------------------------------------------------------------
    if not df_fin_cost_purchases.empty:
        df_fin_cost_purchases['month'] = ensure_month_format(df_fin_cost_purchases['date'])

        df_fin_costs = df_fin_cost_purchases.groupby('month', as_index=False)['total_eur'].sum()
        df_fin_costs.rename(columns={'total_eur': 'financial_costs'}, inplace=True)
    else:
        debug("No confirmed Financial Costs purchases found. Defaulting to 0.")
        df_fin_costs = pd.DataFrame(columns=['month', 'financial_costs'])

    # ----------------------------------------------------------------------
    # STEP 4: Return final monthly aggregated Financial Costs
    # ----------------------------------------------------------------------
    return df_fin_costs

# --------------------------------------------------------------------------
# 18. EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization)
#    ───────────────────────────────────────────────────────────────────────
#
# * Formula:
#     ebitda = mrr - (opex + cogs + financial_costs + cac_costs)
#   Where:
#     mrr = total monthly recurring revenue
#     opex = total operating expenses
#     cogs = total cost of goods sold
#     financial_costs = total financial costs
#     cac_costs = total CAC acquisition spend (not divided by new customers)
#
# * Source Table(s) and Columns:
#     - df_mrr (output of calculate_mrr_components):
#         • 'month'
#         • 'mrr'
#     - df_opex (output of calculate_opex):
#         • 'month'
#         • 'opex'
#     - df_cogs (output of calculate_cogs):
#         • 'month'
#         • 'cogs'
#     - df_financial_costs (output of calculate_financial_costs):
#         • 'month'
#         • 'financial_costs'
#     - df_cac (output of calculate_cac):
#         • 'month'
#         • 'cac_costs'
#
# * Calculation Steps:
#     1. Merge df_mrr, df_opex, df_cogs, df_financial_costs, and df_cac on 'month'.
#     2. Fill any missing values with 0.
#     3. Calculate: ebitda = mrr - (opex + cogs + financial_costs + cac_costs).
#
# * Assumptions / Filters:
#     - All expenses (opex, cogs, financial_costs) are in the same currency as MRR.
#     - Missing months for any cost category are treated as 0 cost.
#
# * Flowchart:
#     df_mrr
#         └── merge df_opex
#             └── merge df_cogs
#                 └── merge df_financial_costs
#                     └── merge df_cac
#                         └── fillna(0)
#                             └── calculate ebitda
#
# * Notes for Verification:
#     - Ensure that expense categories do not overlap to avoid double-counting.
#     - Compare EBITDA trend against historical financial reports if available.
# --------------------------------------------------------------------------

def calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs, df_cac):
    """
    Calculate monthly EBITDA (Earnings Before Interest, Taxes, Depreciation, and Amortization)
    from MRR and expense categories.

    Formula:
        ebitda = mrr - (opex + cogs + financial_costs + cac_costs)

    Parameters:
        df_mrr (pd.DataFrame):
            - 'month' (str, YYYY-MM)
            - 'mrr'   (float): Monthly Recurring Revenue
        df_opex (pd.DataFrame):
            - 'month' (str, YYYY-MM)
            - 'opex'  (float): Operating Expenses
        df_cogs (pd.DataFrame):
            - 'month' (str, YYYY-MM)
            - 'cogs'  (float): Cost of Goods Sold
        df_financial_costs (pd.DataFrame):
            - 'month'           (str, YYYY-MM)
            - 'financial_costs' (float): Financial Costs
        df_cac (pd.DataFrame):
            - 'month'      (str, YYYY-MM)
            - 'cac_costs'  (float): CAC acquisition spend

    Returns:
        pd.DataFrame:
            - 'month'  (str, YYYY-MM)
            - 'ebitda' (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Merge all input DataFrames on 'month'
    # ----------------------------------------------------------------------
    # Outer join ensures that months present in any of the datasets are kept.
    # fillna(0) ensures missing values are treated as zero costs/revenue.
    df = (
        df_mrr
        .merge(df_opex, on='month', how='outer')
        .merge(df_cogs, on='month', how='outer')
        .merge(df_financial_costs, on='month', how='outer')
        .merge(df_cac[['month', 'cac_costs']], on='month', how='outer')
        .fillna(0)
    )

    # ----------------------------------------------------------------------
    # STEP 2: Calculate EBITDA
    # ----------------------------------------------------------------------
    # Subtract total monthly expenses (opex + cogs + financial costs + cac_costs)
    # from the monthly recurring revenue (mrr).
    df['ebitda'] = df['mrr'] - (df['opex'] + df['cogs'] + df['financial_costs'] + df['cac_costs'])

    # ----------------------------------------------------------------------
    # STEP 3: Return final DataFrame
    # ----------------------------------------------------------------------
    return df[['month', 'ebitda']]


# --------------------------------------------------------------------------
# 19. Net Burn (net_burn)           * VALID: Subtracts revenue from core costs
#                                        Revenue from ChartMogul MRR Components
#                                        Costs from Holded Purchases ('total_eur')
#    ─────────────────────────────────
# * Seria mejor calcularlo en base a las entradas del daily ledger (la suma de todos los debits menos la suma de todos los credits)
# * Debits: Gastos, Credits: Entradas (revenue)
#
# * Formula:
#     net_burn = total_costs - total_revenue
#   Where:
#     - total_costs: sum of confirmed EUR purchases from Holded (all suppliers)
#     - total_revenue: sum of MRR from ChartMogul
#
# * Source Table(s) and Columns:
#     - df_HD_purchases_clean:
#         • 'status'       (1 = confirmed)
#         • 'date'         (purchase date)
#         • 'total_eur'    (purchase total in EUR)
#     - df_CM_mrr_components:
#         • 'date'         (month in YYYY-MM format)
#         • 'mrr'          (monthly recurring revenue)
#
# * Calculation Steps:
#     1. From purchases: filter for confirmed rows (status == 1)
#     2. Convert 'date' to month (YYYY-MM) and sum 'total_eur' per month
#     3. From MRR: group by 'date' and sum 'mrr' per month
#     4. Merge revenue and costs on 'month'
#     5. Calculate: net_burn = total_costs - mrr
#
# * Assumptions:
#     - All confirmed purchases are relevant to burn rate
#     - 'mrr' represents revenue; negative values (churn, contraction) are already included
#     - All costs are normalized to EUR
#
# * Notes for Verification:
#     - Ensure that `mrr` is aggregated from components (expansion, contraction, etc.)
#     - Months with missing MRR or costs should default to 0 before subtraction
# --------------------------------------------------------------------------

def calculate_net_burn(df_purchases, df_cm_mrr_components):
    """
    Calculate monthly Net Burn Rate = total confirmed EUR costs - MRR revenue.

    Parameters:
        df_purchases (pd.DataFrame): Holded purchases with:
            - 'status'      (1 = confirmed)
            - 'date'        (purchase date)
            - 'total_eur'   (purchase total in EUR)
        df_cm_mrr_components (pd.DataFrame): ChartMogul MRR components with:
            - 'date'        (YYYY-MM string)
            - 'mrr'         (monthly recurring revenue)

    Returns:
        pd.DataFrame: DataFrame with:
            - 'month'       (YYYY-MM)
            - 'total_costs' (float): Total confirmed purchase costs in EUR
            - 'net_burn'    (float): Net burn (costs - revenue)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Calculate Total Confirmed Costs per Month (EUR)
    # ----------------------------------------------------------------------
    df_confirmed = df_purchases[df_purchases['status'] == 1].copy()
    df_confirmed['month'] = ensure_month_format(df_confirmed['date'])

    df_costs = df_confirmed.groupby('month', as_index=False)['total_eur'].sum()
    df_costs.rename(columns={'total_eur': 'total_costs'}, inplace=True)

    # ----------------------------------------------------------------------
    # STEP 2: Aggregate Monthly MRR from CM data
    # ----------------------------------------------------------------------
    df_revenue = df_cm_mrr_components.copy()
    df_revenue['month'] = ensure_month_format(df_revenue['date'])
    df_revenue = df_revenue.groupby('month', as_index=False)['mrr'].sum()

    # ----------------------------------------------------------------------
    # STEP 3: Merge Costs and Revenue, Fill Missing Values
    # ----------------------------------------------------------------------
    df_merged = pd.merge(df_costs, df_revenue, on='month', how='outer')
    df_merged.fillna({'total_costs': 0, 'mrr': 0}, inplace=True)

    # ----------------------------------------------------------------------
    # STEP 4: Calculate Net Burn
    # ----------------------------------------------------------------------
    df_merged['net_burn'] = df_merged['total_costs'] - df_merged['mrr']

    # ----------------------------------------------------------------------
    # STEP 5: Return final DataFrame
    # ----------------------------------------------------------------------
    return df_merged[['month', 'total_costs', 'net_burn']]


# --------------------------------------------------------------------------
# 20. Burn Rate (burn_rate)
#    ─────────────────────────────────
# 
#
# * Formula:
#     burn_rate = abs(ebitda)
#   Where:
#     ebitda = Earnings Before Interest, Taxes, Depreciation, and Amortization
#
# * Source Table(s) and Columns:
#     - df_ebitda:
#         • 'month'
#         • 'ebitda'
#
# * Calculation Steps:
#     1. Take the absolute value of EBITDA for each month.
#
# * Assumptions / Filters:
#     - Treats burn rate as a positive value regardless of EBITDA sign.
#     - If EBITDA is positive, burn rate equals EBITDA.
#
# * Flowchart:
#     df_ebitda
#         └── abs(ebitda)
#
# * Notes for Verification:
#     - Negative EBITDA indicates a true cash burn.
#     - Positive EBITDA but positive burn_rate should be clearly explained in reporting.
# --------------------------------------------------------------------------

def calculate_burn_rate(df_ebitda):
    """
    Calculate the monthly Burn Rate from EBITDA.

    Formula:
        burn_rate = abs(ebitda)

    Parameters:
        df_ebitda (pd.DataFrame):
            - 'month'  (str, YYYY-MM)
            - 'ebitda' (float): Earnings Before Interest, Taxes, Depreciation, and Amortization

    Returns:
        pd.DataFrame:
            - 'month'     (str, YYYY-MM)
            - 'burn_rate' (float): Absolute value of EBITDA

    Notes:
        - Negative EBITDA indicates actual cash burn (loss-making months).
        - Positive EBITDA still results in a positive burn_rate for consistency.
        - This metric is used as an input for runway calculation.
    """

    # ----------------------------------------------------------------------
    # STEP 1: Work on a copy to avoid mutating the input DataFrame
    # ----------------------------------------------------------------------
    df = df_ebitda.copy()

    # ----------------------------------------------------------------------
    # STEP 2: Calculate burn rate as the absolute value of EBITDA
    # ----------------------------------------------------------------------
    # Even if EBITDA is positive, we report burn_rate as a positive number.
    # This normalizes reporting for both profit and loss months.
    df['burn_rate'] = df['ebitda'].abs()

    # ----------------------------------------------------------------------
    # STEP 3: Return the final DataFrame
    # ----------------------------------------------------------------------
    return df[['month', 'burn_rate']]

# --------------------------------------------------------------------------
# 21. Runway (runway, cash_balance_eur)
#    ─────────────────────────────────
#
# * Formula:
#     runway = cash_balance_eur / burn_rate
#   Where:
#     cash_balance_eur = Total cash/bank balance in EUR (from treasury data)
#     burn_rate        = Monthly burn rate (absolute EBITDA)
#
# * Source Table(s) and Columns:
#     - df_burn_rate:
#         • 'month'
#         • 'burn_rate'
#     - df_cash_balance (optional; if not provided, a numeric constant may be used):
#         • 'month' or 'date'
#         • 'cash_balance_eur'
#
# * Calculation Steps:
#     1. Normalize df_burn_rate['month'] to YYYY-MM.
#     2. If the 2nd arg is numeric → use it as a constant cash balance for all months.
#        If it is a DataFrame → normalize its month, coerce cash to numeric,
#        aggregate to monthly (e.g., last recorded balance per month),
#        then outer-merge into df_burn_rate.
#     3. Fill missing values with 0 for safe division.
#     4. Compute runway = cash_balance_eur / burn_rate (None if burn_rate == 0).
#
# * Assumptions / Filters:
#     - burn_rate is nonnegative (e.g., abs(EBITDA)).
#     - Treasury data may be daily; taking the last balance in each month represents month-end cash.
#
# * Flowchart:
#     df_burn_rate ─┬── normalize month
#                   └── (constant cash OR merge normalized+monthly cash DF)
#                        └── compute runway
#
# * Notes for Verification:
#     - If you want real cash values per month, pass the treasury DataFrame here.
# --------------------------------------------------------------------------

def calculate_runway(df_burn_rate, cash_or_df):
    """
    Calculate the runway in months using cash balance and burn rate.

    Parameters:
        df_burn_rate (pd.DataFrame):
            - 'month'      (str/date)
            - 'burn_rate'  (float)
        cash_or_df (float or pd.DataFrame):
            - If float/int: constant cash balance used for all months
            - If DataFrame: must include:
                • 'month' or 'date' (str/date)
                • 'cash_balance_eur' (float)

    Returns:
        pd.DataFrame:
            - 'month'            (str, YYYY-MM)
            - 'cash_balance_eur' (float)
            - 'runway'           (float)
    """

    # ----------------------------------------------------------------------
    # STEP 1: Work on a copy and normalize 'month'
    # ----------------------------------------------------------------------
    df = df_burn_rate.copy()
    df['month'] = ensure_month_format(df['month'])
    df['burn_rate'] = pd.to_numeric(df['burn_rate'], errors='coerce').fillna(0.0)

    # ----------------------------------------------------------------------
    # STEP 2: Attach cash balance (constant OR from DataFrame)
    # ----------------------------------------------------------------------
    if isinstance(cash_or_df, (int, float)):                     # constant cash path
        df['cash_balance_eur'] = float(cash_or_df or 0)
    else:                                                         # DataFrame path
        cash = cash_or_df.copy()
        # Pick month column and ensure required fields exist
        month_col = 'month' if 'month' in cash.columns else ('date' if 'date' in cash.columns else None)
        if month_col is None:
            raise ValueError("df_cash_balance must include a 'month' or 'date' column.")
        if 'cash_balance_eur' not in cash.columns:
            # best-effort fallback to common alternatives
            for alt in ['balance_eur', 'balance', 'cash']:
                if alt in cash.columns:
                    cash['cash_balance_eur'] = cash[alt]
                    break
            if 'cash_balance_eur' not in cash.columns:
                raise ValueError("df_cash_balance must include 'cash_balance_eur' (or a recognizable balance column).")

        # Normalize to YYYY-MM and aggregate to monthly (month-end by .last())
        cash['month'] = ensure_month_format(cash[month_col])
        cash['cash_balance_eur'] = pd.to_numeric(cash['cash_balance_eur'], errors='coerce')
        cash = (
            cash[['month', 'cash_balance_eur']]
            .groupby('month', as_index=False)['cash_balance_eur']
            .last()
        )

        # Merge monthly cash into burn-rate table
        df = pd.merge(df, cash, on='month', how='outer')
        df['cash_balance_eur'] = df['cash_balance_eur'].fillna(0.0)

    # ----------------------------------------------------------------------
    # STEP 3: Calculate Runway (months)
    # ----------------------------------------------------------------------
    # Avoid division by zero: set runway to None if burn_rate is 0
    df['runway'] = df.apply(
        lambda row: round(row['cash_balance_eur'] / row['burn_rate'], 2) if row['burn_rate'] != 0 else None,
        axis=1
    )

    # ----------------------------------------------------------------------
    # STEP 4: Return final DataFrame
    # ----------------------------------------------------------------------
    return df[['month', 'cash_balance_eur', 'runway']]

# ------------------- Main Pipeline -------------------
def run_pipeline(cash_balance):
    debug("Loading input datasets...")
    df_customers_raw = pd.read_csv('data/INPUT/chartmogul_customers/clean/chartmogul_customers_clean.csv')
    df_purchases = pd.read_csv('data/INPUT/holded_purchases/clean/holded_purchases_clean.csv')
    df_contacts = pd.read_csv('data/INPUT/holded_contacts/clean/holded_contacts_clean.csv')
    df_mrr_components = pd.read_csv('data/INPUT/chartmogul_mrr_components/clean/chartmogul_mrr_components_clean.csv')
    df_cm_metrics = pd.read_csv('data/INPUT/chartmogul_metrics/clean/chartmogul_metrics_clean.csv')
    df_treasury = pd.read_csv('data/INPUT/holded_treasury/clean/holded_treasury_clean.csv')

    # Normalize common treasury balance column names to 'cash_balance_eur' (optional but helpful)
    if 'cash_balance_eur' not in df_treasury.columns:
        for alt in ['balance_eur', 'ending_balance', 'balance', 'cash']:
            if alt in df_treasury.columns:
                df_treasury = df_treasury.rename(columns={alt: 'cash_balance_eur'})
                debug(f"Renamed treasury column '{alt}' -> 'cash_balance_eur'")
                break

    validate_columns(df_contacts, ['id', 'tags'], "Contacts")
    validate_columns(df_purchases, ['date', 'contact', 'total'], "Purchases")

    # --- Compute metrics in script order ---
    df_mrr = calculate_mrr(df_mrr_components)                                           # 1
    df_expansion_mrr = calculate_expansion_mrr(df_mrr_components)                       # 2
    df_contraction_mrr = calculate_contraction_mrr(df_mrr_components)                   # 3
    df_new_mrr = calculate_new_mrr(df_mrr_components)                                   # 4
    df_churned_mrr = calculate_churned_mrr(df_mrr_components)                           # 5
    df_net_new_mrr = calculate_net_new_mrr(df_mrr_components)                           # 6
    df_arr = calculate_arr(df_mrr_components)                                           # 7
    df_arpa = calculate_arpa(df_cm_metrics)                                             # 8
    df_customers = calculate_customers(df_cm_metrics)                                   # 9
    df_customer_churn_rate = calculate_customer_churn_rate(df_cm_metrics)               # 10
    df_revenue_churn_rate = calculate_revenue_churn_rate(df_cm_metrics)                 # 11
    df_ltv = calculate_ltv(df_cm_metrics)                                               # 12
    df_cac = calculate_cac(df_purchases, df_contacts, df_customers_raw)                 # 13  <-- FIX
    df_cac_ltv_ratio = calculate_cac_ltv_ratio(df_ltv, df_cac)                          # 14
    df_opex = calculate_opex(df_purchases, df_contacts)                                 # 15
    df_cogs = calculate_cogs(df_purchases, df_contacts)                                 # 16
    df_financial_costs = calculate_financial_costs(df_purchases, df_contacts)           # 17
    df_ebitda = calculate_ebitda(df_mrr, df_opex, df_cogs, df_financial_costs, df_cac)  # 18
    df_net_burn = calculate_net_burn(df_purchases, df_mrr_components)                   # 19
    df_burn_rate = calculate_burn_rate(df_ebitda)                                       # 20

    # Use treasury DF if available; otherwise fall back to the CLI constant
    cash_source = df_treasury if not df_treasury.empty else cash_balance
    debug(f"Runway cash source: {'treasury DF' if isinstance(cash_source, pd.DataFrame) else 'constant'}")
    df_runway = calculate_runway(df_burn_rate, cash_source)                             # 21

    # --- Merge all metrics in script order ---
    dfs = [
        df_mrr,                         # 1
        df_expansion_mrr,               # 2
        df_contraction_mrr,             # 3
        df_new_mrr,                     # 4
        df_churned_mrr,                 # 5
        df_net_new_mrr,                 # 6
        df_arr,                         # 7
        df_arpa,                        # 8
        df_customers,                   # 9
        df_customer_churn_rate,         # 10
        df_revenue_churn_rate,          # 11
        df_ltv,                         # 12
        df_cac,                         # 13
        df_cac_ltv_ratio,               # 14
        df_opex,                        # 15
        df_cogs,                        # 16
        df_financial_costs,             # 17
        df_ebitda,                      # 18
        df_net_burn,                    # 19
        df_burn_rate,                   # 20
        df_runway                       # 21
    ]

    df_final = dfs[0]
    for df in dfs[1:]:
        df_final = pd.merge(df_final, df, on='month', how='outer')

    df_final = df_final.loc[:, ~df_final.columns.duplicated()]
    df_final = df_final.fillna(0)
    df_final['month'] = pd.to_datetime(df_final['month'], format="%Y-%m")
    df_final = df_final.sort_values(by='month')
    df_final['month'] = df_final['month'].dt.strftime("%Y-%m")

    # --- Reorder columns in script order ---
    preferred_order = [
        'month', 'mrr', 'expansion_mrr', 'contraction_mrr', 'new_mrr', 'churned_mrr',
        'net_new_mrr', 'arr',
        'arpa',
        'customers', 'customer_churn_rate', 'revenue_churn_rate', 'ltv',
        'cac_costs', 'new_customers', 'cac', 'cac_ltv_ratio',
        'opex', 'cogs', 'financial_costs',
        'ebitda', 'burn_rate', 'net_burn', 'cash_balance_eur', 'runway'
    ]

    ordered_columns = [col for col in preferred_order if col in df_final.columns]
    df_final = df_final[ordered_columns]

    # --- Save outputs ---
    current_month = datetime.now().strftime("%Y-%m")
    output_dir = os.path.join("data", "OUTPUT", current_month)
    os.makedirs(output_dir, exist_ok=True)

    # Paths for monthly versioned files
    csv_path = os.path.join(output_dir, f"final_metrics_{current_month}.csv")
    parquet_path = os.path.join(output_dir, f"final_metrics_{current_month}.parquet")

    # Save monthly files
    df_final.to_csv(csv_path, index=False)
    save_parquet(df_final, parquet_path)
    debug(f"Metrics saved at {csv_path} and {parquet_path}")

    # Save summary stats
    summary_stats = df_final.describe().round(2)
    summary_path = os.path.join(output_dir, f"summary_stats_{current_month}.csv")
    summary_stats.to_csv(summary_path)
    debug(f"Summary saved at {summary_path}")

    # --- Save static "latest" version for Power BI or external use ---
    latest_csv = os.path.join("data", "OUTPUT", "final_metrics_latest.csv")
    latest_parquet = os.path.join("data", "OUTPUT", "final_metrics_latest.parquet")

    # Overwrite static files with current month's data
    df_final.to_csv(latest_csv, index=False)
    save_parquet(df_final, latest_parquet)
    debug(f"Static latest metrics saved at {latest_csv} and {latest_parquet}")


# ------------------- Entry Point -------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--cash", type=float, default=10000)
    args = parser.parse_args()
    run_pipeline(cash_balance=args.cash)
