# ================================================================
# HOLDED TREASURY – BUILD MONTHLY CASH SERIES FROM DAILY LEDGER
# ================================================================
# Uses:
#   1) RAW windows JSON from the dailyledger extractor:
#        data/INPUT/holded_treasury/raw/holded_treasury_dailyledger_month_windows.json
#   2) A snapshot “anchor” balance (today’s) from the treasury accounts endpoint:
#        data/INPUT/holded_treasury/raw/holded_treasury_raw.json
#      (sum of all cash/bank accounts in EUR)
#
# Output (consumed by metrics_pipeline.py):
#   data/INPUT/holded_treasury/clean/holded_treasury_clean.csv
#     columns: month, cash_balance_eur
# ================================================================

import os, json, logging
import pandas as pd

LOG_PATH = "logs/pipeline.log"
os.makedirs("logs", exist_ok=True)
logging.basicConfig(filename=LOG_PATH, level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")

RAW_LEDGER = "data/INPUT/holded_treasury/raw/holded_treasury_dailyledger_month_windows.json"
RAW_SNAPSHOT = "data/INPUT/holded_treasury/raw/holded_treasury_raw.json"   # treasury accounts (current)
CLEAN_OUT = "data/INPUT/holded_treasury/clean/holded_treasury_clean.csv"

def load_snapshot_total_eur(path: str) -> float:
    """Sum balances from treasury accounts raw (current snapshot)."""
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    df = pd.DataFrame(data)
    # try common balance field names
    cand = next((c for c in ["balance_eur","balance","amount","currentBalance","available"] if c in df.columns), None)
    if cand is None:
        raise ValueError("Snapshot file does not contain a recognizable balance column.")
    total = pd.to_numeric(df[cand], errors="coerce").fillna(0).sum()
    return float(total)

def build_monthly_from_ledger():
    logging.info("Starting transform_HD_treasury_from_ledger()")

    if not os.path.exists(RAW_LEDGER):
        raise FileNotFoundError(f"Missing {RAW_LEDGER}")
    if not os.path.exists(RAW_SNAPSHOT):
        raise FileNotFoundError(
            f"Missing {RAW_SNAPSHOT} (treasury accounts snapshot). "
            "Run the snapshot extractor so we have an anchor balance."
        )

    with open(RAW_LEDGER, "r", encoding="utf-8") as f:
        windows = json.load(f)

    # Flatten entries → (month, account, debit, credit)
    rows = []
    for w in windows:
        m = w["month"]
        for e in w.get("entries", []):
            rows.append({
                "month": m,
                "account": str(e.get("account", "")),
                "debit": float(e.get("debit", 0) or 0),
                "credit": float(e.get("credit", 0) or 0),
            })

    df = pd.DataFrame(rows)
    if df.empty:
        logging.warning("No ledger entries found. Producing empty clean file.")
        os.makedirs(os.path.dirname(CLEAN_OUT), exist_ok=True)
        pd.DataFrame(columns=["month","cash_balance_eur"]).to_csv(CLEAN_OUT, index=False)
        return

    # Keep cash/bank accounts (PGC: 57*)
    df_cash = df[df["account"].str.startswith("57")].copy()

    # Monthly net cash movement: debits increase cash, credits decrease cash
    monthly_change = (
        df_cash.assign(net=lambda x: x["debit"] - x["credit"])
               .groupby("month", as_index=False)["net"].sum()
               .rename(columns={"net": "net_change"})
    )

    # Anchor with current snapshot
    snapshot_total = load_snapshot_total_eur(RAW_SNAPSHOT)

    # Ensure months are sorted
    monthly_change["month_dt"] = pd.to_datetime(monthly_change["month"], format="%Y-%m")
    monthly_change = monthly_change.sort_values("month_dt").reset_index(drop=True)

    # Backfill balances from latest month using reverse cumulative sum
    # ending_balance[t-1] = ending_balance[t] - net_change[t]
    # ⇒ ending_balance[t] = anchor - cumsum(net_change from t+1..latest)
    monthly_change["rev_cum_change"] = monthly_change["net_change"][::-1].cumsum()[::-1]
    last_rev_cum = monthly_change["rev_cum_change"].iloc[0]  # total change from first to last
    # Balance of the last month equals snapshot_total.
    # For each row, compute: balance_month = snapshot_total - (rev_cum_change - net_change_of_that_row)
    monthly_change["cash_balance_eur"] = snapshot_total - (monthly_change["rev_cum_change"] - monthly_change["net_change"])

    # Output
    out = monthly_change[["month", "cash_balance_eur"]].copy()
    out["cash_balance_eur"] = out["cash_balance_eur"].round(2)

    os.makedirs(os.path.dirname(CLEAN_OUT), exist_ok=True)
    out.to_csv(CLEAN_OUT, index=False)
    logging.info(f"Wrote monthly cash series to {CLEAN_OUT} (rows: {len(out)})")

if __name__ == "__main__":
    build_monthly_from_ledger()
