# ===========================================================
# HOLDED TREASURY – HISTORICAL CASH BALANCE EXTRACTOR (RAW ONLY)
# ===========================================================
# Pulls month-window ledger entries from the Holded API (dailyledger)
# and writes RAW month-window records. A separate transform script
# should consume this RAW file to build the clean monthly cash series
# (e.g., compute net cash change for cash/bank accounts and backfill
# balances from an anchor).
#
# * INPUT:
#     - No input file; data fetched from Holded API
#     - Optional ENV overrides:
#         • HOLDED_API_KEY           → API key (required)
#         • TREASURY_START           → YYYY-MM-DD (default: 24 months ago, 1st day)
#         • TREASURY_END             → YYYY-MM-DD (default: today)
#         • HOLDED_DAILYLEDGER_ENDPOINT (optional override)
#             e.g. https://api.holded.com/api/accounting/v1/dailyledger?starttmp={start}&endtmp={end}
#
# * OUTPUT:
#     - RAW month windows (JSON):
#       data/INPUT/holded_treasury/raw/holded_treasury_dailyledger_month_windows.json
#
# * Features:
#     - Uses .env (local) or GitHub Secrets for API key
#     - **Auto-splits** date windows that hit the 250-row cap (offset appears ignored)
#     - Robust parsing; logs truncated response snippets on issues
#     - Logs all actions to logs/pipeline.log
# ===========================================================

import os
import json
import time
import math
import logging
import requests
from datetime import date, datetime, time as dtime, timezone, timedelta
from calendar import monthrange
from typing import List, Dict, Tuple, Optional
from dotenv import load_dotenv

# ============================================
# LOGGING SETUP
# ============================================

os.makedirs("logs", exist_ok=True)
logging.basicConfig(
    filename="logs/pipeline.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# ============================================
# LOAD API KEY
# ============================================

def load_api_key() -> str:
    if os.path.exists(".env"):
        load_dotenv()
    api_key = os.getenv("HOLDED_API_KEY", "").strip()
    if not api_key:
        logging.error("Missing HOLDED_API_KEY (check .env or GitHub Secrets).")
        raise ValueError("HOLDED_API_KEY not found.")
    return api_key

# ============================================
# DATE HELPERS
# ============================================

def parse_env_date(name: str, default: Optional[date]) -> date:
    raw = os.getenv(name, "").strip()
    if not raw:
        return default if default else date.today()
    try:
        return datetime.strptime(raw, "%Y-%m-%d").date()
    except ValueError:
        logging.warning(f"Invalid {name}='{raw}', falling back to default {default}.")
        return default if default else date.today()

def default_start_date(months_back: int = 24) -> date:
    today = date.today()
    # go back N months to the FIRST day of that month
    y, m = today.year, today.month
    m -= months_back
    while m <= 0:
        m += 12
        y -= 1
    return date(y, m, 1)

def month_end(y: int, m: int) -> date:
    return date(y, m, monthrange(y, m)[1])

def iter_month_windows(start_d: date, end_d: date) -> List[Tuple[str, date, date]]:
    """
    Returns list of tuples: (month_str, start_date, end_date)
      - month_str: 'YYYY-MM'
      - start_date: first day of month (or TREASURY_START if later)
      - end_date: last day of month (capped to TREASURY_END)
    """
    out: List[Tuple[str, date, date]] = []
    y, m = start_d.year, start_d.month
    while (y < end_d.year) or (y == end_d.year and m <= end_d.month):
        first = date(y, m, 1)
        last = month_end(y, m)
        start = max(first, start_d)
        end = min(last, end_d)
        out.append((f"{y:04d}-{m:02d}", start, end))
        m += 1
        if m == 13:
            m = 1
            y += 1
    return out

def to_epoch_seconds(d: date, end_of_day: bool = False) -> int:
    if end_of_day:
        dt = datetime.combine(d, dtime(23, 59, 59, tzinfo=timezone.utc))
    else:
        dt = datetime.combine(d, dtime(0, 0, 0, tzinfo=timezone.utc))
    return int(dt.timestamp())

# ============================================
# API CALL (DAILYLEDGER) + DE-CAPPING LOGIC
# ============================================

def _build_dailyledger_url(start_ts: int, end_ts: int, limit: int) -> str:
    """
    Build the dailyledger URL from env template; append limit/offset if missing.
    """
    tmpl = (os.getenv("HOLDED_DAILYLEDGER_ENDPOINT", "") or "").strip()
    if not tmpl:
        tmpl = "https://api.holded.com/api/accounting/v1/dailyledger?starttmp={start}&endtmp={end}"
    if "{start}" not in tmpl or "{end}" not in tmpl:
        raise ValueError("HOLDED_DAILYLEDGER_ENDPOINT must contain both '{start}' and '{end}' placeholders.")
    url = tmpl.format(start=start_ts, end=end_ts)
    # Append limit & a (ignored) offset hint unless caller already added a limit
    if "limit=" not in url:
        url += f"&limit={limit}&offset=0"
    return url

def fetch_dailyledger_for_window(api_key: str, start_ts: int, end_ts: int, limit: int = 250) -> list:
    """
    Calls dailyledger for the given epoch window and returns a list of entries.
    We explicitly include limit= to expose the 250 cap and handle it upstream.
    """
    url = _build_dailyledger_url(start_ts, end_ts, limit)
    headers = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "key": api_key,
    }

    resp = requests.get(url, headers=headers, timeout=45)
    ct = (resp.headers.get("content-type") or "").lower()

    if resp.status_code >= 400 or "text/html" in ct:
        snippet = resp.text[:400].replace("\n", " ")
        raise RuntimeError(f"dailyledger call failed: {url} -> status={resp.status_code}, ct={ct}, body~{snippet}")

    # Expect JSON (list or dict). Parse leniently.
    try:
        payload = resp.json()
    except ValueError:
        snippet = resp.text[:200].replace("\n", " ")
        raise RuntimeError(f"dailyledger returned non-JSON: ct={ct}, body~{snippet}")

    # Normalize to list of entries
    if isinstance(payload, dict) and "data" in payload and isinstance(payload["data"], list):
        entries = payload["data"]
    elif isinstance(payload, list):
        entries = payload
    else:
        # Fallback: unknown shape — wrap into a single-element list so len() behaves
        entries = [payload]

    return entries

def collect_entries_no_truncation(api_key: str, start_date: date, end_date: date, limit: int = 250,
                                  depth: int = 0, max_depth: int = 12) -> list:
    """
    Fetch entries for [start_date, end_date], splitting the window if we hit the 250 cap.
    This works around the API ignoring `offset` by bisecting until each sub-window < limit.
    """
    start_ts = to_epoch_seconds(start_date, end_of_day=False)
    end_ts   = to_epoch_seconds(end_date,   end_of_day=True)
    entries  = fetch_dailyledger_for_window(api_key, start_ts, end_ts, limit=limit)

    # If we hit the cap and can still split the window, bisect and recurse.
    span_days = (end_date - start_date).days
    if len(entries) >= limit and span_days > 0 and depth < max_depth:
        mid = start_date + timedelta(days=span_days // 2)
        left  = collect_entries_no_truncation(api_key, start_date, mid,  limit, depth+1, max_depth)
        right = collect_entries_no_truncation(api_key, mid + timedelta(days=1), end_date, limit, depth+1, max_depth)
        return left + right

    # Gentle pacing between leaf calls
    time.sleep(0.12)
    return entries

# ============================================
# EXTRACTOR (RAW ONLY)
# ============================================

def extract_holded_treasury():
    """
    Generates RAW month-window dailyledger records by querying the
    ledger for each month in [TREASURY_START, TREASURY_END].
    The transform step should derive monthly net cash changes and
    reconstruct balances using an anchor (e.g., today's treasury).
    """
    api_key = load_api_key()

    # Resolve date window
    start_d = parse_env_date("TREASURY_START", default_start_date(24))
    end_d   = parse_env_date("TREASURY_END", date.today())
    if end_d < start_d:
        raise ValueError("TREASURY_END is earlier than TREASURY_START.")

    windows = iter_month_windows(start_d, end_d)
    logging.info(f"Pulling Holded dailyledger for {len(windows)} month windows "
                 f"from {windows[0][1]} to {windows[-1][2]}.")

    # Prepare RAW output folder
    raw_dir = "data/INPUT/holded_treasury/raw"
    os.makedirs(raw_dir, exist_ok=True)

    raw_records: List[Dict] = []

    # Loop month windows
    for idx, (month_str, start_date, end_date) in enumerate(windows, start=1):
        try:
            entries = collect_entries_no_truncation(api_key, start_date, end_date, limit=250)
            raw_records.append({
                "month": month_str,
                "start": start_date.isoformat(),
                "end": end_date.isoformat(),
                "starttmp": to_epoch_seconds(start_date, end_of_day=False),
                "endtmp": to_epoch_seconds(end_date,   end_of_day=True),
                "entries": entries
            })

            count = len(entries)
            logging.info(f"  • {idx:02d}/{len(windows)} {month_str} [{start_date}→{end_date}] → {count} entries (after split if needed)")
        except Exception as e:
            logging.error(f"Window {month_str} [{start_date}→{end_date}] failed: {e}")
            raise

    # Save RAW month windows
    raw_path = os.path.join(raw_dir, "holded_treasury_dailyledger_month_windows.json")
    with open(raw_path, "w", encoding="utf-8") as f:
        json.dump(raw_records, f, indent=2, ensure_ascii=False)
    logging.info(f"Saved RAW dailyledger month windows to {raw_path} (windows: {len(raw_records)})")
    logging.info("Note: The CLEAN monthly cash series is produced by the transform script.")

# ============================================
# ENTRY POINT
# ============================================

if __name__ == "__main__":
    extract_holded_treasury()
