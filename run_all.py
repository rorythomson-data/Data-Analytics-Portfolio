import subprocess
import logging
import os
import sys
import time
from datetime import datetime

# ------------------- CONFIG -------------------
API_SCRIPT = "run_api_scripts.py"
METRICS_SCRIPT = "metrics_pipeline.py"

# ------------------- DEBUG: SHOW PYTHON INTERPRETER -------------------
print(f" Using Python interpreter: {sys.executable}")

# ------------------- LOGGING -------------------
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, "run_all.log")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(log_file, mode='w', encoding='utf-8'),
        logging.StreamHandler()
    ]
)

status_summary = []

def run_script(script_name):
    """Run a Python script and log its success or failure."""
    logging.info(f" Starting {script_name} ...")
    print(f" Starting {script_name} ...")
    start_time = time.time()

    try:
        result = subprocess.run(
            [sys.executable, script_name],  # Ensures we use the same interpreter
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace"
        )
        elapsed = round(time.time() - start_time, 2)
        if result.returncode == 0:
            logging.info(f"✔ {script_name} - SUCCESS ({elapsed}s)")
            print(f"✔ {script_name} - SUCCESS ({elapsed}s)")
            status_summary.append((script_name, "SUCCESS", elapsed))
        else:
            logging.error(f"✖ {script_name} - FAILED ({elapsed}s)")
            logging.error(result.stderr)
            print(f"✖ {script_name} - FAILED ({elapsed}s)")
            status_summary.append((script_name, "FAILED", elapsed))
    except Exception as e:
        elapsed = round(time.time() - start_time, 2)
        logging.exception(f"✖ {script_name} - ERROR: {e} ({elapsed}s)")
        print(f"✖ {script_name} - ERROR ({elapsed}s)")
        status_summary.append((script_name, "ERROR", elapsed))

def save_summary():
    """Save a summary of all scripts' statuses to OUTPUT/YYYY-MM/."""
    current_month = datetime.now().strftime("%Y-%m")
    output_dir = os.path.join("data", "OUTPUT", current_month)
    os.makedirs(output_dir, exist_ok=True)
    summary_file = os.path.join(output_dir, "pipeline_status.txt")

    with open(summary_file, "w", encoding="utf-8") as f:
        f.write("PIPELINE STATUS SUMMARY\n")
        f.write("=======================\n")
        for script, status, elapsed in status_summary:
            f.write(f"{script}: {status} ({elapsed}s)\n")

    logging.info(f"Summary saved at {summary_file}")
    print(f"\n Summary saved at {summary_file}")

def run_pipeline():
    logging.info("Starting Full Pipeline Execution")
    print("Starting Full Pipeline Execution\n")

    # Step 1: Run API scripts
    run_script(API_SCRIPT)

    # Step 2: Run metrics pipeline
    run_script(METRICS_SCRIPT)

    # Save summary
    save_summary()
    logging.info("✅ Pipeline run completed.")
    print("✅ Pipeline run completed.")

# ------------------- MAIN -------------------
if __name__ == "__main__":
    run_pipeline()
