#!/usr/bin/env python3
"""
Main ingestion job:
- fetch users from API
- transform with PII handling
- upsert into random_users.csv
- print summary for logs
"""

from pathlib import Path
import sys
from .api_client import fetch_random_users
from .transformations import transform_users
from .io_utils import upsert_random_users_csv
from .crypto_utils import BASE_DIR  # reuse project root from crypto_utils


def run_ingestion_job() -> dict: # this will be a dict of metrics about the ingestion job.
    """
    Run one ingestion cycle and return metrics as a dict
    (so FastAPI or other callers can inspect the result).
    """

    """
    Orchestrate the ingestion pipeline.

    Steps:
      1) Fetch raw users from RandomUser API.
      2) Transform into secure DataFrame (hash/encrypt PII).
      3) Append to CSV, drop duplicates.
      4) Extract retry info from the response object.
      5) Collect metrics.
      6) Print summary metrics.
    Returns:
        dict: Metrics about the ingestion job.
    """
    # --------------------------------------------------------------------------------------------------
    # 1) Fetch from API - Fetch raw users from RandomUser API.
    # --------------------------------------------------------------------------------------------------
    users, resp = fetch_random_users(timeout=15)
    # --------------------------------------------------------------------------------------------------
    # 2) Transform - Transform into secure DataFrame (hash/encrypt PII)
    # --------------------------------------------------------------------------------------------------
    df_raw, df_secure = transform_users(users)
    # --------------------------------------------------------------------------------------------------
    # 3) Write/append to CSV - Append to CSV, drop duplicates.
    # --------------------------------------------------------------------------------------------------
    df_final, csv_path = upsert_random_users_csv(df_secure, BASE_DIR)
    # --------------------------------------------------------------------------------------------------
    # 4) Extract retry info from the response object
    # --------------------------------------------------------------------------------------------------
    raw = getattr(resp, "raw", None)
    retries = getattr(raw, "retries", None)
    retries_total = getattr(retries, "total", 0)
    # The "resp" object sometimes contains deeper info. It has different nested attributes we can access in different levels.
    # It follows this pattern:
    # resp → raw → retries → total = resp.raw.retries.total
    # If it doesn't have that info, we default to None to avoid errors.
    
    # Part1: getattr(resp, "raw", None): This tries to get the "raw" attribute from the response object. If "raw" doesn't 
    # exist, it returns None.

    # Part2: getattr(raw, "retries", None): This tries to get the "retries" attribute from the "raw" object. If "retries" 
    # doesn't exist, it returns None.

    # Part3: getattr(retries, "total", 0) This accesses the "total" attribute of the "retries" object, which indicates the 
    # total number of retries that were attempted. If there were no retries attribute (e.g., if no retries were needed), 
    # it defaults to 0.
    
    # --------------------------------------------------------------------------------------------------
    # 5) Collect the previous metrics about the ingestion job
    # --------------------------------------------------------------------------------------------------
    metrics = { # could be logged or sent to monitoring system
        "http_status": resp.status_code,
        "retries_used": retries_total,
        "rows_fetched": len(df_raw),
        "rows_after_dedup": len(df_final),
        "csv_path": str(csv_path),
    }

    # --------------------------------------------------------------------------------------------------
    # 6) Print summary info for logging/monitoring (stdout → your .sh logs)
    # --------------------------------------------------------------------------------------------------
    
    print(f"wrote {metrics['rows_after_dedup']} rows to {metrics['csv_path']}")
    print("api_url=https://randomuser.me ...")
    print(
        f"http_status={metrics['http_status']} "
        f"retries_used={metrics['retries_used']}"
    )
    print(
        f"rows_fetched={metrics['rows_fetched']} "
        f"rows_after_dedup={metrics['rows_after_dedup']} "
        f"output={metrics['csv_path']}"
    )

    return metrics

def main() -> int: # This function will be called when the script is executed directly.
    _ = run_ingestion_job() 
    # We have run_ingestion_job() that FastAPI can import.
    # The run_ingestion_job() triggers the steps that our program needs and return metrics as a dict to use if needed.
    # By convention in Python, _ is used when you don’t care about the value returned because you won’t use it here.
    # run_ingestion_job() returns a dict with metrics, but main() doesn’t need it.
    return 0 # Returns int exit code (0 = success, non-zero = failure).

# Only run the job when this file is executed directly (not when it's imported)
if __name__ == "__main__":
    # Exit code is important for cron / the .sh wrapper.
    raise SystemExit(main()) #SystemExit(0) tells the OS: “Program finished successfully”
