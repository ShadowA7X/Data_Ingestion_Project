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


def main() -> int:
    """
    Orchestrate the ingestion pipeline.

    Steps:
      1) Fetch raw users from RandomUser API.
      2) Transform into secure DataFrame (hash/encrypt PII).
      3) Append to CSV, drop duplicates.
      4) Print summary metrics.

    Returns:
      int exit code (0 = success, non-zero = failure)
    """
    # 1) Fetch from API
    users, resp = fetch_random_users(timeout=15)

    # 2) Transform
    df_raw, df_secure = transform_users(users)

    # 3) Write/append to CSV
    df_final, csv_path = upsert_random_users_csv(df_secure, BASE_DIR)

    # 4) Print summary info for logging/monitoring
    print(f"wrote {len(df_final)} rows to {csv_path}")
    print("api_url=https://randomuser.me ...")

    # retries_used is a bit low-level; we guard it to avoid AttributeError
    retries = getattr(getattr(resp, "raw", None), "retries", None)
    retries_total = retries.total if retries else 0

    print(f"http_status={resp.status_code} retries_used={retries_total}")
    print(
        f"rows_fetched={len(df_raw)} "
        f"rows_after_dedup={len(df_final)} "
        f"output={csv_path}"
    )

    return 0

# Only run the job when this file is executed directly (not when it's imported)
if __name__ == "__main__":
    # Exit code is important for cron / your .sh wrapper.
    raise SystemExit(main())
