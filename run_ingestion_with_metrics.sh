#!/usr/bin/env bash
set -euo pipefail

BASE="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="$BASE/env/bin/python"
JOB="$BASE/API_Ingestion.py"
LOG_DIR="$BASE/logs"
LOG="$LOG_DIR/cron-$(date -u +%Y%m%d).log"
LOCKDIR="$BASE/.run_lock"


mkdir -p "$LOG_DIR"

# Log everything from here (stdout+stderr) into the daily log
exec >> "$LOG" 2>&1

# prevent overlapping runs
if mkdir "$LOCKDIR" 2>/dev/null; then
  trap 'rmdir "$LOCKDIR"' EXIT
else
  exit 0
fi

cd "$BASE"

# ---- structured run header
START_TS_UTC="$(date -u '+%Y-%m-%d %H:%M:%S %Z')"
START_SEC=$(date +%s)
echo "==== RUN START ${START_TS_UTC} ===="
RUN_ID=$(uuidgen)
echo "run_id=$RUN_ID"
echo "pwd=$(pwd) py=$("$PY" -V) job=$(basename "$JOB") host=$(hostname)"

# run job
"$PY" "$JOB"
rc=$?

END_TS_UTC="$(date -u '+%Y-%m-%d %H:%M:%S %Z')"
END_SEC=$(date +%s)
DUR=$((END_SEC-START_SEC))

if [[ $rc -eq 0 ]]; then
  echo "status=SUCCESS duration_sec=$DUR"
else
  echo "status=FAILURE exit_code=$rc duration_sec=$DUR"
fi

echo "==== RUN END   ${END_TS_UTC} ===="
