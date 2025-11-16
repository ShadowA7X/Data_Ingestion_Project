#!/usr/bin/env bash

#--------------------------------------------------------------------------------------------------------
#-- Description: Run the API_Ingestion.py script with logging, locking, and metrics.
#--------------------------------------------------------------------------------------------------------
# Its job is to run the Python script (API_Ingestion.py) in a safe, reliable, and measurable way. 
# It adds important features that the Python script doesn't handle by itself, like:
# 1) Logging: It captures all output and errors to a daily log file.
# 2) Locking: It prevents the script from running twice at the same time.
# 3) Metrics: It records the start time, end time, duration, and success/failure status of the job.

#--------------------------------------------------------------------------------------------------------
set -euo pipefail 
# This is a best practice that makes your script fail fast, fail loudly, and fail safely, preventing it from running
# in an unpredictable or broken state.
# The options mean:
# -e : Exit immediately if a command fails.
# -u : Treat unset variables as an error and exit immediately. For example, if you defined LOG_DIR but accidentally
#      typed LOG_DR later, the script will stop instead of using an empty, broken path.
# -o pipefail : It controls how errors are handled in "pipelines" (when you chain commands with |) By default, only 
#.              the last command's error matters. This option makes it so if any command in the pipeline fails, 
#               the whole pipeline is considered a failure.
#--------------------------------------------------------------------------------------------------------
BASE="$(cd -- "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# (1) "$(dirname "${BASH_SOURCE[0]}")": Finds the directory where the script (run_ingestion_with_metrics.sh) is located.
# (2) cd -- "...": Changes the terminal's current location to that directory.
# (3) && pwd: If the cd was successful, this prints the full, absolute path to that directory (BASE)
# (4) BASE=...: The final, absolute path (like /Users/jasonaraos/.../Data_Ingestion) is stored in the BASE variable 
# Goal: This makes the script portable. You can run it from anywhere, and BASE will always correctly point to the project's root folder.


PY="$BASE/venv/bin/python" # path to python interpreter
JOB_MODULE="ingestion.job" # Python module to run (package.module)
LOG_DIR="$BASE/logs" # path to log directory
LOG="$LOG_DIR/cron-$(date -u +%Y%m%d).log" # daily log file path
LOCKDIR="$BASE/.run_lock" # path to lock directory


mkdir -p "$LOG_DIR" # ensure log directory exists

# ---------------------------------------------------------------------------------
# Redirect all output to log file and Locking mechanism to prevent overlapping runs
# ---------------------------------------------------------------------------------

#----------Redirect all output (stdout and stderr) to log file
exec >> "$LOG" 2>&1 
# Explanation:
# Think of your script as having two "pipes" coming out of it:
# Pipe 1 (stdout): For normal output (like our print statements).
# Pipe 2 (stderr): For error messages.
# This command does the following:
# exec: Says "this new rule applies to the rest of the script."
# >> "$LOG": This takes Pipe 1 (stdout) and redirects it to append (>>) to your log file ($LOG).
# 2>&1: This takes Pipe 2 (stderr) and redirects it (2>) to the same place as Pipe 1 (&1), which is now your log file.
# In a Nutshell: After this line, all normal output and all error messages will be captured and saved in your 
# log file (.../logs/cron-20251115.log). This is how you log everything that happens.

# ----------Locking mechanism to prevent overlapping runs
if mkdir "$LOCKDIR" 2>/dev/null; then # try to create lock directory (.run_lock)
  trap 'rmdir "$LOCKDIR"' EXIT # ensure lock directory (.run_lock) is removed on script exit (when the final line of this script is reached)
else
  exit 0 # exit if lock directory (.run_lock) already exists (another instance is running)
fi
# Explanation:
# This part ensures that only one instance of this script runs at any given time.
# This is important because if the script is still running from a previous execution,
# starting a new one could cause conflicts, data corruption, or excessive resource usage.
# The locking is done using a directory ($LOCKDIR). The mkdir command is atomic, meaning it either creates the directory
# or fails if it already exists (those fails are not prompted to the user because of 2>/dev/null).
# If it succeeds, the script continues; if it fails, the script exits immediately.
# The trap command ensures that when the script finishes (whether it ends normally or is interrupted),
# the lock directory is removed, allowing future runs of the script.
# ---------------------------------------------------------------------------------

# ------------------------------------------------------------------------
# ---------------- Main Script Execution with Metrics Logging ------------
# ------------------------------------------------------------------------

cd "$BASE" # change to base directory

# --------Initial variables for metrics
# Once you execute the script run_ingestion_with_metrics.sh, these variables will help you track and log important information about the run 
RUN_ID=$(uuidgen) # generate unique run id for this execution
START_TS_UTC="$(date -u '+%Y-%m-%d %H:%M:%S %Z')" # start timestamp to log when the job started
START_SEC=$(date +%s) # start time in seconds since epoch to calculate duration later

# --------Log initial metrics: start time, run id, job name, host, python version
# This will print important information about the job run to the log file thanks to the earlier redirection setup (exec >> "$LOG" 2>&1) 
echo "==== RUN START ${START_TS_UTC} ===="
echo "Run_id=$RUN_ID"
echo "Pwd=$(pwd)"
echo "Py=$("$PY" -V)"
echo "Job=$(basename "$JOB_MODULE")"
echo "Host=$(hostname)"

# ----------------------------------------------------------------------------------------
# ----------------Execute the job script and capture its exit code------------------------
# ----------------------------------------------------------------------------------------
# run job
"$PY" -m "$JOB_MODULE" # execute the Python script (ingestion.py) using the specified Python interpreter
# -m means: run thi Python module inside a package, not a standalone script file. This says:
# 1) Find a package called ingestion
# 2) Inside it, find a module called job
# 3) Run that module as the main program

rc=$? # capture the exit code of the Python script
# ---------------------------------------------------------------------------------------- 
# ---------------------------------------------------------------------------------------- 

# --------Final metrics: end time, duration, status
END_TS_UTC="$(date -u '+%Y-%m-%d %H:%M:%S %Z')" # end timestamp
END_SEC=$(date +%s) # end time in seconds since epoch
DUR=$((END_SEC-START_SEC)) # duration in seconds

if [[ $rc -eq 0 ]]; then # check if exit code indicates success ($rc = 0 means success)
  echo "Status=SUCCESS"
  echo "Duration_sec=$DUR"
else
  echo "Status=FAILURE"
  echo "Exit_code=$rc"
  echo "Duration_sec=$DUR"
fi

echo "==== RUN END   ${END_TS_UTC} ===="
echo ""