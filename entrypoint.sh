#!/usr/bin/env bash
set -euo pipefail

APP_HOME="${APP_HOME:-/opt/nifty_etl}"
DATA_DIR="${DATA_DIR:-/data/nifty_data}"
INTERVAL="${RUN_INTERVAL_SECONDS:-86400}"
LOGFILE="${DATA_DIR}/etl_container.log"

mkdir -p "${DATA_DIR}"
touch "${LOGFILE}"

cd "${APP_HOME}"

# initialize persistent dir if empty by running a full fetch once
initialize_if_empty(){
  if [ -z "$(ls -A "${DATA_DIR}" 2>/dev/null)" ]; then
    echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | Persistent data empty - running initial full fetch" >> "${LOGFILE}"
    # main.py should support a flag to do an initial full fetch (e.g., --init)
    python main.py --out-base "${DATA_DIR}" --init >> "${LOGFILE}" 2>&1 || echo "Initial fetch failed" >> "${LOGFILE}"
    echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | Initial fetch finished" >> "${LOGFILE}"
  fi
}

run_etl(){
  echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | START ETL" >> "${LOGFILE}"
  python main.py --out-base "${DATA_DIR}" >> "${LOGFILE}" 2>&1 || echo "ETL failed at $(date)" >> "${LOGFILE}"
  echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | END ETL" >> "${LOGFILE}"
}

if [[ "${1:-}" == "run_once" ]]; then
  initialize_if_empty
  run_etl
  exit 0
fi

# continuous loop: init once then run periodically
initialize_if_empty
while true; do
  run_etl
  echo "$(date -u +'%Y-%m-%dT%H:%M:%SZ') | sleeping for ${INTERVAL} seconds" >> "${LOGFILE}"
  sleep "${INTERVAL}"
done
