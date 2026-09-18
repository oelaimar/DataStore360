#!/usr/bin/env bash
set -euo pipefail

AIRFLOW_HOME="${AIRFLOW_HOME:-/opt/airflow}"

# When running as root, fix ownership of the runtime directories so that the
# airflow user (AIRFLOW_UID) can read/write them. When the compose file runs
# the container as "${AIRFLOW_UID}:0" (the host uid), the bind mounts already
# have the right ownership and this is skipped.
if [[ "$(id -u)" == "0" ]]; then
    AIRFLOW_UID="${AIRFLOW_UID:-50000}"
    chown -R "${AIRFLOW_UID}:0" \
        "${AIRFLOW_HOME}/dags" \
        "${AIRFLOW_HOME}/logs" \
        "${AIRFLOW_HOME}/plugins" \
        "${AIRFLOW_HOME}/data" \
        "${AIRFLOW_HOME}/reports"
fi

# Generate the Simple Auth Manager password file from environment variables
# so credentials are never stored in a tracked file on disk. Airflow reads
# this file by default (core.simple_auth_manager_passwords_file).
if [[ -n "${AIRFLOW_ADMIN_USERNAME:-}" && -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
    python -c 'import json, os; print(json.dumps({os.environ["AIRFLOW_ADMIN_USERNAME"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]}))' \
        > "${AIRFLOW_HOME}/simple_auth_manager_passwords.json.generated"
else
    echo "WARNING: AIRFLOW_ADMIN_USERNAME or AIRFLOW_ADMIN_PASSWORD not set, Airflow will generate a random password." >&2
fi

exec /entrypoint "$@"