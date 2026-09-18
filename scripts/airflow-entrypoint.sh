#!/usr/bin/env bash
set -euo pipefail

# Generate the Simple Auth Manager password file from environment variables
# so credentials are never stored in a tracked file on disk.
if [[ -n "${AIRFLOW_ADMIN_USERNAME:-}" && -n "${AIRFLOW_ADMIN_PASSWORD:-}" ]]; then
    python -c 'import json, os; print(json.dumps({os.environ["AIRFLOW_ADMIN_USERNAME"]: os.environ["AIRFLOW_ADMIN_PASSWORD"]}))' \
        > "${AIRFLOW_HOME:-/opt/airflow}/simple_auth_manager_passwords.json.generated"
else
    echo "WARNING: AIRFLOW_ADMIN_USERNAME or AIRFLOW_ADMIN_PASSWORD not set, Airflow will generate a random password." >&2
fi

exec /entrypoint "$@"