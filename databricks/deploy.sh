#!/bin/bash
# Deploys databricks/04_production_scheduler.py to the live Databricks workspace copy that the
# scheduled job actually runs. Editing the local .py file alone does NOT update what's live --
# Databricks Jobs run the workspace copy, which only changes when explicitly re-imported. See this
# directory's own README.md ("Deploying a change to the live notebook") for the manual equivalent
# this script wraps, and why pushing to git alone never updates Databricks.
#
# Usage (from the repo root or from inside databricks/):
#   ./databricks/deploy.sh [profile]
#
# profile defaults to $DATABRICKS_PROFILE if set, otherwise the databricks CLI's own default
# profile. If the default profile's token is stale/invalid, pass a working named profile explicitly
# (check `databricks auth profiles` for one), e.g.:
#   ./databricks/deploy.sh your-work-profile

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NOTEBOOK="04_production_scheduler"
LOCAL_FILE="${SCRIPT_DIR}/${NOTEBOOK}.py"
PROFILE="${1:-${DATABRICKS_PROFILE:-DEFAULT}}"

if [ ! -f "$LOCAL_FILE" ]; then
  echo "ERROR: ${LOCAL_FILE} not found." >&2
  exit 1
fi

echo "[1/3] Resolving your Databricks username (profile: ${PROFILE})..."
USERNAME=$(databricks current-user me --profile "$PROFILE" --output json | python3 -c "import json, sys; print(json.load(sys.stdin)['userName'])")
if [ -z "$USERNAME" ]; then
  echo "ERROR: could not resolve a Databricks username for profile '${PROFILE}'. Check 'databricks auth profiles' for a working profile." >&2
  exit 1
fi
WORKSPACE_PATH="/Users/${USERNAME}/Ahead-of-the-Storm-DATAPIPELINE/databricks/${NOTEBOOK}"
echo "      -> ${WORKSPACE_PATH}"

echo "[2/3] Importing ${LOCAL_FILE}..."
databricks workspace import \
  "$WORKSPACE_PATH" \
  --file "$LOCAL_FILE" \
  --language PYTHON --format SOURCE --overwrite \
  --profile "$PROFILE"

echo "[3/3] Verifying the deploy actually took (export + diff against local file)..."
TMP_CHECK="$(mktemp)"
trap 'rm -f "$TMP_CHECK"' EXIT
databricks workspace export "$WORKSPACE_PATH" --format SOURCE --profile "$PROFILE" > "$TMP_CHECK"
if diff -q "$TMP_CHECK" "$LOCAL_FILE" > /dev/null; then
  echo "DONE. Deployed content matches the local file exactly."
else
  echo "WARNING: deployed content differs from the local file -- diff below:" >&2
  diff "$TMP_CHECK" "$LOCAL_FILE" || true
  exit 1
fi
