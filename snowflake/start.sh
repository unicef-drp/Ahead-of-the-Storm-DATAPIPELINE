#!/bin/bash
# Startup script for Impact Analysis Pipeline
# Logs configuration and executes the pipeline with passed arguments

set -e

echo "==========================================="
echo "Impact Analysis Pipeline Starting"
echo "==========================================="
echo "Storage Backend: ${DATA_PIPELINE_DB:-LOCAL}"
if [ "${DATA_PIPELINE_DB}" = "SNOWFLAKE" ] || [ "${SNOWFLAKE_DATABASE}" != "" ]; then
  echo "Snowflake Database: ${SNOWFLAKE_DATABASE:-<not set>}"
  echo "Snowflake Schema: ${SNOWFLAKE_SCHEMA:-<not set>}"
  echo "Snowflake Stage: ${SNOWFLAKE_STAGE_NAME:-<not set>}"
fi
echo "SPCS Mode: ${SPCS_RUN:-false}"
echo "==========================================="

# Run the pipeline with passed arguments
exec python main_pipeline.py "$@"

