# Snowflake Container Services (SPCS) Deployment Guide

This guide explains how to deploy the Hurricane Impact Analysis Pipeline to Snowflake Container Services (SPCS).

## Overview

The Impact Analysis Pipeline processes hurricane forecast data from Snowflake tables and generates impact views and reports. When deployed to SPCS, it:

- Reads hurricane track and envelope data from Snowflake tables (`TC_TRACKS`, `TC_ENVELOPES_COMBINED`)
- Processes geospatial impact analysis (schools, health centers, population)
- Generates impact views and reports
- Writes results to Snowflake internal stage

## Prerequisites

1. **Snowflake CLI** installed and configured
   ```bash
   pip install snowflake-cli-labs
   snow connection add <your-connection-name>
   ```

2. **Docker** installed and running

3. **Snowflake account** with SPCS enabled

4. **Compute Pool** created in Snowflake
   ```sql
   -- View available compute pool instance families
   SHOW COMPUTE POOL INSTANCE FAMILIES;
   
   -- Create compute pool (adjust instance family as needed)
   CREATE COMPUTE POOL IF NOT EXISTS impact_analysis_pool
     MIN_NODES = 1
     MAX_NODES = 1
     INSTANCE_FAMILY = CPU_X64_M;
   ```

5. **Image Repository** created in Snowflake
   ```sql
   CREATE OR REPLACE IMAGE REPOSITORY SERVICES;
   ```
   This should be run in the database and schema where you plan to deploy your SPCS service.

6. **Snowflake Tables** must exist:
   - `TC_TRACKS` - Hurricane track data
   - `TC_ENVELOPES_COMBINED` - Hurricane envelope data

7. **Snowflake Stage** must exist (if using `DATA_PIPELINE_DB=SNOWFLAKE`):
   ```sql
   CREATE OR REPLACE STAGE impact_analysis_stage;
   ```

## Building the Docker Image

From the repository root directory:

```bash
docker build -f snowflake/Dockerfile -t impact-analysis-pipeline:latest . --platform=linux/amd64
```

**Important:** The `--platform=linux/amd64` flag is required for SPCS compatibility.

This will:
- Install all system dependencies (geospatial libraries, GDAL, PROJ, GEOS)
- Install Python dependencies from `requirements.txt`
- Copy all pipeline modules
- Set up the container entrypoint

## Tagging the Image for Snowflake Registry

### Step 1: Get Registry URL

```bash
snow spcs image-registry url --connection default
```

This will output something like: `orgname-account.registry.snowflakecomputing.com`

### Step 2: Tag the Image

The image tag format is:
```
<registry-url>/<database>/<schema>/<service>/<image-name>:<tag>
```

**Important:** Database and schema names must be **lowercase** in the Docker tag (even if they use uppercase in Snowflake).

Example:
```bash
docker tag impact-analysis-pipeline:latest \
  orgname-account.registry.snowflakecomputing.com/mydatabase/myschema/myservice/impact-analysis-pipeline:latest
```

Where:
- `orgname-account` = your registry URL (from `snow spcs image-registry url`)
- `mydatabase` = your database name (lowercase)
- `myschema` = your schema name (lowercase)
- `myservice` = your service name (lowercase)
- `impact-analysis-pipeline` = your image name
- `latest` = tag/version

## Pushing to Snowflake Registry

### Step 1: Authenticate with Snowflake Registry

```bash
snow spcs image-registry login --connection default
```

This automatically logs Docker into your Snowflake image registry.

### Step 2: Push the Image

```bash
docker push orgname-account.registry.snowflakecomputing.com/mydatabase/myschema/myservice/impact-analysis-pipeline:latest
```

## Running the Pipeline

### Option 1: Running Locally (for Testing)

```bash
docker run --rm \
  -e DATA_PIPELINE_DB=SNOWFLAKE \
  -e SNOWFLAKE_ACCOUNT='your-account' \
  -e SNOWFLAKE_USER='your_user' \
  -e SNOWFLAKE_PASSWORD='your_password' \
  -e SNOWFLAKE_WAREHOUSE='your_warehouse' \
  -e SNOWFLAKE_DATABASE='your_database' \
  -e SNOWFLAKE_SCHEMA='your_schema' \
  -e SNOWFLAKE_STAGE_NAME='your_stage' \
  impact-analysis-pipeline:latest \
  --type initialize --countries TWN --zoom 14
```

### Option 2: Running in SPCS (Production)

Execute the pipeline as a SPCS job using `EXECUTE JOB SERVICE`:

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL impact_analysis_pool
   NAME = impact_analysis_job
   ASYNC = TRUE
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: impact-analysis-pipeline
       image: /your_database/your_schema/your_service/impact-analysis-pipeline:latest
       env:
          # SPCS OAuth (automatically handled when SPCS_RUN=true)
          SPCS_RUN: true
          
          # Snowflake connection (required)
          SNOWFLAKE_ACCOUNT: your-account
          SNOWFLAKE_DATABASE: your_database
          SNOWFLAKE_SCHEMA: your_schema
          SNOWFLAKE_WAREHOUSE: your_warehouse
          
          # Storage configuration
          DATA_PIPELINE_DB: SNOWFLAKE
          SNOWFLAKE_STAGE_NAME: your_stage
          
          # Pipeline parameters
          ZOOM_LEVEL: "14"
          REWRITE: "0"
       args:
         - "--type"
         - "update"
         - "--countries"
         - "TWN"
         - "--time_delta"
         - "9"
   $$;
```

### Option 3: Running as a Scheduled Job

Create a scheduled job that runs the pipeline periodically:

```sql
CREATE OR REPLACE JOB impact_analysis_scheduled_job
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours
  AS
  EXECUTE JOB SERVICE
    IN COMPUTE POOL impact_analysis_pool
    NAME = impact_analysis_job
    ASYNC = TRUE
    FROM SPECIFICATION $$
    spec:
      containers:
      - name: impact-analysis-pipeline
        image: /your_database/your_schema/your_service/impact-analysis-pipeline:latest
        env:
          SPCS_RUN: true
          SNOWFLAKE_ACCOUNT: your-account
          SNOWFLAKE_DATABASE: your_database
          SNOWFLAKE_SCHEMA: your_schema
          SNOWFLAKE_WAREHOUSE: your_warehouse
          DATA_PIPELINE_DB: SNOWFLAKE
          SNOWFLAKE_STAGE_NAME: your_stage
        args:
          - "--type"
          - "update"
          - "--countries"
          - "TWN"
          - "--time_delta"
          - "9"
    $$;
```

## Environment Variables

### Required (SPCS Mode)

- `SPCS_RUN`: Set to `true` when running in SPCS (enables OAuth authentication)
- `SNOWFLAKE_ACCOUNT`: Your Snowflake account identifier
- `SNOWFLAKE_DATABASE`: Database name
- `SNOWFLAKE_SCHEMA`: Schema name
- `SNOWFLAKE_WAREHOUSE`: Warehouse name (for non-SPCS connections)

### Required (if using Snowflake storage)

- `DATA_PIPELINE_DB`: Set to `SNOWFLAKE` to use Snowflake stage for storage
- `SNOWFLAKE_STAGE_NAME`: Name of the Snowflake stage to use

### Optional

- `ZOOM_LEVEL`: Zoom level for mercator tiles (default: `14`)
- `REWRITE`: Set to `1` to force reprocessing, `0` to skip existing (default: `0`)
- `ROOT_DATA_DIR`: Base data directory (default: `geodb`)
- `VIEWS_DIR`: Views subdirectory (default: `aos_views`)
- `RESULTS_DIR`: Results directory (default: `results`)

## Pipeline Arguments

The pipeline accepts command-line arguments:

- `--type`: Pipeline mode (`initialize` or `update`)
- `--countries`: Space-separated list of country codes (e.g., `TWN DOM VNM`)
- `--zoom`: Zoom level for tiles (default: `14`)
- `--rewrite`: Set to `1` to force reprocessing (default: `0`)
- `--time_delta`: Number of days in the past to consider storms (default: `9`)
- `--date`: Process only storms on a specific date (YYYY-MM-DD format)
- `--storm`: Process only a specific storm (e.g., `FUNG-WONG`)

## Example: Initialize Pipeline for Taiwan

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL impact_analysis_pool
   NAME = impact_analysis_init
   ASYNC = TRUE
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: impact-analysis-pipeline
       image: /your_database/your_schema/your_service/impact-analysis-pipeline:latest
       env:
          SPCS_RUN: true
          SNOWFLAKE_ACCOUNT: your-account
          SNOWFLAKE_DATABASE: your_database
          SNOWFLAKE_SCHEMA: your_schema
          SNOWFLAKE_WAREHOUSE: your_warehouse
          DATA_PIPELINE_DB: SNOWFLAKE
          SNOWFLAKE_STAGE_NAME: your_stage
       args:
         - "--type"
         - "initialize"
         - "--countries"
         - "TWN"
         - "--zoom"
         - "14"
         - "--rewrite"
         - "0"
   $$;
```

## Example: Process Specific Storm

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL impact_analysis_pool
   NAME = impact_analysis_update
   ASYNC = TRUE
   FROM SPECIFICATION $$
   spec:
     containers:
     - name: impact-analysis-pipeline
       image: /your_database/your_schema/your_service/impact-analysis-pipeline:latest
       env:
          SPCS_RUN: true
          SNOWFLAKE_ACCOUNT: your-account
          SNOWFLAKE_DATABASE: your_database
          SNOWFLAKE_SCHEMA: your_schema
          SNOWFLAKE_WAREHOUSE: your_warehouse
          DATA_PIPELINE_DB: SNOWFLAKE
          SNOWFLAKE_STAGE_NAME: your_stage
       args:
         - "--type"
         - "update"
         - "--countries"
         - "TWN"
         - "--date"
         - "2025-11-10"
         - "--storm"
         - "FUNG-WONG"
   $$;
```

## Monitoring and Logs

SPCS jobs provide logs and monitoring through Snowflake:

```sql
-- View job status
SHOW JOBS;

-- View job logs
SELECT SYSTEM$GET_JOB_LOGS('impact_analysis_job');
```

## Troubleshooting

### Image Build Fails

- Ensure you're using `--platform=linux/amd64` flag
- Check that all required files are present in the repository root

### Container Fails to Start

- Verify all required environment variables are set
- Check that the Snowflake stage exists and is accessible
- Ensure SPCS_RUN=true when running in SPCS

### Authentication Errors

- In SPCS mode, ensure `SPCS_RUN=true` is set
- The OAuth token is automatically provided by SPCS at `/snowflake/session/token`
- For local testing, use `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`

### Missing Data

- Verify that `TC_TRACKS` and `TC_ENVELOPES_COMBINED` tables contain data
- Check that the specified countries have been initialized (run `--type initialize` first)