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
   snow connection add <connection-name>
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

5. **Network Security Configuration**
   
   The pipeline requires network connectivity to external APIs (GeoRepo, GADM, geoBoundaries/HDX). Configure network rules and external access integration:
   
   ```sql
   USE ROLE ACCOUNTADMIN;
   
   -- Create network rule for egress access (allows outbound HTTP/HTTPS)
   CREATE OR REPLACE NETWORK RULE impact_analysis_egress_access
     MODE = EGRESS
     TYPE = HOST_PORT
     VALUE_LIST = ('0.0.0.0:80', '0.0.0.0:443');
   
   -- Create external access integration
   CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION impact_analysis_egress_access_integration
     ALLOWED_NETWORK_RULES = (impact_analysis_egress_access)
     ENABLED = true;
   ```
   
   **Note:** The network rule allows outbound HTTP (port 80) and HTTPS (port 443) to any destination (0.0.0.0). This is required for accessing external APIs:
   - GeoRepo (UNICEF administrative boundaries API)
   - GADM (Global Administrative Areas)
   - HDX/geoBoundaries (Humanitarian Data Exchange)
   - Other geospatial data sources

6. **Image Repository** created in Snowflake
   ```sql
   CREATE OR REPLACE IMAGE REPOSITORY SERVICES;
   ```
   This should be run in the database and schema where the SPCS service will be deployed.

7. **Snowflake Tables** must exist:
   - `TC_TRACKS` - Hurricane track data
   - `TC_ENVELOPES_COMBINED` - Hurricane envelope data

8. **Snowflake Stage** must exist (if using `DATA_PIPELINE_DB=SNOWFLAKE`):
   
   The stage must be created in the same database and schema where the pipeline is running:
   
   ```sql
  -- Create the stage (replace AOTS_ANALYSIS with the stage name from SNOWFLAKE_STAGE_NAME env var)
   CREATE OR REPLACE STAGE AOTS_ANALYSIS;
   
   -- Grant usage on the stage to the role that will run the pipeline
   GRANT USAGE ON STAGE AOTS_ANALYSIS TO ROLE <role_name>;
   ```
   
   **Important:** The stage name must match the value of `SNOWFLAKE_STAGE_NAME` environment variable exactly (case-sensitive).

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

This outputs something like: `orgname-account.registry.snowflakecomputing.com`

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
- `orgname-account` = registry URL (from `snow spcs image-registry url`)
- `mydatabase` = database name (lowercase)
- `myschema` = schema name (lowercase)
- `myservice` = service name (lowercase)
- `impact-analysis-pipeline` = image name
- `latest` = tag/version

## Pushing to Snowflake Registry

### Step 1: Authenticate with Snowflake Registry

```bash
snow spcs image-registry login --connection default
```

This automatically logs Docker into the Snowflake image registry.

### Step 2: Push the Image

```bash
docker push orgname-account.registry.snowflakecomputing.com/mydatabase/myschema/myservice/impact-analysis-pipeline:latest
```

## Running the Pipeline

### Option 1: Running Locally (for Testing)

Test the container locally with Snowflake stage before deploying to SPCS:

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

**Note:** This uses password authentication to connect to Snowflake. The container runs locally but writes to Snowflake stage, allowing testing of the full pipeline before deploying to SPCS.

### Option 2: Running in SPCS (Production)

Execute the pipeline as a SPCS job using `EXECUTE JOB SERVICE`:

```sql
EXECUTE JOB SERVICE
   IN COMPUTE POOL impact_analysis_pool
   NAME = impact_analysis_job
   ASYNC = TRUE
   EXTERNAL_ACCESS_INTEGRATIONS = (IMPACT_ANALYSIS_EGRESS_ACCESS_INTEGRATION)
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
          
          # SPCS internal network (optional, but recommended for SPCS)
          # These are automatically provided by SPCS but can be explicitly set
          # SNOWFLAKE_HOST: <internal-host>
          # SNOWFLAKE_PORT: <internal-port>
          
          # Storage configuration
          DATA_PIPELINE_DB: SNOWFLAKE
          SNOWFLAKE_STAGE_NAME: your_stage
          
          # Pipeline parameters
          ZOOM_LEVEL: "14"
          REWRITE: "0"
          
          # Optional: SSL/certificate handling (set to true if experiencing certificate issues)
          # SNOWFLAKE_INSECURE_MODE: false
       args:
         - "--type"
         - "update"
         - "--countries"
         - "TWN"
         - "--time_delta"
         - "9"
   $$;
```

### Option 3: Running as a Scheduled Job (Automatic Processing)

Create a scheduled job that automatically processes the latest storms:

```sql
CREATE OR REPLACE JOB impact_analysis_auto_latest_storms
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'  -- Every 6 hours at minute 0
  COMMENT = 'Automatically process latest storms for all active countries'
  AS
  EXECUTE JOB SERVICE
    IN COMPUTE POOL impact_analysis_pool
    NAME = impact_analysis_latest_storms
    ASYNC = TRUE
    EXTERNAL_ACCESS_INTEGRATIONS = (IMPACT_ANALYSIS_EGRESS_ACCESS_INTEGRATION)
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
          ZOOM_LEVEL: "14"
          REWRITE: "0"
          # API Keys (required for data fetching)
          GIGA_SCHOOL_LOCATION_API_KEY: <your_giga_api_key>
          HEALTHSITES_API_KEY: <your_healthsites_api_key>
          GEOREPO_API_KEY: <your_georepo_api_key>
          GEOREPO_USER_EMAIL: <your_georepo_email>
        args:
          - "--type"
          - "update"
          - "--time_delta"
          - "9"
          # No --countries specified: will use all active countries from Snowflake PIPELINE_COUNTRIES table
    $$;
```

**Key Points:**
- `--time_delta 9` processes storms from the last 9 days
- No `--countries` specified: automatically uses all active countries from the `PIPELINE_COUNTRIES` table
- Runs automatically on the schedule (every 6 hours in this example)

**Schedule Examples:**
- `'USING CRON 0 */6 * * * UTC'` - Every 6 hours
- `'USING CRON 0 */12 * * * UTC'` - Every 12 hours  
- `'USING CRON 0 0 * * * UTC'` - Daily at midnight UTC
- `'USING CRON 0 0,6,12,18 * * * UTC'` - 4 times per day (00:00, 06:00, 12:00, 18:00 UTC)
- `'USING CRON 0 0 * * 1 UTC'` - Weekly on Mondays at midnight UTC


## Environment Variables

### Required (SPCS Mode)

- `SPCS_RUN`: Set to `true` when running in SPCS (enables OAuth authentication)
- `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE_DATABASE`: Database name
- `SNOWFLAKE_SCHEMA`: Schema name
- `SNOWFLAKE_WAREHOUSE`: Warehouse name

### API Keys (Required for Data Fetching)

- `GIGA_SCHOOL_LOCATION_API_KEY`: Required for fetching school locations via GIGA API
- `HEALTHSITES_API_KEY`: Required for fetching health center locations via HealthSites API
- `GEOREPO_API_KEY`: Optional but preferred for fetching administrative boundaries (GeoRepo API)
- `GEOREPO_USER_EMAIL`: Optional, required if using `GEOREPO_API_KEY`

**Note:** These API keys must be provided as environment variables in the SPCS job specification. Without them, the pipeline will fall back to alternative data sources (e.g., GADM instead of GeoRepo) or skip data fetching (e.g., schools/health centers).

### Optional (SPCS Mode)

- `SNOWFLAKE_HOST`: SPCS internal network host (optional, usually auto-provided by SPCS)
- `SNOWFLAKE_PORT`: SPCS internal network port (optional, usually auto-provided by SPCS)
- `SNOWFLAKE_INSECURE_MODE`: Set to `true` to disable SSL certificate validation (default: `false`, only use if experiencing certificate issues)

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
   EXTERNAL_ACCESS_INTEGRATIONS = (IMPACT_ANALYSIS_EGRESS_ACCESS_INTEGRATION)
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
          # SPCS internal network (optional, but recommended for SPCS)
          # SNOWFLAKE_HOST: <internal-host>
          # SNOWFLAKE_PORT: <internal-port>
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
   EXTERNAL_ACCESS_INTEGRATIONS = (IMPACT_ANALYSIS_EGRESS_ACCESS_INTEGRATION)
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
          # SPCS internal network (optional, but recommended for SPCS)
          # SNOWFLAKE_HOST: <internal-host>
          # SNOWFLAKE_PORT: <internal-port>
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

## Troubleshooting

### Image Build Fails

- Ensure the `--platform=linux/amd64` flag is used
- Check that all required files are present in the repository root

### Container Fails to Start

- Verify all required environment variables are set
- Check that the Snowflake stage exists and is accessible
- Ensure SPCS_RUN=true when running in SPCS

### Authentication Errors

- In SPCS mode, ensure `SPCS_RUN=true` is set
- The OAuth token is automatically provided by SPCS at `/snowflake/session/token`
- Verify the token file exists: check that `/snowflake/session/token` is accessible
- If using SPCS internal network, ensure `SNOWFLAKE_HOST` and `SNOWFLAKE_PORT` are set (if required by the SPCS setup)
- For local testing, use `SNOWFLAKE_USER` and `SNOWFLAKE_PASSWORD`
- If experiencing SSL/certificate errors, try setting `SNOWFLAKE_INSECURE_MODE=true` (use with caution)

### Missing Data

- Verify that `TC_TRACKS` and `TC_ENVELOPES_COMBINED` tables contain data
- Check that the specified countries have been initialized (run `--type initialize` first)