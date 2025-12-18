# ==============================================================================
# License
# ==============================================================================
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# UNSUPPORTED BY SNOWFLAKE - CUSTOMER SUPPORTED ONLY
#
# ==============================================================================
# Dockerfile for Impact Analysis Pipeline (SPCS Deployment)
# ==============================================================================
# This container runs the hurricane impact analysis pipeline including:
# - Reading hurricane track and envelope data from Snowflake tables
# - Processing geospatial impact analysis (schools, health centers, population)
# - Generating impact views and reports
# - Writing results to Snowflake internal stage
#
# The container will execute main_pipeline.py and terminate when complete.
#
# Build command (must specify platform for SPCS):
#   docker build -f snowflake/Dockerfile -t impact-analysis-pipeline:latest . --platform=linux/amd64
# ==============================================================================

FROM python:3.11-slim

LABEL maintainer="Ahead of the Storm Pipeline"
LABEL description="Hurricane Impact Analysis Pipeline with Snowflake Integration (SPCS)"
LABEL version="1.0"

# ==============================================================================
# Install system dependencies
# ==============================================================================

# Install system packages required for:
# - geospatial: Shapely, geopandas, GDAL, PROJ, GEOS
RUN apt-get update && apt-get install -y --no-install-recommends \
    # Geospatial libraries
    libgeos-dev \
    libproj-dev \
    libgdal-dev \
    gdal-bin \
    # Compilation tools (required for building some Python packages)
    build-essential \
    gcc \
    g++ \
    # Utilities
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

# ==============================================================================
# Set environment variables
# ==============================================================================

# Python environment
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

# Set working directory
WORKDIR /app

# ==============================================================================
# Copy application code
# ==============================================================================

# Copy core pipeline modules
COPY main_pipeline.py /app/
COPY impact_analysis.py /app/
COPY config.py /app/
COPY data_store_utils.py /app/
COPY snowflake_utils.py /app/
COPY reports.py /app/
COPY country_utils.py /app/

# Copy gigaspatial library (local copy with multiprocessing fixes)
COPY gigaspatial/ /app/gigaspatial/

# Copy requirements
COPY requirements.txt /app/requirements.txt

# ==============================================================================
# Install Python dependencies
# ==============================================================================

# Upgrade pip
RUN pip install --no-cache-dir --upgrade pip setuptools wheel

# Install Python packages
RUN pip install --no-cache-dir -r requirements.txt

# ==============================================================================
# Create directories for data (if using local storage as fallback)
# ==============================================================================

RUN mkdir -p /app/geodb/aos_views/mercator_views \
    /app/geodb/aos_views/admin_views \
    /app/geodb/aos_views/track_views \
    /app/geodb/aos_views/school_views \
    /app/geodb/aos_views/hc_views \
    /app/results/jsons

# ==============================================================================
# Copy startup script
# ==============================================================================

# Copy startup script
COPY snowflake/start.sh /app/start.sh
RUN chmod +x /app/start.sh

# ==============================================================================
# Set default environment variables (can be overridden at runtime)
# ==============================================================================

# Pipeline storage configuration
# Note: DATA_PIPELINE_DB should be set at runtime (LOCAL, BLOB, or SNOWFLAKE)
# Default to LOCAL for testing/imports, but should be SNOWFLAKE for SPCS deployment
ENV DATA_PIPELINE_DB=LOCAL

# Pipeline output directories
ENV ROOT_DATA_DIR=geodb
ENV VIEWS_DIR=aos_views
ENV RESULTS_DIR=results
ENV BBOX_FILE=bbox.parquet
ENV STORMS_FILE=storms.json

# Snowflake configuration
# NOTE: These MUST be set at runtime via environment variables or SPCS job configuration
# Do not set defaults here - they will be provided when the container runs
# Required: SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD (or SPCS_RUN=true)
# Required: SNOWFLAKE_WAREHOUSE, SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA
# Required if DATA_PIPELINE_DB=SNOWFLAKE: SNOWFLAKE_STAGE_NAME
# 
# SPCS OAuth: When running in SPCS, set SPCS_RUN=true at runtime
# The code will automatically detect SPCS_RUN and use OAuth token from /snowflake/session/token

# Pipeline defaults (can be overridden at runtime)
ENV ZOOM_LEVEL=14
ENV REWRITE=0

# ==============================================================================
# Entrypoint
# ==============================================================================

# Run the impact analysis pipeline
# Arguments will be passed at runtime (e.g., --type initialize --countries TWN)
ENTRYPOINT ["/app/start.sh"]

# Default command (can be overridden)
CMD ["--help"]
