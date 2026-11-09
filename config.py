#!/usr/bin/env python3
"""
Configuration Module

This module provides centralized configuration management.
It handles environment variable loading and provides a single source of truth for configuration.

Key Components:
- Centralized environment variable loading
- Configuration validation
- Default value management
- Environment-specific settings

Usage:
    from config import config
    snowflake_account = config.SNOWFLAKE_ACCOUNT
"""

import os
from dotenv import load_dotenv

# Load environment variables from the project root
# This assumes the .env file is in the project root directory
load_dotenv()


def _is_missing(value) -> bool:
    """
    True for None, empty string, or whitespace-only string.
    """
    return value is None or not str(value).strip()


class Config:
    """Centralized configuration class"""
    
    # Snowflake Configuration
    SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
    SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
    SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
    SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
    SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
    SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')
    SNOWFLAKE_STAGE_NAME = os.getenv('SNOWFLAKE_STAGE_NAME')
    
    # Azure Blob Storage Configuration
    ACCOUNT_URL = os.getenv('ACCOUNT_URL')
    SAS_TOKEN = os.getenv('SAS_TOKEN')
    # CONTAINER_NAME must be set explicitly and passed to ADLSDataStore rather than
    # left to its own container=config.ADLS_CONTAINER_NAME default: that default reads
    # the generic ADLS_CONTAINER_NAME env var, the same one any other bare
    # ADLSDataStore() construction elsewhere in this repo relies on for an
    # unrelated shared storage account. Leaving
    # this repo's own output store to that same default risks silently writing to
    # whatever container ADLS_CONTAINER_NAME happens to point at for that other purpose.
    CONTAINER_NAME = os.getenv('CONTAINER_NAME')
    DATA_PIPELINE_DB = os.getenv('DATA_PIPELINE_DB', 'LOCAL').strip().upper()

    # Hazard source data (wind/gust envelopes, tracks, precip/runoff) read mode.
    # Independent of DATA_PIPELINE_DB, which only governs this repo's OWN
    # output/cache. BLOB (default) reads the data from the Azure Blob
    # container written by TC-ECMWF-Forecast-Pipeline (HAZARD_BLOB_* vars
    # required). LOCAL reads TC-ECMWF-Forecast-Pipeline's local-disk output,
    # mainly for local end-to-end runs. SNOWFLAKE reads
    # TC_ENVELOPES_COMBINED/TC_GUST_ENVELOPES_COMBINED/TC_TRACKS/MET_FORECASTS
    # directly.
    HAZARD_DATA_SOURCE = os.getenv('HAZARD_DATA_SOURCE', 'BLOB').strip().upper()

    # HAZARD_DATA_SOURCE=LOCAL: directories matching TC-ECMWF-Forecast-Pipeline's
    # own output locations (WIND_EXTRACTED_DIR/TRANSFORMED_DATA_DIR/MET_DATA_DIR
    # in that repo, all flat, one file per cycle). No defaults here: these are
    # arbitrary local paths on whatever machine is running this repo, there is
    # no single install location to guess, so an unset one must fail loudly
    # rather than silently pointing at the wrong directory.
    HAZARD_LOCAL_WIND_DIR = os.getenv('HAZARD_LOCAL_WIND_DIR')
    HAZARD_LOCAL_TRACKS_DIR = os.getenv('HAZARD_LOCAL_TRACKS_DIR')
    HAZARD_LOCAL_MET_DIR = os.getenv('HAZARD_LOCAL_MET_DIR')

    # HAZARD_DATA_SOURCE=BLOB: deliberately separate from this app's own
    # ACCOUNT_URL/SAS_TOKEN (which configure THIS app's own output storage);
    # the upstream repo's hazard data typically lives in a different storage
    # account/container entirely.
    HAZARD_BLOB_ACCOUNT_URL = os.getenv('HAZARD_BLOB_ACCOUNT_URL')
    HAZARD_BLOB_SAS_TOKEN = os.getenv('HAZARD_BLOB_SAS_TOKEN')
    HAZARD_BLOB_CONTAINER = os.getenv('HAZARD_BLOB_CONTAINER')

    # Application Configuration
    RESULTS_DIR = os.getenv('RESULTS_DIR', 'results')
    STORMS_FILE = os.getenv('STORMS_FILE', 'storms.json')
    VIEWS_DIR = os.getenv('VIEWS_DIR', 'aos_views')
    ROOT_DATA_DIR = os.getenv('ROOT_DATA_DIR', 'geodb')

    @classmethod
    def validate_snowflake_config(cls):
        """Validate that all required Snowflake configuration is present"""
        required_vars = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_USER', 
            'SNOWFLAKE_PASSWORD',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA'
        ]
        
        missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
        if missing:
            raise ValueError(f"Missing Snowflake environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_snowflake_storage_config(cls):
        """Validate that all required Snowflake storage configuration is present (for DATA_PIPELINE_DB=SNOWFLAKE)"""
        import os
        # Check if running in SPCS mode
        spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
        
        # Base required variables (always needed)
        required_vars = [
            'SNOWFLAKE_ACCOUNT',
            'SNOWFLAKE_WAREHOUSE',
            'SNOWFLAKE_DATABASE',
            'SNOWFLAKE_SCHEMA',
            'SNOWFLAKE_STAGE_NAME'
        ]
        
        # User/password only required in non-SPCS mode
        if not spcs_run:
            required_vars.extend(['SNOWFLAKE_USER', 'SNOWFLAKE_PASSWORD'])
        
        missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
        if missing:
            raise ValueError(f"Missing Snowflake storage environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_azure_config(cls):
        """Validate that all required Azure configuration is present"""
        if cls.DATA_PIPELINE_DB == 'BLOB':
            required_vars = ['ACCOUNT_URL', 'SAS_TOKEN', 'CONTAINER_NAME']
            missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
            if missing:
                raise ValueError(f"Missing Azure environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_hazard_data_source_config(cls):
        """Validate configuration for HAZARD_DATA_SOURCE LOCAL or BLOB (upstream
        hazard source data read mode), independent of DATA_PIPELINE_DB, which is
        about this repo's own output/cache instead. HAZARD_DATA_SOURCE=SNOWFLAKE
        reads through direct queries and has no data store to validate."""
        if cls.HAZARD_DATA_SOURCE == 'LOCAL':
            required_vars = ['HAZARD_LOCAL_WIND_DIR', 'HAZARD_LOCAL_TRACKS_DIR', 'HAZARD_LOCAL_MET_DIR']
            missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
            if missing:
                raise ValueError(f"Missing HAZARD_DATA_SOURCE=LOCAL environment variables: {', '.join(missing)}")
        elif cls.HAZARD_DATA_SOURCE == 'BLOB':
            required_vars = ['HAZARD_BLOB_ACCOUNT_URL', 'HAZARD_BLOB_SAS_TOKEN', 'HAZARD_BLOB_CONTAINER']
            missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
            if missing:
                raise ValueError(f"Missing HAZARD_DATA_SOURCE=BLOB environment variables: {', '.join(missing)}")
        else:
            raise ValueError(
                f"HAZARD_DATA_SOURCE '{cls.HAZARD_DATA_SOURCE}' has no data store "
                f"(expected LOCAL or BLOB)"
            )

# Create a global config instance
config = Config()
