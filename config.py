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
    DATA_PIPELINE_DB = os.getenv('DATA_PIPELINE_DB', 'LOCAL').strip().upper()

    # Hazard source data (wind/gust envelopes, tracks, precip/runoff) read mode.
    # Independent of DATA_PIPELINE_DB, which only governs this repo's OWN
    # output/cache. SNOWFLAKE (default) reads TC_ENVELOPES_COMBINED/
    # TC_GUST_ENVELOPES_COMBINED/TC_TRACKS/MET_FORECASTS directly, same as
    # today. LOCAL/BLOB read the same data from TC-ECMWF-Forecast-Pipeline's
    # own local-disk or Blob output instead, mainly for local end-to-end
    # testing without a live Snowflake connection populated with real
    # hurricane data.
    HAZARD_DATA_SOURCE = os.getenv('HAZARD_DATA_SOURCE', 'SNOWFLAKE').strip().upper()

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
    
    # Report Configuration (optional)
    REPORTS_JSON_DIR = os.getenv('REPORTS_JSON_DIR', 'jsons')  # Subdirectory for JSON reports under RESULTS_DIR
    REPORT_TEMPLATE_PATH = os.getenv('REPORT_TEMPLATE_PATH', 'impact-report-template.html')  # HTML template path
    
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
            required_vars = ['ACCOUNT_URL', 'SAS_TOKEN']
            missing = [var for var in required_vars if _is_missing(getattr(cls, var))]
            if missing:
                raise ValueError(f"Missing Azure environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_storage_config(cls):
        """Validate storage configuration based on DATA_PIPELINE_DB setting"""
        if cls.DATA_PIPELINE_DB == 'BLOB':
            cls.validate_azure_config()
        elif cls.DATA_PIPELINE_DB == 'SNOWFLAKE':
            cls.validate_snowflake_storage_config()
        elif cls.DATA_PIPELINE_DB != 'LOCAL':
            raise ValueError(
                f"Unrecognized DATA_PIPELINE_DB value: '{cls.DATA_PIPELINE_DB}' "
                f"(expected LOCAL, BLOB, or SNOWFLAKE)"
            )

    @classmethod
    def validate_hazard_data_source_config(cls):
        """Validate configuration for HAZARD_DATA_SOURCE (upstream hazard
        source data read mode), independent of validate_storage_config(),
        which is about this repo's own output/cache instead. Only ever
        called for LOCAL/BLOB (via get_hazard_data_store()); the SNOWFLAKE
        case needs no extra validation beyond the existing Snowflake
        credential checks already performed elsewhere."""
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
                f"Unrecognized HAZARD_DATA_SOURCE value: '{cls.HAZARD_DATA_SOURCE}' "
                f"(expected SNOWFLAKE, LOCAL, or BLOB)"
            )

# Create a global config instance
config = Config()
