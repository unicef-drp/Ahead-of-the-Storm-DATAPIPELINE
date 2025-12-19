#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Impact Analysis Pipeline.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

The module supports three storage backends:
- LOCAL: Local filesystem storage (default)
- BLOB: Azure Blob Storage (requires ACCOUNT_URL and SAS_TOKEN)
- SNOWFLAKE: Snowflake internal stage (requires SNOWFLAKE_STAGE_NAME)

Storage backend is configured via the DATA_PIPELINE_DB environment variable.

Key Components:
- Centralized data store initialization based on environment variables
- Automatic validation of required configuration for each storage backend
- Consistent data store configuration across the application

Usage:
    from data_store_utils import get_data_store
    data_store = get_data_store()  # Returns LocalDataStore, ADLSDataStore, or SnowflakeDataStore
"""

# Import GigaSpatial components
from gigaspatial.core.io.adls_data_store import ADLSDataStore
from gigaspatial.core.io.local_data_store import LocalDataStore
from gigaspatial.core.io.snowflake_data_store import SnowflakeDataStore

# Import centralized configuration
from config import config as app_config
import os

def get_data_store():
    """
    Get the appropriate data store based on centralized configuration
    
    Returns:
        DataStore: Configured data store instance
    """
    data_pipeline_db = app_config.DATA_PIPELINE_DB
    
    if data_pipeline_db == 'BLOB':
        app_config.validate_azure_config()
        return ADLSDataStore()
    elif data_pipeline_db == 'SNOWFLAKE':
        app_config.validate_snowflake_storage_config()
        
        # Check if running in SPCS mode
        spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
        
        # In SPCS mode, user/password are not required (OAuth handles authentication)
        # But SnowflakeDataStore still needs them for initialization
        # Pass empty strings and let the connection use SPCS OAuth via snowflake_utils
        if spcs_run:
            # For SPCS mode, use a connection that supports OAuth
            # SnowflakeDataStore will need to be updated to support SPCS, but for now
            # pass None and handle it in the connection creation
            return SnowflakeDataStore(
                account=app_config.SNOWFLAKE_ACCOUNT,
                user=None,  # Not needed in SPCS mode
                password=None,  # Not needed in SPCS mode
                warehouse=app_config.SNOWFLAKE_WAREHOUSE,
                database=app_config.SNOWFLAKE_DATABASE,
                schema=app_config.SNOWFLAKE_SCHEMA,
                stage_name=app_config.SNOWFLAKE_STAGE_NAME
            )
        else:
            # Non-SPCS mode: use password authentication
            return SnowflakeDataStore(
                account=app_config.SNOWFLAKE_ACCOUNT,
                user=app_config.SNOWFLAKE_USER,
                password=app_config.SNOWFLAKE_PASSWORD,
                warehouse=app_config.SNOWFLAKE_WAREHOUSE,
                database=app_config.SNOWFLAKE_DATABASE,
                schema=app_config.SNOWFLAKE_SCHEMA,
                stage_name=app_config.SNOWFLAKE_STAGE_NAME
            )
    elif data_pipeline_db == 'LOCAL':
        return LocalDataStore()
    else:
        # Default to local storage
        return LocalDataStore()
