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
