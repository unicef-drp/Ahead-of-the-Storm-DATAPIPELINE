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
    DATA_PIPELINE_DB = os.getenv('DATA_PIPELINE_DB', 'LOCAL')
    
    # Application Configuration
    RESULTS_DIR = os.getenv('RESULTS_DIR', 'project_results/climate/lacro_project')
    BBOX_FILE = os.getenv('BBOX_FILE', 'bbox.parquet')
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
        
        missing = [var for var in required_vars if not getattr(cls, var)]
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
        
        missing = [var for var in required_vars if not getattr(cls, var)]
        if missing:
            raise ValueError(f"Missing Snowflake storage environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_azure_config(cls):
        """Validate that all required Azure configuration is present"""
        if cls.DATA_PIPELINE_DB == 'BLOB':
            required_vars = ['ACCOUNT_URL', 'SAS_TOKEN']
            missing = [var for var in required_vars if not getattr(cls, var)]
            if missing:
                raise ValueError(f"Missing Azure environment variables: {', '.join(missing)}")
    
    @classmethod
    def validate_storage_config(cls):
        """Validate storage configuration based on DATA_PIPELINE_DB setting"""
        if cls.DATA_PIPELINE_DB == 'BLOB':
            cls.validate_azure_config()
        elif cls.DATA_PIPELINE_DB == 'SNOWFLAKE':
            cls.validate_snowflake_storage_config()

# Create a global config instance
config = Config()
