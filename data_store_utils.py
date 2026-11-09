#!/usr/bin/env python3
"""
Data Store Utilities Module

This module provides centralized data store management utilities for the Impact Analysis Pipeline.
It consolidates data store initialization logic and provides a single source of truth for data store configuration.

The module supports three storage backends:
- LOCAL: Local filesystem storage (default)
- BLOB: Azure Blob Storage (requires ACCOUNT_URL, SAS_TOKEN, and CONTAINER_NAME)
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
import threading
import queue
import logging

logger = logging.getLogger(__name__)


# A raw OS-level socket write()/read() (Azure Blob upload/download) can hang
# indefinitely on a dead/black-holed connection, past azure-storage-blob's own
# connect/read timeouts (those don't cover a stuck syscall waiting on kernel-
# level TCP retransmission). gigaspatial's ADLSDataStore has no protection
# against this, so it's added here: an external watchdog runs the real call in
# a daemon worker thread and raises TimeoutError if it doesn't return within
# _BLOB_OP_TIMEOUT_SECONDS. Python can't kill a thread stuck in a blocking
# syscall, so the worker is abandoned rather than joined - acceptable since the
# pipeline's existing per-country/per-operation try/except already handles the
# raised exception, which beats hanging the whole process. daemon=True (not
# ThreadPoolExecutor, whose workers are non-daemon) so an abandoned worker
# never blocks process exit. Applied by monkeypatching the class, so every
# ADLSDataStore instance is covered regardless of which factory constructed it.
_BLOB_OP_TIMEOUT_SECONDS = 120  # well above typical operation time, well below a stuck-connection hang


def _with_hard_timeout(func, timeout=_BLOB_OP_TIMEOUT_SECONDS):
    # `timeout` may be a number, or a callable taking the wrapped call's own
    # (*args, **kwargs) and returning seconds (used to scale with payload size).
    def wrapper(*args, **kwargs):
        result_q = queue.Queue(maxsize=1)
        timeout_s = timeout(*args, **kwargs) if callable(timeout) else timeout

        def _run():
            try:
                result_q.put(('ok', func(*args, **kwargs)))
            except Exception as e:
                result_q.put(('error', e))

        t = threading.Thread(target=_run, daemon=True)
        t.start()
        try:
            status, value = result_q.get(timeout=timeout_s)
        except queue.Empty:
            logger.error(
                f"{func.__qualname__} exceeded {timeout_s}s, likely a stuck Azure Blob "
                f"connection (see this module's own header comment), aborting this "
                f"operation rather than hanging indefinitely"
            )
            raise TimeoutError(f"{func.__qualname__} exceeded {timeout_s}s (stuck Blob connection)")
        if status == 'error':
            raise value
        return value
    return wrapper


# Five ADLSDataStore methods carry the same stuck-connection risk: list_files,
# file_exists, remove, exists (used by the stale-file retry path in
# create_views_from_envelopes_in_country()), and is_dir (called by gigaspatial's
# WorldPopConfig.get_data_unit_paths() under DATA_PIPELINE_DB=BLOB; safe to wrap
# directly since it returns a plain bool after fully consuming its own generator).
# list_files_iter() is deliberately left unwrapped - it's lazy, so wrapping its
# creation wouldn't guard the real I/O that happens later, item by item, during
# iteration.
#
# read_file and write_file are wrapped separately below, around faster replacement
# implementations of the gigaspatial methods.
for _method_name in ('list_files', 'file_exists', 'remove', 'exists', 'is_dir'):
    setattr(ADLSDataStore, _method_name, _with_hard_timeout(getattr(ADLSDataStore, _method_name)))


# gigaspatial's ADLSDataStore.read_file() downloads with no max_concurrency
# (blob_client.download_blob().readall()), so every download is single-connection,
# sequential chunk-by-chunk. Fine for small files, but MET_FORECASTS tp/ro Zarr files are
# genuinely ~1.3GB/~266MB (see precip_utils.py's own docstring), and this sequential path
# can exceed _BLOB_OP_TIMEOUT_SECONDS (120s) for the 1.3GB file, which the hard-timeout
# wrapper then misclassifies as "stuck", while the same blob downloaded with
# max_concurrency=4 completes in about 63s. This mirrors
# _write_file_with_concurrency's own already-established fix for the identical class of
# gigaspatial gap on the upload side.
_READ_MAX_CONCURRENCY = 4


def _read_file_with_concurrency(self, path, encoding=None):
    try:
        blob_key = self._to_blob_key(path)
        blob_client = self.container_client.get_blob_client(blob_key)
        blob_data = blob_client.download_blob(max_concurrency=_READ_MAX_CONCURRENCY).readall()
        if encoding is None:
            return blob_data
        return blob_data.decode(encoding)
    except Exception as e:
        raise IOError(f"Error reading file {path}: {e}")


ADLSDataStore.read_file = _with_hard_timeout(_read_file_with_concurrency)


# gigaspatial's ADLSDataStore.write_file() uploads with no max_concurrency, so every
# upload is single-connection and non-chunked - and azure-storage-blob only chunks
# payloads above max_single_put_size (64MB default), so WorldPop .tif files (~25MB)
# would never trigger it even if max_concurrency were added alone. There's no public
# way to lower max_single_put_size on ADLSDataStore (no constructor param, no per-call
# upload_blob() kwarg), so this replacement sets it directly on the shared BlobClient's
# _config once per call (a private-by-convention attribute, but the only reachable one;
# idempotent, so concurrent write_file calls racing on it is harmless) before uploading
# with max_concurrency. 4MB matches azure-storage-blob's own default block size
# (guarantees chunking for any file above a few MB); concurrency
# 4 matches TC-ECMWF-Forecast-Pipeline's own upload_to_blob() and gives real parallelism
# for a ~25-70MB/4MB-block file without over-provisioning idle workers. _BLOB_OP_TIMEOUT_
# SECONDS stays the floor for writes, but the write watchdog scales with payload size
# (_write_timeout below): WorldPop age/sex .tif files are ~70MB and gigaspatial uploads
# many of them in parallel over one uplink, so a fixed 120s would cut off slow-but-healthy
# uploads and misclassify them as stuck connections.
#
# Side effect: a parallel upload drops conditional-access kwargs
# (etag/if-match), irrelevant today since this call always passes overwrite=True with no
# conditional kwargs. A mid-upload chunk failure still leaves the target blob untouched
# either way, since commit_block_list() only runs after every chunk succeeds. This is a
# local override of gigaspatial's write_file(), not a change to the installed library;
# it also mutates the shared config for any future upload_file()/upload_directory() call
# on the same instance (unused in this repo) - same correctness, no parallelism gain,
# since those don't pass max_concurrency.
_WRITE_MAX_CONCURRENCY = 4
_WRITE_MAX_SINGLE_PUT_SIZE = 4 * 1024 * 1024  # 4MB, matches azure-storage-blob's own max_block_size default


def _write_file_with_concurrency(self, path, data) -> None:
    blob_key = self._to_blob_key(path)
    blob_client = self.blob_service_client.get_blob_client(
        container=self.container, blob=blob_key, snapshot=None
    )
    # Required for max_concurrency below to have any effect at this payload size -
    # see this block's own header comment above _WRITE_MAX_CONCURRENCY.
    blob_client._config.max_single_put_size = _WRITE_MAX_SINGLE_PUT_SIZE

    if isinstance(data, str):
        binary_data = data.encode()
    elif isinstance(data, bytes):
        binary_data = data
    else:
        raise ValueError(f'Unsupported data type. Only "bytes" or "string" accepted')

    blob_client.upload_blob(binary_data, overwrite=True, max_concurrency=_WRITE_MAX_CONCURRENCY)


_WRITE_TIMEOUT_SECONDS_PER_MB = 5  # ~0.2MB/s floor per upload, tolerates many concurrent uploads sharing one uplink


def _write_timeout(self, path, data) -> float:
    return max(_BLOB_OP_TIMEOUT_SECONDS, len(data) / (1024 * 1024) * _WRITE_TIMEOUT_SECONDS_PER_MB)


ADLSDataStore.write_file = _with_hard_timeout(_write_file_with_concurrency, timeout=_write_timeout)


def get_snowflake_auth_kwargs() -> dict:
    """
    Returns {'user':..., 'password':...} for SnowflakeDataStore construction:
    both None under SPCS_RUN=true (OAuth handles identity, passing real
    credentials is unnecessary and the values may not even be set), else the
    configured username/password.

    Single source of truth for the SPCS_RUN branch, shared by every module
    that constructs its own SnowflakeDataStore directly: this module's own
    SNOWFLAKE case below, glofas_utils.get_river_forecasts_data_store(),
    and precip_utils.get_met_forecasts_data_store().
    """
    spcs_run = os.getenv('SPCS_RUN', 'false').lower() == 'true'
    if spcs_run:
        return {'user': None, 'password': None}
    return {'user': app_config.SNOWFLAKE_USER, 'password': app_config.SNOWFLAKE_PASSWORD}


def get_data_store():
    """
    Get the appropriate data store based on centralized configuration

    Returns:
        DataStore: Configured data store instance
    """
    data_pipeline_db = app_config.DATA_PIPELINE_DB

    if data_pipeline_db == 'BLOB':
        app_config.validate_azure_config()
        return ADLSDataStore(
            container=app_config.CONTAINER_NAME,
            account_url=app_config.ACCOUNT_URL,
            sas_token=app_config.SAS_TOKEN,
        )
    elif data_pipeline_db == 'SNOWFLAKE':
        app_config.validate_snowflake_storage_config()
        return SnowflakeDataStore(
            account=app_config.SNOWFLAKE_ACCOUNT,
            warehouse=app_config.SNOWFLAKE_WAREHOUSE,
            database=app_config.SNOWFLAKE_DATABASE,
            schema=app_config.SNOWFLAKE_SCHEMA,
            stage_name=app_config.SNOWFLAKE_STAGE_NAME,
            **get_snowflake_auth_kwargs(),
        )
    elif data_pipeline_db == 'LOCAL':
        return LocalDataStore()
    else:
        # A mistyped DATA_PIPELINE_DB (e.g. 'SNOWFALKE') must fail loudly here
        raise ValueError(
            f"Unrecognized DATA_PIPELINE_DB value: '{data_pipeline_db}' "
            f"(expected LOCAL, BLOB, or SNOWFLAKE)"
        )


def get_hazard_data_store(base_path: str = ''):
    """
    Data store for reading upstream hazard source data (wind/gust envelopes,
    tracks, precip/runoff), governed by HAZARD_DATA_SOURCE, independent of
    DATA_PIPELINE_DB, which only governs this repo's own output/cache.

    Only called when HAZARD_DATA_SOURCE is LOCAL or BLOB (the SNOWFLAKE case
    keeps using the existing direct-query functions in snowflake_utils.py/
    precip_utils.py unchanged, no data store needed).

    Args:
        base_path: for LOCAL only, the specific directory this call needs
            (wind, tracks, or met; they can differ), passed to LocalDataStore
            so callers can use paths relative to that directory.

    Returns:
        DataStore: LocalDataStore or ADLSDataStore instance.
    """
    app_config.validate_hazard_data_source_config()
    hazard_data_source = app_config.HAZARD_DATA_SOURCE

    if hazard_data_source == 'LOCAL':
        return LocalDataStore(base_path=base_path)
    elif hazard_data_source == 'BLOB':
        return ADLSDataStore(
            container=app_config.HAZARD_BLOB_CONTAINER,
            account_url=app_config.HAZARD_BLOB_ACCOUNT_URL,
            sas_token=app_config.HAZARD_BLOB_SAS_TOKEN,
        )
    else:
        raise ValueError(
            f"get_hazard_data_store() should only be called for HAZARD_DATA_SOURCE "
            f"LOCAL or BLOB, got '{hazard_data_source}'"
        )
