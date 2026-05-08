#!/usr/bin/env python3
"""
Dataverse Audit Sync - Unified Deployment
==========================================

ONE codebase, MULTIPLE deployment options:
  - Console job:    python main.py
  - Docker:         docker run audit-sync
  - Azure Function: triggered via function_app.py wrapper
  - Background job: any orchestrator (k8s, systemd, etc.)

AUTO-ADAPTIVE BEHAVIOR:
  - Detects if it needs to catch up on backlog (large windows, fast)
  - Switches to live mode when current (small windows, gentle)
  - Same code, smart behavior based on lastSyncEnd state

Configuration: All behavior controlled via config.json
"""

import os
import sys
import json
import logging
import logging.handlers
import asyncio
import aiohttp
import uuid
import signal
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import snowflake.connector
from snowflake.connector import DictCursor
import msal


# ============================================================================
# Configuration Loading
# ============================================================================

def load_config() -> Dict:
    """Load configuration from config.json"""
    config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.json')
    
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: config.json not found at {config_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in config.json: {e}", file=sys.stderr)
        sys.exit(1)


def setup_logging(config: Dict) -> logging.Logger:
    """Configure logging based on config.json"""
    log_config = config.get("logging", {})
    level_str = log_config.get("level", "INFO")
    log_level = getattr(logging, level_str, logging.INFO)
    
    log_format = log_config.get("format", "[%(asctime)s] %(levelname)s [%(name)s]: %(message)s")
    date_format = log_config.get("date_format", "%Y-%m-%d %H:%M:%S")
    
    # Clear existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Console handler
    output_config = log_config.get("output", {})
    if output_config.get("console", True):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        console_handler.setLevel(log_level)
        logging.root.addHandler(console_handler)
    
    # File handler with rotation
    file_config = output_config.get("file", {})
    if file_config.get("enabled", False):
        log_file = file_config.get("path", "./logs/audit-sync.log")
        max_size = file_config.get("max_size_mb", 100) * 1024 * 1024
        backup_count = file_config.get("backup_count", 5)
        
        log_dir = os.path.dirname(log_file)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file, maxBytes=max_size, backupCount=backup_count
        )
        file_level = getattr(logging, file_config.get("level", "DEBUG"), logging.DEBUG)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        logging.root.addHandler(file_handler)
    
    logging.root.setLevel(log_level)
    
    # Per-component log levels
    components = log_config.get("components", {})
    for component, comp_level in components.items():
        comp_level_int = getattr(logging, comp_level, logging.INFO)
        logging.getLogger(component).setLevel(comp_level_int)
    
    return logging.getLogger("sync")


# ============================================================================
# Globals (initialized in main)
# ============================================================================

CONFIG = load_config()
logger = setup_logging(CONFIG)

# Environment variables (secrets only)
DATAVERSE_ORG_URL = os.getenv("DATAVERSE_ORG_URL", "https://yourorg.crm.dynamics.com")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE", "AUDIT_DB")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")

# Optional: filter to single entity (e.g., for parallel containers)
ENTITY_FILTER = os.getenv("ENTITY")

# Override start time for initial backlog (e.g., "2026-01-01T00:00:00")
OVERRIDE_START_TIME = os.getenv("OVERRIDE_START_TIME")

# Shutdown flag for graceful exit
_shutdown_requested = False


def _handle_shutdown(signum, frame):
    """Graceful shutdown handler for SIGTERM/SIGINT"""
    global _shutdown_requested
    logger.info(f"Received signal {signum}, requesting graceful shutdown...")
    _shutdown_requested = True


# ============================================================================
# Adaptive Window Sizing
# ============================================================================

def determine_window_size(last_sync_end: datetime) -> int:
    """
    Adaptive window sizing based on how far behind we are.
    
    - If lastSyncEnd is older than threshold (e.g., 1 hour), use BACKLOG window (60 min)
    - Otherwise, use CONTINUOUS window (10 min)
    
    This makes the same code:
    - Aggressively catch up when far behind (backlog mode)
    - Gently process new data when current (live mode)
    """
    auto_detect = CONFIG.get("modeAutoDetect", {})
    
    if not auto_detect.get("enabled", True):
        # Auto-detect disabled, use BACKLOG_MODE env var
        backlog_mode = os.getenv("BACKLOG_MODE", "false").lower() == "true"
        return CONFIG["windowSizeMinutes"]["backlog" if backlog_mode else "continuous"]
    
    threshold_minutes = auto_detect.get("backlog_threshold_minutes", 60)
    lag_minutes = (datetime.utcnow() - last_sync_end).total_seconds() / 60
    
    if lag_minutes > threshold_minutes:
        return CONFIG["windowSizeMinutes"]["backlog"]
    else:
        return CONFIG["windowSizeMinutes"]["continuous"]


# ============================================================================
# Authentication
# ============================================================================

async def get_dataverse_token() -> str:
    """Get OAuth 2.0 token from Microsoft Entra ID"""
    auth_logger = logging.getLogger("auth")
    dv_auth = CONFIG.get("dataverse", {}).get("auth", {})
    authority_url = dv_auth.get("authority_url", "https://login.microsoftonline.com/common")
    scope = dv_auth.get("scope", "https://org.dynamics.com/.default")
    
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=authority_url
    )
    
    token_response = app.acquire_token_by_username_password(
        username=CLIENT_ID,
        password=CLIENT_SECRET,
        scopes=[scope]
    )
    
    if "access_token" not in token_response:
        raise Exception(f"Failed to acquire token: {token_response.get('error_description')}")
    
    auth_logger.debug("OAuth token acquired")
    return token_response["access_token"]


# ============================================================================
# Snowflake Operations
# ============================================================================

def get_snowflake_connection():
    """Create Snowflake connection"""
    sf_config = CONFIG.get("snowflake", {}).get("connection", {})
    timeout = sf_config.get("timeout_seconds", 30)
    
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        login_timeout=timeout
    )


def get_sync_state(connection, entity: str) -> datetime:
    """Get last sync time from Snowflake state table"""
    sf_logger = logging.getLogger("snowflake")
    
    try:
        cursor = connection.cursor(DictCursor)
        cursor.execute(
            "SELECT last_sync_end FROM sync_state WHERE entity = %s",
            (entity,)
        )
        row = cursor.fetchone()
        if row and row.get("LAST_SYNC_END"):
            sync_end = row["LAST_SYNC_END"]
            if isinstance(sync_end, str):
                return datetime.fromisoformat(sync_end)
            return sync_end
    except Exception as e:
        sf_logger.warning(f"Could not fetch state for {entity}: {e}")
    
    # No prior state - check for OVERRIDE_START_TIME or default to 1 hour ago
    if OVERRIDE_START_TIME:
        try:
            return datetime.fromisoformat(OVERRIDE_START_TIME)
        except ValueError:
            sf_logger.warning(f"Invalid OVERRIDE_START_TIME: {OVERRIDE_START_TIME}")
    
    # Default: 1 hour ago (small initial window if no override)
    return datetime.utcnow() - timedelta(hours=1)


def update_sync_state(connection, entity: str, last_sync_end: datetime, record_count: int):
    """Atomically update last sync time in Snowflake"""
    cursor = connection.cursor()
    cursor.execute(
        """MERGE INTO sync_state target
           USING (SELECT %s AS entity, %s AS last_sync_end, %s AS record_count, %s AS updated_at) source
           ON target.entity = source.entity
           WHEN MATCHED THEN UPDATE SET
             last_sync_end = source.last_sync_end,
             record_count = target.record_count + source.record_count,
             updated_at = source.updated_at
           WHEN NOT MATCHED THEN INSERT (entity, last_sync_end, record_count, updated_at)
             VALUES (source.entity, source.last_sync_end, source.record_count, source.updated_at)
        """,
        (entity, last_sync_end.isoformat(), record_count, datetime.utcnow().isoformat())
    )
    connection.commit()


# ============================================================================
# Dataverse API Operations
# ============================================================================

async def fetch_audits(
    session: aiohttp.ClientSession,
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str
) -> List[str]:
    """Query Dataverse audits via Web API with pagination support"""
    dv_logger = logging.getLogger("dataverse")
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    timeout = dv_api.get("timeout_seconds", 30)
    page_size = CONFIG.get("dataverse", {}).get("query", {}).get("page_size", 5000)
    
    audits = []
    filter_query = (
        f"createdon ge {window_start.isoformat()}Z "
        f"and createdon lt {window_end.isoformat()}Z"
    )
    url = (
        f"{DATAVERSE_ORG_URL}/api/data/{api_version}/audits"
        f"?$filter={filter_query}&$select=auditid&$top={page_size}"
    )
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Prefer": f"odata.maxpagesize={page_size}"
    }
    
    try:
        # Handle pagination via @odata.nextLink
        next_url = url
        while next_url:
            async with session.get(
                next_url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    for item in data.get("value", []):
                        audits.append(item["auditid"])
                    next_url = data.get("@odata.nextLink")
                else:
                    dv_logger.error(f"[{entity}] Audit query failed: HTTP {response.status}")
                    break
    except asyncio.TimeoutError:
        dv_logger.error(f"[{entity}] Timeout fetching audits after {timeout}s")
    except Exception as e:
        dv_logger.error(f"[{entity}] Error fetching audits: {e}")
    
    dv_logger.info(f"[{entity}] Fetched {len(audits)} audits for window {window_start} to {window_end}")
    return audits


async def fetch_audit_details_with_retry(
    session: aiohttp.ClientSession,
    token: str,
    audit_id: str,
    attributes: List[str]
) -> Optional[Dict]:
    """Fetch audit details via RetrieveAuditDetails action with exponential backoff"""
    dv_logger = logging.getLogger("dataverse")
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    timeout = dv_api.get("timeout_seconds", 30)
    max_retries = dv_api.get("max_retries", 3)
    retry_delay = dv_api.get("retry_delay_seconds", 1)
    perf_config = CONFIG.get("performance", {})
    backoff_multiplier = perf_config.get("backoff_multiplier", 2.0)
    max_backoff = perf_config.get("max_backoff_seconds", 30)
    
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "auditId": audit_id,
        "propertySet": attributes
    }
    
    url = f"{DATAVERSE_ORG_URL}/api/data/{api_version}/RetrieveAuditDetails"
    
    for attempt in range(1, max_retries + 1):
        try:
            async with session.post(
                url, json=payload, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("AuditRecord", {})
                elif attempt < max_retries:
                    delay = min(retry_delay * (backoff_multiplier ** (attempt - 1)), max_backoff)
                    dv_logger.warning(
                        f"[Retry {attempt}/{max_retries}] Audit {audit_id}: "
                        f"HTTP {response.status}, waiting {delay:.1f}s"
                    )
                    await asyncio.sleep(delay)
        except asyncio.TimeoutError:
            if attempt < max_retries:
                delay = min(retry_delay * (backoff_multiplier ** (attempt - 1)), max_backoff)
                dv_logger.warning(f"[Retry {attempt}/{max_retries}] Audit {audit_id}: Timeout, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
        except Exception as e:
            if attempt < max_retries:
                delay = min(retry_delay * (backoff_multiplier ** (attempt - 1)), max_backoff)
                dv_logger.warning(f"[Retry {attempt}/{max_retries}] Audit {audit_id}: {type(e).__name__}, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
    
    dv_logger.error(f"Failed to fetch details for audit {audit_id} after {max_retries} retries")
    return None


# ============================================================================
# Window Processing
# ============================================================================

async def process_window(
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str,
    attributes: List[str]
) -> int:
    """Process a single time window for one entity. Returns record count."""
    dv_query = CONFIG.get("dataverse", {}).get("query", {})
    concurrent_fetch = dv_query.get("concurrent_audit_fetch", 5)
    sf_query = CONFIG.get("snowflake", {}).get("query", {})
    batch_insert_size = sf_query.get("batch_insert_size", 100)
    features = CONFIG.get("features", {})
    log_progress_interval = CONFIG.get("monitoring", {}).get("log_progress_every_records", 1000)
    dry_run = features.get("dry_run", False)
    
    window_minutes = int((window_end - window_start).total_seconds() / 60)
    logger.info(f"[{entity}] Processing window {window_start.isoformat()} to {window_end.isoformat()} ({window_minutes} min)")
    
    connection = get_snowflake_connection()
    
    try:
        async with aiohttp.ClientSession() as session:
            # Step 1: Fetch audit IDs for window
            audit_ids = await fetch_audits(session, token, window_start, window_end, entity)
            
            if not audit_ids:
                logger.info(f"[{entity}] No audits in window")
                # Still update state to advance window
                if features.get("enable_state_tracking", True) and not dry_run:
                    update_sync_state(connection, entity, window_end, 0)
                return 0
            
            # Step 2: Concurrently fetch audit details (in batches)
            audit_details = []
            for i in range(0, len(audit_ids), concurrent_fetch):
                if _shutdown_requested:
                    logger.warning(f"[{entity}] Shutdown requested, stopping window processing")
                    return 0
                
                batch = audit_ids[i:i + concurrent_fetch]
                tasks = [
                    fetch_audit_details_with_retry(session, token, audit_id, attributes)
                    for audit_id in batch
                ]
                results = await asyncio.gather(*tasks)
                audit_details.extend([r for r in results if r])
                
                if len(audit_details) > 0 and len(audit_details) % log_progress_interval == 0:
                    logger.info(f"[{entity}] Progress: {len(audit_details)}/{len(audit_ids)} fetched")
            
            # Step 3: Insert to Snowflake (or dry-run)
            if dry_run:
                logger.warning(f"[{entity}] DRY RUN: Would insert {len(audit_details)} records to Snowflake")
            elif audit_details:
                cursor = connection.cursor()
                run_id = str(uuid.uuid4())
                
                # Batch insert
                for i in range(0, len(audit_details), batch_insert_size):
                    batch_to_insert = audit_details[i:i + batch_insert_size]
                    
                    if features.get("enable_idempotent_upserts", True):
                        # Idempotent MERGE (replay-safe)
                        for details in batch_to_insert:
                            cursor.execute(
                                """MERGE INTO audit_logs target
                                   USING (SELECT %s AS audit_id, %s AS entity, %s AS changes,
                                                 %s AS processed_at, %s AS run_id) source
                                   ON target.audit_id = source.audit_id
                                   WHEN MATCHED THEN UPDATE SET
                                     changes = source.changes,
                                     processed_at = source.processed_at,
                                     run_id = source.run_id
                                   WHEN NOT MATCHED THEN INSERT
                                     (audit_id, entity, changes, processed_at, run_id)
                                   VALUES
                                     (source.audit_id, source.entity, source.changes,
                                      source.processed_at, source.run_id)
                                """,
                                (
                                    details.get("auditid", ""),
                                    entity,
                                    json.dumps(details),
                                    datetime.utcnow().isoformat(),
                                    run_id
                                )
                            )
                    else:
                        # Plain INSERT (faster but not replay-safe)
                        for details in batch_to_insert:
                            cursor.execute(
                                """INSERT INTO audit_logs
                                   (audit_id, entity, changes, processed_at, run_id)
                                   VALUES (%s, %s, %s, %s, %s)
                                """,
                                (
                                    details.get("auditid", ""),
                                    entity,
                                    json.dumps(details),
                                    datetime.utcnow().isoformat(),
                                    run_id
                                )
                            )
                
                connection.commit()
                logger.info(f"[{entity}] Inserted {len(audit_details)} records to Snowflake")
            
            # Step 4: Atomic state update (only after successful insert)
            if features.get("enable_state_tracking", True) and not dry_run:
                update_sync_state(connection, entity, window_end, len(audit_details))
                logger.debug(f"[{entity}] State updated: lastSyncEnd={window_end.isoformat()}")
            
            return len(audit_details)
    
    except Exception as e:
        logger.error(f"[{entity}] Error processing window: {e}", exc_info=True)
        raise
    finally:
        connection.close()


# ============================================================================
# Entity Processor (catches up backlog → continues live)
# ============================================================================

async def process_entity_continuous(
    token: str,
    entity: str,
    attributes: List[str]
) -> int:
    """
    Process an entity with adaptive behavior:
    - If far behind: large windows, fast catch-up (backlog mode)
    - When close to current: small windows, gentle live mode
    
    Loops until shutdown requested OR exit_when_caught_up is true.
    """
    features = CONFIG.get("features", {})
    perf_config = CONFIG.get("performance", {})
    sleep_between = perf_config.get("sleep_between_windows_seconds", 1)
    sleep_caught_up = perf_config.get("sleep_when_caught_up_seconds", 60)
    exit_when_caught_up = features.get("exit_when_caught_up", False)
    
    connection = get_snowflake_connection()
    try:
        last_sync_end = get_sync_state(connection, entity)
    finally:
        connection.close()
    
    logger.info(f"[{entity}] Starting from lastSyncEnd={last_sync_end.isoformat()}")
    logger.info(f"[{entity}] Tracking attributes: {', '.join(attributes)}")
    
    total_processed = 0
    
    while not _shutdown_requested:
        # Adaptive window sizing
        window_size_minutes = determine_window_size(last_sync_end)
        window_start = last_sync_end
        window_end = window_start + timedelta(minutes=window_size_minutes)
        
        # If we'd advance into the future, we're caught up
        now = datetime.utcnow()
        if window_end > now:
            if exit_when_caught_up:
                logger.info(f"[{entity}] Caught up to current time, exiting (exit_when_caught_up=true)")
                break
            else:
                # Live mode: wait for new data
                logger.info(
                    f"[{entity}] Caught up to current time. "
                    f"Sleeping {sleep_caught_up}s before next check..."
                )
                # Sleep in small increments to allow graceful shutdown
                slept = 0
                while slept < sleep_caught_up and not _shutdown_requested:
                    await asyncio.sleep(min(5, sleep_caught_up - slept))
                    slept += 5
                continue
        
        # Determine mode for logging
        lag_minutes = (now - last_sync_end).total_seconds() / 60
        mode = "BACKLOG" if window_size_minutes == CONFIG["windowSizeMinutes"]["backlog"] else "LIVE"
        logger.info(f"[{entity}] Mode={mode}, lag={lag_minutes:.1f}min, window={window_size_minutes}min")
        
        try:
            count = await process_window(token, window_start, window_end, entity, attributes)
            total_processed += count
            last_sync_end = window_end
            
            # Brief pause between windows to be gentle on APIs
            if sleep_between > 0:
                await asyncio.sleep(sleep_between)
        
        except Exception as e:
            logger.error(f"[{entity}] Window processing failed, will retry: {e}")
            # Don't advance window on error; sleep and retry
            await asyncio.sleep(10)
    
    logger.info(f"[{entity}] Processing stopped. Total records: {total_processed}")
    return total_processed


# ============================================================================
# Main Entry Point
# ============================================================================

async def run_sync(invocation_source: str = "console"):
    """
    Main sync orchestrator. Works for any host (console/container/function).
    
    Args:
        invocation_source: "console", "container", or "function" (for logging)
    """
    features = CONFIG.get("features", {})
    perf_config = CONFIG.get("performance", {})
    max_concurrent = perf_config.get("max_concurrent_entities", 3)
    
    logger.info("=" * 70)
    logger.info(f"Dataverse Audit Sync - Unified Deployment")
    logger.info(f"Invocation source: {invocation_source}")
    logger.info(f"Auto-detect mode: {CONFIG.get('modeAutoDetect', {}).get('enabled', True)}")
    logger.info(f"Exit when caught up: {features.get('exit_when_caught_up', False)}")
    logger.info(f"Dry run: {features.get('dry_run', False)}")
    logger.info(f"Max concurrent entities: {max_concurrent}")
    logger.info("=" * 70)
    
    if features.get("dry_run", False):
        logger.warning("DRY RUN MODE - No changes will be written to Snowflake")
    
    # Get OAuth token
    try:
        token = await get_dataverse_token()
        logger.info("OAuth token acquired successfully")
    except Exception as e:
        logger.error(f"Failed to acquire OAuth token: {e}")
        raise
    
    # Determine which entities to process
    if ENTITY_FILTER:
        entities_to_process = [e for e in CONFIG["entities"] if e["name"] == ENTITY_FILTER]
        if not entities_to_process:
            logger.error(f"Entity '{ENTITY_FILTER}' not found in config.json")
            sys.exit(1)
        logger.info(f"Filtering to single entity: {ENTITY_FILTER}")
    else:
        entities_to_process = CONFIG["entities"]
        logger.info(f"Processing {len(entities_to_process)} entities from config")
    
    # Process entities (concurrent or sequential based on config)
    if max_concurrent > 1 and len(entities_to_process) > 1:
        logger.info(f"Running entities concurrently (max {max_concurrent} at a time)")
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def process_with_limit(entity_config):
            async with semaphore:
                return await process_entity_continuous(
                    token, entity_config["name"], entity_config["attributes"]
                )
        
        tasks = [process_with_limit(e) for e in entities_to_process]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total = 0
        for entity_config, result in zip(entities_to_process, results):
            if isinstance(result, Exception):
                logger.error(f"[{entity_config['name']}] Failed: {result}")
            else:
                total += result
    else:
        logger.info("Running entities sequentially")
        total = 0
        for entity_config in entities_to_process:
            try:
                count = await process_entity_continuous(
                    token, entity_config["name"], entity_config["attributes"]
                )
                total += count
            except Exception as e:
                logger.error(f"[{entity_config['name']}] Failed: {e}")
                continue
    
    logger.info("=" * 70)
    logger.info(f"Sync completed. Total records processed: {total}")
    logger.info("=" * 70)
    return total


def main():
    """Synchronous entry point for console/container deployments"""
    # Register signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, _handle_shutdown)
    signal.signal(signal.SIGTERM, _handle_shutdown)
    
    try:
        asyncio.run(run_sync(invocation_source="console"))
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
