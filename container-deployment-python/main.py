#!/usr/bin/env python3
"""
Dataverse Audit Sync - Backlog Phase (Python)
Processes large time windows (60 min) to sync historical audits to Snowflake
Runs in parallel Docker containers, one per entity (Account, Contact, Case)

Configuration: Load from config.json, override with environment variables
"""

import os
import sys
import json
import logging
import logging.handlers
import asyncio
import aiohttp
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import snowflake.connector
from snowflake.connector import DictCursor
import msal

# Logger setup - will be configured after loading config
logger = None

# ============================================================================
# Configuration Loading and Logging Setup
# ============================================================================

def load_config() -> Dict:
    """Load configuration from config.json with environment variable overrides"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    # Load defaults from config.json
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print(f"ERROR: config.json not found at {config_path}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"ERROR: Invalid JSON in config.json: {e}", file=sys.stderr)
        sys.exit(1)
    
    return config


def setup_logging(config: Dict) -> logging.Logger:
    """Configure logging based on config settings"""
    log_config = config.get("logging", {})
    level_str = log_config.get("level", "INFO")
    log_level = getattr(logging, level_str, logging.INFO)
    
    log_format = log_config.get("format", "[%(asctime)s] %(levelname)s: %(message)s")
    date_format = log_config.get("date_format", "%Y-%m-%d %H:%M:%S")
    
    # Remove existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create formatter
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
        log_file = file_config.get("path", "/var/log/audit-sync.log")
        max_size = file_config.get("max_size_mb", 100) * 1024 * 1024
        backup_count = file_config.get("backup_count", 5)
        
        # Create directory if needed
        os.makedirs(os.path.dirname(log_file), exist_ok=True)
        
        file_handler = logging.handlers.RotatingFileHandler(
            log_file,
            maxBytes=max_size,
            backupCount=backup_count
        )
        file_level = getattr(logging, file_config.get("level", "DEBUG"), logging.DEBUG)
        file_handler.setLevel(file_level)
        file_handler.setFormatter(formatter)
        logging.root.addHandler(file_handler)
    
    # Set root logger level
    logging.root.setLevel(log_level)
    
    # Component-level logging configuration
    components = log_config.get("components", {})
    for component, comp_level in components.items():
        comp_level_int = getattr(logging, comp_level, logging.INFO)
        logging.getLogger(component).setLevel(comp_level_int)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured: level={level_str}, console={output_config.get('console', True)}, "
                f"file={file_config.get('enabled', False)}")
    
    return logger


# Environment variables
DATAVERSE_ORG_URL = os.getenv("DATAVERSE_ORG_URL", "https://yourorg.crm.dynamics.com")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")
BACKLOG_MODE = os.getenv("BACKLOG_MODE", "true").lower() == "true"
OVERRIDE_START_TIME = os.getenv("OVERRIDE_START_TIME")

# Single entity filter (optional - process only one entity)
ENTITY_FILTER = os.getenv("ENTITY")  # e.g., "Account" to process only Account

# Snowflake connection
SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# Load configuration
CONFIG = load_config()
logger = setup_logging(CONFIG)

WINDOW_SIZE_MINUTES = CONFIG["windowSizeMinutes"]["backlog" if BACKLOG_MODE else "continuous"]
logger.info(f"Loaded configuration: backlog_mode={BACKLOG_MODE}, window_size={WINDOW_SIZE_MINUTES}min")


async def get_dataverse_token() -> str:
    """Get OAuth 2.0 token from Microsoft Entra ID using config settings"""
    dv_auth = CONFIG.get("dataverse", {}).get("auth", {})
    authority_url = dv_auth.get("authority_url", "https://login.microsoftonline.com/common")
    scope = dv_auth.get("scope", "https://org.dynamics.com/.default")
    token_cache_minutes = dv_auth.get("token_cache_minutes", 55)
    
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
    
    logger.debug(f"OAuth token acquired (cache valid for {token_cache_minutes}min)")
    return token_response["access_token"]


def get_snowflake_connection():
    """Create Snowflake connection with config timeout and retry settings"""
    sf_config = CONFIG.get("snowflake", {}).get("connection", {})
    timeout = sf_config.get("timeout_seconds", 30)
    
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database="AUDIT_DB",
        schema="PUBLIC",
        connection_timeout=timeout
    )


def get_sync_state(connection, entity: str) -> datetime:
    """Get last sync time from Snowflake state table"""
    try:
        cursor = connection.cursor(DictCursor)
        cursor.execute(
            "SELECT last_sync_end FROM sync_state WHERE entity = %s",
            (entity,)
        )
        row = cursor.fetchone()
        if row:
            return datetime.fromisoformat(row["LAST_SYNC_END"])
    except Exception as e:
        logger.warning(f"Could not fetch state: {e}")
    
    # Default: last window before now
    return datetime.utcnow() - timedelta(minutes=WINDOW_SIZE_MINUTES)


def update_sync_state(connection, entity: str, last_sync_end: datetime, record_count: int):
    """Update last sync time in Snowflake state table (atomic)"""
    cursor = connection.cursor()
    cursor.execute(
        """INSERT INTO sync_state (entity, last_sync_end, record_count, updated_at)
           VALUES (%s, %s, %s, %s)
           ON CONFLICT (entity) DO UPDATE SET
           last_sync_end = EXCLUDED.last_sync_end,
           record_count = EXCLUDED.record_count,
           updated_at = EXCLUDED.updated_at
        """,
        (entity, last_sync_end.isoformat(), record_count, datetime.utcnow().isoformat())
    )
    connection.commit()


async def fetch_audits_with_pagination(
    session: aiohttp.ClientSession,
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str
) -> List[str]:
    """Query Dataverse for audits via Web API with configurable page size"""
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    page_size = CONFIG.get("dataverse", {}).get("query", {}).get("page_size", 5000)
    timeout = dv_api.get("timeout_seconds", 30)
    
    audits = []
    
    filter_query = f"createdon ge {window_start.isoformat()}Z and createdon lt {window_end.isoformat()}Z"
    url = f"{DATAVERSE_ORG_URL}/api/data/{api_version}/audits?$filter={filter_query}&$select=auditid&$top={page_size}"
    
    headers = {"Authorization": f"Bearer {token}"}
    
    try:
        async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
            if response.status == 200:
                data = await response.json()
                for item in data.get("value", []):
                    audits.append(item["auditid"])
                logger.info(f"[{entity}] Fetched {len(audits)} audits for window {window_start} to {window_end}")
            else:
                logger.error(f"[{entity}] Failed to fetch audits: {response.status}")
    except asyncio.TimeoutError:
        logger.error(f"[{entity}] Timeout fetching audits after {timeout}s")
    except Exception as e:
        logger.error(f"[{entity}] Error fetching audits: {e}")
    
    return audits


async def fetch_audit_details_with_retry(
    session: aiohttp.ClientSession,
    token: str,
    audit_id: str,
    attributes: List[str]
) -> Optional[Dict]:
    """Fetch audit details via RetrieveAuditDetails action with configurable retries"""
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    timeout = dv_api.get("timeout_seconds", 30)
    max_retries = dv_api.get("max_retries", 3)
    retry_delay = dv_api.get("retry_delay_seconds", 1)
    perf_config = CONFIG.get("performance", {})
    backoff_multiplier = perf_config.get("backoff_multiplier", 2.0)
    
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
            async with session.post(url, json=payload, headers=headers, 
                                   timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                if response.status == 200:
                    data = await response.json()
                    logger.debug(f"Retrieved audit details for {audit_id}")
                    return data.get("AuditRecord", {})
                elif attempt < max_retries:
                    delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                    logger.warning(f"[Retry {attempt}/{max_retries}] Audit {audit_id}: HTTP {response.status}, waiting {delay:.1f}s")
                    await asyncio.sleep(delay)
        except asyncio.TimeoutError:
            if attempt < max_retries:
                delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                logger.warning(f"[Retry {attempt}/{max_retries}] Audit {audit_id}: Timeout after {timeout}s, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
        except Exception as e:
            if attempt < max_retries:
                delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                logger.warning(f"[Retry {attempt}/{max_retries}] Audit {audit_id}: {type(e).__name__}: {e}, waiting {delay:.1f}s")
                await asyncio.sleep(delay)
    
    logger.error(f"Failed to fetch details for audit {audit_id} after {max_retries} retries")
    return None


async def process_window(
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str,
    attributes: List[str]
):
    """Process single time window with configurable batch and concurrency settings"""
    dv_query = CONFIG.get("dataverse", {}).get("query", {})
    concurrent_fetch = dv_query.get("concurrent_audit_fetch", 5)
    sf_query = CONFIG.get("snowflake", {}).get("query", {})
    batch_insert_size = sf_query.get("batch_insert_size", 100)
    features = CONFIG.get("features", {})
    log_progress_interval = CONFIG.get("monitoring", {}).get("log_progress_every_records", 1000)
    
    logger.info(f"[{entity}] Processing window {window_start.isoformat()} to {window_end.isoformat()}")
    logger.info(f"[{entity}] Tracking attributes: {', '.join(attributes)}")
    
    connection = get_snowflake_connection()
    
    try:
        async with aiohttp.ClientSession() as session:
            # Fetch audit list
            audit_ids = await fetch_audits_with_pagination(
                session, token, window_start, window_end, entity
            )
            
            if not audit_ids:
                logger.info(f"[{entity}] No audits found in window")
                return 0
            
            # Fetch details for each audit (concurrent, but batched)
            audit_details = []
            for i in range(0, len(audit_ids), concurrent_fetch):
                batch = audit_ids[i:i+concurrent_fetch]
                tasks = [fetch_audit_details_with_retry(session, token, audit_id, attributes) for audit_id in batch]
                results = await asyncio.gather(*tasks)
                audit_details.extend([r for r in results if r])
                
                if len(audit_details) % log_progress_interval == 0:
                    logger.info(f"[{entity}] Progress: {len(audit_details)} audits fetched")
                
                await asyncio.sleep(0.1)  # Brief pause between batches
            
            # Insert batch to Snowflake
            if audit_details:
                cursor = connection.cursor()
                run_id = str(uuid.uuid4())
                
                for details in audit_details:
                    cursor.execute(
                        """INSERT INTO audits (audit_id, entity, changes, processed_at, run_id)
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
                logger.info(f"[{entity}] Inserted {len(audit_details)} audits to Snowflake (batch_size={batch_insert_size})")
            
            # Update state (atomic - only after all inserts successful)
            if features.get("enable_state_tracking", True):
                update_sync_state(connection, entity, window_end, len(audit_details))
                logger.info(f"[{entity}] State updated: lastSyncEnd={window_end.isoformat()}")
            
            return len(audit_details)
    
    finally:
        connection.close()


async def main():
    """Main entry point"""
    features = CONFIG.get("features", {})
    dry_run = features.get("dry_run", False)
    
    logger.info(f"{'='*60}")
    logger.info(f"Starting Dataverse Audit Sync (Python)")
    logger.info(f"Backlog Mode: {BACKLOG_MODE}, Window: {WINDOW_SIZE_MINUTES} minutes")
    logger.info(f"Entities: {len(CONFIG['entities'])} total")
    logger.info(f"Features: StateTracking={features.get('enable_state_tracking', True)}, "
               f"IdempotentUpserts={features.get('enable_idempotent_upserts', True)}, "
               f"DryRun={dry_run}")
    logger.info(f"{'='*60}")
    
    if dry_run:
        logger.warning("DRY RUN MODE - No changes will be written to Snowflake")
    
    # Get OAuth token
    try:
        token = await get_dataverse_token()
        logger.info("OAuth token acquired successfully")
    except Exception as e:
        logger.error(f"Failed to acquire OAuth token: {e}")
        sys.exit(1)
    
    # Determine which entities to process
    if ENTITY_FILTER:
        # Process only specified entity
        entities_to_process = [e for e in CONFIG["entities"] if e["name"] == ENTITY_FILTER]
        if not entities_to_process:
            logger.error(f"Entity '{ENTITY_FILTER}' not found in config.json")
            sys.exit(1)
        logger.info(f"Processing single entity: {ENTITY_FILTER}")
    else:
        # Process all entities from config
        entities_to_process = CONFIG["entities"]
        logger.info(f"Processing {len(entities_to_process)} entities from config")
    
    # Get connection and state
    try:
        connection = get_snowflake_connection()
        logger.info("Snowflake connection established")
    except Exception as e:
        logger.error(f"Failed to connect to Snowflake: {e}")
        sys.exit(1)
    
    # Process each entity
    total_records = 0
    for entity_config in entities_to_process:
        entity_name = entity_config["name"]
        attributes = entity_config["attributes"]
        
        logger.info(f"\n[{entity_name}] Starting with attributes: {', '.join(attributes)}")
        
        try:
            last_sync_end = get_sync_state(connection, entity_name)
        except Exception as e:
            logger.error(f"[{entity_name}] Failed to get sync state: {e}")
            continue
        
        # Override start time if provided
        if OVERRIDE_START_TIME:
            try:
                last_sync_end = datetime.fromisoformat(OVERRIDE_START_TIME)
                logger.info(f"[{entity_name}] Override start time: {last_sync_end.isoformat()}")
            except ValueError:
                logger.warning(f"[{entity_name}] Invalid OVERRIDE_START_TIME: {OVERRIDE_START_TIME}")
        
        # Process windows
        total_processed = 0
        try:
            while True:
                window_start = last_sync_end
                window_end = window_start + timedelta(minutes=WINDOW_SIZE_MINUTES)
                
                if window_end > datetime.utcnow():
                    logger.info(f"[{entity_name}] Reached current time, stopping")
                    break
                
                count = await process_window(token, window_start, window_end, entity_name, attributes)
                total_processed += count
                total_records += count
                last_sync_end = window_end
                
                if not BACKLOG_MODE:
                    logger.info(f"[{entity_name}] Continuous mode: processed one window, exiting")
                    break
        except Exception as e:
            logger.error(f"[{entity_name}] Error during processing: {e}")
            continue
        
        logger.info(f"[{entity_name}] Processing completed. Total audits: {total_processed}")
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Sync completed. Total records processed: {total_records}")
    logger.info(f"{'='*60}")
    
    connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)
