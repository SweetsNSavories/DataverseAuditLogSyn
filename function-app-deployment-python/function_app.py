#!/usr/bin/env python3
"""
Dataverse Audit Sync - Continuous Phase (Python)
Azure Functions timer-triggered function
Runs every 10 minutes to capture new audits to Snowflake

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
from typing import Optional, Dict, List
import azure.functions as func
import snowflake.connector
from snowflake.connector import DictCursor
import msal

# Logger setup - will be configured after loading config
logger = None

# ============================================================================
# Configuration Loading and Logging Setup
# ============================================================================

def load_config() -> Dict:
    """Load configuration from config.json"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
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
    
    # Azure Functions has specific logger setup, enhance it
    logger_obj = logging.getLogger("AuditSyncTimer")
    logger_obj.setLevel(log_level)
    
    # Create formatter
    formatter = logging.Formatter(log_format, datefmt=date_format)
    
    # Console handler (Azure Functions logs to console by default)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    console_handler.setLevel(log_level)
    logger_obj.addHandler(console_handler)
    
    # Component-level logging configuration
    components = log_config.get("components", {})
    for component, comp_level in components.items():
        comp_level_int = getattr(logging, comp_level, logging.INFO)
        logging.getLogger(component).setLevel(comp_level_int)
    
    logger_obj.info(f"Logging configured: level={level_str}")
    
    return logger_obj

# Environment variables
DATAVERSE_ORG_URL = os.getenv("DATAVERSE_ORG_URL", "https://yourorg.crm.dynamics.com")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

# Load configuration
CONFIG = load_config()
logger = setup_logging(CONFIG)
WINDOW_SIZE_MINUTES = CONFIG["windowSizeMinutes"]["continuous"]
logger.info(f"Loaded configuration: window_size={WINDOW_SIZE_MINUTES} min (continuous)")


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
    """Get last sync time from Snowflake"""
    try:
        cursor = connection.cursor(DictCursor)
        cursor.execute(
            "SELECT last_sync_end FROM sync_state WHERE entity = %s",
            (entity,)
        )
        row = cursor.fetchone()
        if row:
            return datetime.fromisoformat(row["LAST_SYNC_END"])
    except:
        pass
    
    return datetime.utcnow() - timedelta(minutes=WINDOW_SIZE_MINUTES)


def update_sync_state(connection, entity: str, last_sync_end: datetime, record_count: int):
    """Update sync state"""
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


async def fetch_audits(
    session: aiohttp.ClientSession,
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str
) -> list:
    """Query Dataverse for audits with configurable API version and page size"""
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
                logger.info(f"[{entity}] Fetched {len(audits)} audits")
            else:
                logger.error(f"[{entity}] Audit query failed: HTTP {response.status}")
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
    """Fetch audit details with retries"""
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
                    return data.get("AuditRecord", {})
                elif attempt < max_retries:
                    delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                    logger.debug(f"[Retry {attempt}/{max_retries}] {audit_id}: HTTP {response.status}")
                    await asyncio.sleep(delay)
        except asyncio.TimeoutError:
            if attempt < max_retries:
                delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                logger.debug(f"[Retry {attempt}/{max_retries}] {audit_id}: Timeout")
                await asyncio.sleep(delay)
        except Exception as e:
            if attempt < max_retries:
                delay = retry_delay * (backoff_multiplier ** (attempt - 1))
                logger.debug(f"[Retry {attempt}/{max_retries}] {audit_id}: {type(e).__name__}")
                await asyncio.sleep(delay)
    
    logger.error(f"Failed to fetch details for audit {audit_id} after {max_retries} retries")
    return None


async def process_entity(token: str, entity: str, attributes: List[str]):
    """Process single entity with configurable concurrency and batching"""
    dv_query = CONFIG.get("dataverse", {}).get("query", {})
    concurrent_fetch = dv_query.get("concurrent_audit_fetch", 5)
    sf_query = CONFIG.get("snowflake", {}).get("query", {})
    batch_insert_size = sf_query.get("batch_insert_size", 100)
    features = CONFIG.get("features", {})
    log_progress_interval = CONFIG.get("monitoring", {}).get("log_progress_every_records", 1000)
    
    logger.info(f"[{entity}] Processing with attributes: {', '.join(attributes)}")
    connection = get_snowflake_connection()
    
    try:
        # Get last sync time
        last_sync_end = get_sync_state(connection, entity)
        window_start = last_sync_end
        window_end = datetime.utcnow()
        
        logger.info(f"[{entity}] Processing {window_start.isoformat()} to {window_end.isoformat()}")
        
        async with aiohttp.ClientSession() as session:
            # Fetch audits
            audit_ids = await fetch_audits(session, token, window_start, window_end, entity)
            
            if not audit_ids:
                logger.info(f"[{entity}] No new audits")
                return 0
            
            # Fetch details (concurrent, but batched)
            audit_details = []
            for i in range(0, len(audit_ids), concurrent_fetch):
                batch = audit_ids[i:i+concurrent_fetch]
                tasks = [fetch_audit_details_with_retry(session, token, aid, attributes) for aid in batch]
                results = await asyncio.gather(*tasks)
                audit_details.extend([r for r in results if r])
                
                if len(audit_details) % log_progress_interval == 0:
                    logger.info(f"[{entity}] Progress: {len(audit_details)} audits fetched")
                
                await asyncio.sleep(0.1)
            
            # Insert to Snowflake
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
                logger.info(f"[{entity}] Inserted {len(audit_details)} audits (batch_size={batch_insert_size})")
            
            # Update state
            if features.get("enable_state_tracking", True):
                update_sync_state(connection, entity, window_end, len(audit_details))
                logger.info(f"[{entity}] State updated: lastSyncEnd={window_end.isoformat()}")
            
            return len(audit_details)
    
    finally:
        connection.close()


async def async_main():
    """Async main - process all configured entities"""
    features = CONFIG.get("features", {})
    dry_run = features.get("dry_run", False)
    
    try:
        logger.info(f"Continuous sync start: {len(CONFIG['entities'])} entities configured")
        
        if dry_run:
            logger.warning("DRY RUN MODE - No changes will be written to Snowflake")
        
        token = await get_dataverse_token()
        logger.info("OAuth token acquired")
        
        # Process all entities from config
        entities_to_process = CONFIG["entities"]
        total_records = 0
        
        for entity_config in entities_to_process:
            entity_name = entity_config["name"]
            attributes = entity_config["attributes"]
            try:
                count = await process_entity(token, entity_name, attributes)
                total_records += count
            except Exception as e:
                logger.error(f"[{entity_name}] Error processing: {e}")
                continue
        
        logger.info(f"Continuous sync completed: {total_records} total records processed")
    
    except Exception as e:
        logger.error(f"Fatal error in async_main: {e}", exc_info=True)
        raise


def main(mytimer: func.TimerRequest) -> None:
    """Azure Functions entry point - timer trigger"""
    try:
        logger.info(f"AuditSync triggered at {datetime.utcnow().isoformat()}")
        
        # Get token and process
        asyncio.run(async_main())
        
        logger.info("AuditSync completed successfully")
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)
        raise
        
        logger.info("All entities processed")
    except Exception as e:
        logger.error(f"Error in async_main: {e}")
        raise


# For local testing
if __name__ == "__main__":
    import logging.config
    logging.basicConfig(level=logging.INFO)
    asyncio.run(async_main())
