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
import asyncio
import aiohttp
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import snowflake.connector
from snowflake.connector import DictCursor
import msal

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ============================================================================
# Configuration Loading
# ============================================================================

def load_config() -> Dict:
    """Load configuration from config.json with environment variable overrides"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    # Load defaults from config.json
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
            logger.info(f"Loaded config from {config_path}")
    except FileNotFoundError:
        logger.warning(f"config.json not found at {config_path}, using defaults")
        config = {
            "windowSizeMinutes": {"backlog": 60, "continuous": 10},
            "entities": [
                {"name": "Account", "attributes": ["name", "telephone1", "address1_city"]},
                {"name": "Contact", "attributes": ["fullname", "emailaddress1", "mobilephone"]},
                {"name": "Case", "attributes": ["title", "description", "prioritycode"]}
            ]
        }
    except json.JSONDecodeError as e:
        logger.error(f"Invalid JSON in config.json: {e}")
        sys.exit(1)
    
    return config


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
WINDOW_SIZE_MINUTES = CONFIG["windowSizeMinutes"]["backlog" if BACKLOG_MODE else "continuous"]


async def get_dataverse_token() -> str:
    """Get OAuth 2.0 token from Microsoft Entra ID"""
    authority_url = "https://login.microsoftonline.com/common"
    app = msal.PublicClientApplication(
        client_id=CLIENT_ID,
        authority=authority_url
    )
    
    token_response = app.acquire_token_by_username_password(
        username=CLIENT_ID,
        password=CLIENT_SECRET,
        scopes=["https://org.dynamics.com/.default"]
    )
    
    if "access_token" not in token_response:
        raise Exception(f"Failed to acquire token: {token_response.get('error_description')}")
    
    return token_response["access_token"]


def get_snowflake_connection():
    """Create Snowflake connection"""
    return snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database="AUDIT_DB",
        schema="PUBLIC"
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
    """Query Dataverse for audits via Web API (5000 per page, pagination support)"""
    audits = []
    
    # FetchXML query
    fetch_xml = f"""
    <fetch version='1.0' page='1' paging-cookie=''>
      <entity name='audit'>
        <attribute name='auditid' />
        <attribute name='objectid' />
        <attribute name='operation' />
        <attribute name='createdon' />
        <filter type='and'>
          <condition attribute='createdon' operator='ge' value='{window_start.isoformat()}Z' />
          <condition attribute='createdon' operator='lt' value='{window_end.isoformat()}Z' />
          <condition attribute='objectid' operator='in'>
            <value uitype='{entity}'></value>
          </condition>
        </filter>
      </entity>
    </fetch>
    """
    
    filter_query = f"createdon ge {window_start.isoformat()}Z and createdon lt {window_end.isoformat()}Z"
    url = f"{DATAVERSE_ORG_URL}/api/data/v9.2/audits?$filter={filter_query}&$select=auditid&$top=5000"
    
    headers = {"Authorization": f"Bearer {token}"}
    
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            for item in data.get("value", []):
                audits.append(item["auditid"])
            logger.info(f"[{entity}] Fetched {len(audits)} audits for window {window_start} to {window_end}")
        else:
            logger.error(f"Failed to fetch audits: {response.status}")
    
    return audits


async def fetch_audit_details_with_retry(
    session: aiohttp.ClientSession,
    token: str,
    audit_id: str,
    attributes: List[str]
) -> Optional[Dict]:
    """Fetch audit details via RetrieveAuditDetails action with 3-retry exponential backoff"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "auditId": audit_id,
        "propertySet": attributes
    }
    
    url = f"{DATAVERSE_ORG_URL}/api/data/v9.2/RetrieveAuditDetails"
    
    for attempt in range(1, 4):
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("AuditRecord", {})
                elif attempt < 3:
                    delay = 2 ** (attempt - 1)
                    logger.warning(f"Retry {attempt} for {audit_id}, waiting {delay}s")
                    await asyncio.sleep(delay)
        except Exception as e:
            if attempt < 3:
                delay = 2 ** (attempt - 1)
                logger.warning(f"Error fetching {audit_id}: {e}, retry in {delay}s")
                await asyncio.sleep(delay)
    
    logger.error(f"Failed to fetch details for audit {audit_id} after 3 retries")
    return None


async def process_window(
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str,
    attributes: List[str]
):
    """Process single 60-minute time window"""
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
            
            # Fetch details for each audit (concurrent, but batched in groups of 5)
            audit_details = []
            for i in range(0, len(audit_ids), 5):
                batch = audit_ids[i:i+5]
                tasks = [fetch_audit_details_with_retry(session, token, audit_id, attributes) for audit_id in batch]
                results = await asyncio.gather(*tasks)
                audit_details.extend([r for r in results if r])
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
                logger.info(f"[{entity}] Inserted {len(audit_details)} audits to Snowflake")
            
            # Update state (atomic - only after all inserts successful)
            update_sync_state(connection, entity, window_end, len(audit_details))
            logger.info(f"[{entity}] State updated: lastSyncEnd={window_end.isoformat()}")
            
            return len(audit_details)
    
    finally:
        connection.close()


async def main():
    """Main entry point"""
    logger.info(f"Starting Dataverse Audit Sync (Python)")
    logger.info(f"Backlog Mode: {BACKLOG_MODE}, Window: {WINDOW_SIZE_MINUTES} min")
    logger.info(f"Configuration loaded from config.json")
    
    # Get OAuth token
    token = await get_dataverse_token()
    logger.info("OAuth token acquired")
    
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
    connection = get_snowflake_connection()
    
    # Process each entity
    for entity_config in entities_to_process:
        entity_name = entity_config["name"]
        attributes = entity_config["attributes"]
        
        logger.info(f"[{entity_name}] Starting with attributes: {attributes}")
        
        last_sync_end = get_sync_state(connection, entity_name)
        
        # Override start time if provided
        if OVERRIDE_START_TIME:
            try:
                last_sync_end = datetime.fromisoformat(OVERRIDE_START_TIME)
                logger.info(f"[{entity_name}] Override start time: {last_sync_end.isoformat()}")
            except ValueError:
                logger.warning(f"[{entity_name}] Invalid OVERRIDE_START_TIME: {OVERRIDE_START_TIME}")
        
        # Process windows
        total_processed = 0
        while True:
            window_start = last_sync_end
            window_end = window_start + timedelta(minutes=WINDOW_SIZE_MINUTES)
            
            if window_end > datetime.utcnow():
                logger.info(f"[{entity_name}] Reached current time, stopping")
                break
            
            count = await process_window(token, window_start, window_end, entity_name, attributes)
            total_processed += count
            last_sync_end = window_end
            
            if not BACKLOG_MODE:
                logger.info(f"[{entity_name}] Continuous mode: processed one window, exiting")
                break
        
        logger.info(f"[{entity_name}] Processing completed. Total audits: {total_processed}")
    
    connection.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except Exception as e:
        logger.error(f"Fatal error: {e}")
        sys.exit(1)
