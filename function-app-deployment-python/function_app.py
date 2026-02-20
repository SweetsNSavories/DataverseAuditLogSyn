#!/usr/bin/env python3
"""
Dataverse Audit Sync - Continuous Phase (Python)
Azure Functions timer-triggered function
Runs every 10 minutes to capture new audits to Snowflake
"""

import os
import json
import logging
import asyncio
import aiohttp
import uuid
from datetime import datetime, timedelta
from typing import Optional, Dict
import azure.functions as func
import snowflake.connector
from snowflake.connector import DictCursor
import msal

# Configure logging
logger = logging.getLogger("AuditSyncTimer")

# Environment variables
DATAVERSE_ORG_URL = os.getenv("DATAVERSE_ORG_URL", "https://yourorg.crm.dynamics.com")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")

WINDOW_SIZE_MINUTES = 10


async def get_dataverse_token() -> str:
    """Get OAuth 2.0 token"""
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
    """Query Dataverse for audits"""
    audits = []
    filter_query = f"createdon ge {window_start.isoformat()}Z and createdon lt {window_end.isoformat()}Z"
    url = f"{DATAVERSE_ORG_URL}/api/data/v9.2/audits?$filter={filter_query}&$select=auditid&$top=5000"
    
    headers = {"Authorization": f"Bearer {token}"}
    
    async with session.get(url, headers=headers) as response:
        if response.status == 200:
            data = await response.json()
            for item in data.get("value", []):
                audits.append(item["auditid"])
            logger.info(f"[{entity}] Fetched {len(audits)} audits")
        else:
            logger.error(f"Audit query failed: {response.status}")
    
    return audits


async def fetch_audit_details_with_retry(
    session: aiohttp.ClientSession,
    token: str,
    audit_id: str
) -> Optional[Dict]:
    """Fetch audit details with retry"""
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    
    payload = {
        "auditId": audit_id,
        "propertySet": ["name", "telephone1", "address1_city"]
    }
    
    url = f"{DATAVERSE_ORG_URL}/api/data/v9.2/RetrieveAuditDetails"
    
    for attempt in range(1, 4):
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                if response.status == 200:
                    data = await response.json()
                    return data.get("AuditRecord", {})
                elif attempt < 3:
                    await asyncio.sleep(2 ** (attempt - 1))
        except Exception as e:
            if attempt < 3:
                await asyncio.sleep(2 ** (attempt - 1))
    
    return None


async def process_entity(token: str, entity: str):
    """Process single entity"""
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
            
            # Fetch details
            audit_details = []
            for i in range(0, len(audit_ids), 5):
                batch = audit_ids[i:i+5]
                tasks = [fetch_audit_details_with_retry(session, token, aid) for aid in batch]
                results = await asyncio.gather(*tasks)
                audit_details.extend([r for r in results if r])
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
                logger.info(f"[{entity}] Inserted {len(audit_details)} audits")
            
            # Update state
            update_sync_state(connection, entity, window_end, len(audit_details))
            
            return len(audit_details)
    
    finally:
        connection.close()


def main(mytimer: func.TimerRequest) -> None:
    """Azure Functions entry point - timer trigger"""
    try:
        logger.info(f"AuditSync triggered at {datetime.utcnow().isoformat()}")
        
        # Get token
        asyncio.run(async_main())
        
        logger.info("AuditSync completed successfully")
    except Exception as e:
        logger.error(f"Error: {e}")
        raise


async def async_main():
    """Async main"""
    token = await get_dataverse_token()
    
    entities = ["Account", "Contact", "Case"]
    for entity in entities:
        await process_entity(token, entity)


# For local testing
if __name__ == "__main__":
    import logging.config
    logging.basicConfig(level=logging.INFO)
    asyncio.run(async_main())
