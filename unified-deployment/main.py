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
import msal

# Load .env BEFORE reading any os.getenv values. override=True so the file's
# values always win over any stale CLIENT_SECRET / etc. left in the shell
# environment from an earlier session.
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv(override=True)
except ImportError:
    # python-dotenv is optional - in container/function hosts the env vars
    # come from app settings / docker -e flags, not from .env files.
    pass

from sinks import get_sink, AuditSink, SinkError, SinkPartialWriteError


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

# Environment variables (secrets only). Sink-specific creds are read inside the sink.
DATAVERSE_ORG_URL = os.getenv("DATAVERSE_ORG_URL", "https://yourorg.crm.dynamics.com")
TENANT_ID = os.getenv("TENANT_ID")
CLIENT_ID = os.getenv("CLIENT_ID")
CLIENT_SECRET = os.getenv("CLIENT_SECRET")

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
    """
    Acquire an OAuth 2.0 access token via the **client_credentials** flow
    (service principal). Scope = <DATAVERSE_ORG_URL>/.default so the token
    audience matches the Dataverse environment we're talking to.
    """
    auth_logger = logging.getLogger("auth")
    dv_auth = CONFIG.get("dataverse", {}).get("auth", {})
    authority_template = dv_auth.get(
        "authority_url", "https://login.microsoftonline.com/{tenant}"
    )
    if "{tenant}" in authority_template:
        if not TENANT_ID:
            raise Exception(
                "TENANT_ID env var required when authority_url contains {tenant}"
            )
        authority = authority_template.replace("{tenant}", TENANT_ID)
    else:
        authority = authority_template

    # Scope must be the audience .default, NOT a hard-coded org.dynamics.com
    configured_scope = dv_auth.get("scope")
    if configured_scope and "{org}" in configured_scope:
        scope = configured_scope.replace("{org}", DATAVERSE_ORG_URL)
    else:
        scope = configured_scope or f"{DATAVERSE_ORG_URL}/.default"

    app = msal.ConfidentialClientApplication(
        client_id=CLIENT_ID,
        client_credential=CLIENT_SECRET,
        authority=authority,
    )
    token_response = app.acquire_token_for_client(scopes=[scope])

    if "access_token" not in token_response:
        raise Exception(
            f"Failed to acquire token: "
            f"{token_response.get('error_description') or token_response}"
        )
    auth_logger.debug("OAuth token acquired (client_credentials)")
    return token_response["access_token"]


# ============================================================================
# Sink Operations (pluggable: snowflake | cosmos | adls | onelake)
# ============================================================================

def _resolve_initial_state(entity: str, sink: AuditSink) -> datetime:
    """Sink-agnostic state lookup with override + default-to-1h fallback."""
    saved = sink.get_state(entity)
    if saved is not None:
        return saved
    if OVERRIDE_START_TIME:
        try:
            return datetime.fromisoformat(OVERRIDE_START_TIME)
        except ValueError:
            logger.warning(f"Invalid OVERRIDE_START_TIME: {OVERRIDE_START_TIME}")
    return datetime.utcnow() - timedelta(hours=1)


# ============================================================================
# Dataverse API Operations
# ============================================================================

async def fetch_audits(
    session: aiohttp.ClientSession,
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str
) -> List[Dict]:
    """
    Query Dataverse audits via Web API with pagination support.
    Returns header rows (auditid + a few descriptors) which are then enriched
    with RetrieveAuditDetails when they represent attribute changes.
    """
    import urllib.parse
    dv_logger = logging.getLogger("dataverse")
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    timeout = dv_api.get("timeout_seconds", 30)
    page_size = CONFIG.get("dataverse", {}).get("query", {}).get("page_size", 5000)

    audits: List[Dict] = []
    filter_q = urllib.parse.quote(
        f"createdon ge {window_start.isoformat()}Z "
        f"and createdon lt {window_end.isoformat()}Z",
        safe="=",
    )
    select = "auditid,createdon,objecttypecode,operation,action,_userid_value"
    url = (
        f"{DATAVERSE_ORG_URL}/api/data/{api_version}/audits"
        f"?$filter={filter_q}&$select={select}&$top={page_size}"
    )
    headers = {
        "Authorization": f"Bearer {token}",
        "OData-Version": "4.0",
        "Accept": "application/json",
        "Prefer": f"odata.maxpagesize={page_size}",
    }

    try:
        next_url = url
        while next_url:
            async with session.get(
                next_url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    audits.extend(data.get("value", []))
                    next_url = data.get("@odata.nextLink")
                else:
                    body = await response.text()
                    dv_logger.error(
                        f"[{entity}] Audit query failed HTTP {response.status}: {body[:300]}"
                    )
                    break
    except asyncio.TimeoutError:
        dv_logger.error(f"[{entity}] Timeout fetching audits after {timeout}s")
    except Exception as e:
        dv_logger.error(f"[{entity}] Error fetching audits: {e}")

    dv_logger.info(
        f"[{entity}] Fetched {len(audits)} audits for window "
        f"{window_start.isoformat()}Z .. {window_end.isoformat()}Z"
    )
    return audits


def _filter_audit_detail(detail: Dict, attributes: List[str]) -> Dict:
    """
    Prune AttributeAuditDetail OldValue/NewValue/ChangedAttributes to the
    allow-listed attribute names.

    Behavior:
    - If `attributes` is empty/None, return `detail` unchanged (= Option C: store everything).
    - System keys starting with '@' (e.g. '@odata.type') are always preserved.
    - Non-attribute detail types (Relationship, Action, etc.) are left as-is
      because they don't carry a per-attribute payload.
    - Operates on a shallow copy so we don't mutate the upstream response.
    """
    if not attributes or not isinstance(detail, dict):
        return detail

    detail_type = detail.get("@odata.type") or ""
    # Only AttributeAuditDetail carries OldValue/NewValue/ChangedAttributes.
    # Examples: '#Microsoft.Dynamics.CRM.AttributeAuditDetail'
    if "AttributeAuditDetail" not in detail_type and not (
        "OldValue" in detail or "NewValue" in detail or "ChangedAttributes" in detail
    ):
        return detail

    allow = set(attributes)
    pruned = dict(detail)

    def _prune_entity(value):
        if not isinstance(value, dict):
            return value
        return {
            k: v for k, v in value.items()
            if k.startswith("@") or k in allow
        }

    if "OldValue" in pruned:
        pruned["OldValue"] = _prune_entity(pruned["OldValue"])
    if "NewValue" in pruned:
        pruned["NewValue"] = _prune_entity(pruned["NewValue"])
    if "ChangedAttributes" in pruned and isinstance(pruned["ChangedAttributes"], list):
        pruned["ChangedAttributes"] = [
            ca for ca in pruned["ChangedAttributes"]
            if isinstance(ca, dict) and ca.get("LogicalName") in allow
        ]

    return pruned


async def fetch_audit_details_with_retry(
    session: aiohttp.ClientSession,
    token: str,
    audit_header: Dict,
    attributes: List[str]
) -> Optional[Dict]:
    """
    Enrich an audit header with RetrieveAuditDetails (bound function).
    Returns the original header merged with the audit detail payload so
    sinks always see a single dict per audit.
    """
    dv_logger = logging.getLogger("dataverse")
    dv_api = CONFIG.get("dataverse", {}).get("api", {})
    api_version = dv_api.get("version", "v9.2")
    timeout = dv_api.get("timeout_seconds", 30)
    max_retries = dv_api.get("max_retries", 3)
    retry_delay = dv_api.get("retry_delay_seconds", 1)
    perf_config = CONFIG.get("performance", {})
    backoff_multiplier = perf_config.get("backoff_multiplier", 2.0)
    max_backoff = perf_config.get("max_backoff_seconds", 30)

    audit_id = audit_header.get("auditid")
    if not audit_id:
        return None

    headers = {
        "Authorization": f"Bearer {token}",
        "OData-Version": "4.0",
        "Accept": "application/json",
    }

    # RetrieveAuditDetails is a bound FUNCTION on the audit entity, not an action
    url = (
        f"{DATAVERSE_ORG_URL}/api/data/{api_version}/audits({audit_id})"
        f"/Microsoft.Dynamics.CRM.RetrieveAuditDetails()"
    )

    for attempt in range(1, max_retries + 1):
        try:
            async with session.get(
                url, headers=headers,
                timeout=aiohttp.ClientTimeout(total=timeout),
            ) as response:
                if response.status == 200:
                    data = await response.json()
                    detail = data.get("AuditDetail") or {}
                    # Apply per-entity attribute allow-list (no-op if list empty)
                    detail = _filter_audit_detail(detail, attributes)
                    enriched = dict(audit_header)
                    enriched["auditDetail"] = detail
                    enriched["auditDetailType"] = detail.get("@odata.type")
                    return enriched
                elif response.status in (404, 403):
                    # No detail available (e.g., metadata change) - keep header only
                    return dict(audit_header)
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
                dv_logger.warning(
                    f"[Retry {attempt}/{max_retries}] Audit {audit_id}: Timeout, waiting {delay:.1f}s"
                )
                await asyncio.sleep(delay)
        except Exception as e:
            if attempt < max_retries:
                delay = min(retry_delay * (backoff_multiplier ** (attempt - 1)), max_backoff)
                dv_logger.warning(
                    f"[Retry {attempt}/{max_retries}] Audit {audit_id}: "
                    f"{type(e).__name__}, waiting {delay:.1f}s"
                )
                await asyncio.sleep(delay)

    dv_logger.error(
        f"Failed to fetch details for audit {audit_id} after {max_retries} retries"
    )
    # Return header so the sink still records that the audit existed
    return dict(audit_header)


# ============================================================================
# Window Processing
# ============================================================================

async def process_window(
    sink: AuditSink,
    token: str,
    window_start: datetime,
    window_end: datetime,
    entity: str,
    attributes: List[str]
) -> int:
    """Process a single time window for one entity. Returns record count."""
    dv_query = CONFIG.get("dataverse", {}).get("query", {})
    concurrent_fetch = dv_query.get("concurrent_audit_fetch", 5)
    features = CONFIG.get("features", {})
    log_progress_interval = CONFIG.get("monitoring", {}).get("log_progress_every_records", 1000)
    dry_run = features.get("dry_run", False)

    window_minutes = int((window_end - window_start).total_seconds() / 60)
    logger.info(
        f"[{entity}] Processing window {window_start.isoformat()} -> "
        f"{window_end.isoformat()} ({window_minutes} min)"
    )

    async with aiohttp.ClientSession() as session:
        # Step 1: Fetch audit headers (now full dicts, not just IDs)
        audit_headers = await fetch_audits(session, token, window_start, window_end, entity)

        if not audit_headers:
            logger.info(f"[{entity}] No audits in window")
            if features.get("enable_state_tracking", True) and not dry_run:
                sink.update_state(entity, window_end, 0)
            return 0

        # Step 2: Concurrently enrich with RetrieveAuditDetails (in batches)
        enriched: List[Dict] = []
        for i in range(0, len(audit_headers), concurrent_fetch):
            if _shutdown_requested:
                logger.warning(
                    f"[{entity}] Shutdown requested, stopping window processing"
                )
                return 0
            batch = audit_headers[i:i + concurrent_fetch]
            tasks = [
                fetch_audit_details_with_retry(session, token, header, attributes)
                for header in batch
            ]
            results = await asyncio.gather(*tasks)
            enriched.extend([r for r in results if r])

            if enriched and len(enriched) % log_progress_interval == 0:
                logger.info(
                    f"[{entity}] Progress: {len(enriched)}/{len(audit_headers)} fetched"
                )

        # Step 3: Write via the configured sink
        run_id = str(uuid.uuid4())
        if dry_run:
            logger.warning(
                f"[{entity}] DRY RUN: Would write {len(enriched)} records to {sink.name}"
            )
            written = 0
        elif enriched:
            try:
                written = sink.write_audits(
                    entity=entity,
                    records=enriched,
                    window_end=window_end,
                    run_id=run_id,
                )
                logger.info(f"[{entity}] {sink.name} sink: wrote {written} records")
            except SinkPartialWriteError as e:
                # CRITICAL: Re-raise so the outer loop's exception handler
                # skips the `last_sync_end = window_end` assignment and the
                # window gets replayed. Already-written records will be
                # no-op upserts on the next pass (idempotent on auditid).
                logger.error(
                    f"[{entity}] Sink reported partial failure - WINDOW WILL BE RETRIED "
                    f"(succeeded={e.written}, failed={e.failed})"
                )
                raise
            except SinkError as e:
                logger.error(
                    f"[{entity}] Sink write failed - WINDOW WILL BE RETRIED: {e}"
                )
                raise
        else:
            written = 0

        # Step 4: Atomic state update (only after successful write)
        if features.get("enable_state_tracking", True) and not dry_run:
            sink.update_state(entity, window_end, written)
            logger.debug(
                f"[{entity}] State updated: lastSyncEnd={window_end.isoformat()}"
            )

        return written


# ============================================================================
# Entity Processor (catches up backlog → continues live)
# ============================================================================

async def process_entity_continuous(
    sink: AuditSink,
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

    last_sync_end = _resolve_initial_state(entity, sink)
    logger.info(f"[{entity}] Starting from lastSyncEnd={last_sync_end.isoformat()}")
    logger.info(f"[{entity}] Tracking attributes: {', '.join(attributes)}")

    total_processed = 0

    while not _shutdown_requested:
        window_size_minutes = determine_window_size(last_sync_end)
        window_start = last_sync_end
        window_end = window_start + timedelta(minutes=window_size_minutes)

        now = datetime.utcnow()
        if window_end > now:
            if exit_when_caught_up:
                logger.info(
                    f"[{entity}] Caught up to current time, exiting "
                    f"(exit_when_caught_up=true)"
                )
                break
            logger.info(
                f"[{entity}] Caught up to current time. "
                f"Sleeping {sleep_caught_up}s before next check..."
            )
            slept = 0
            while slept < sleep_caught_up and not _shutdown_requested:
                await asyncio.sleep(min(5, sleep_caught_up - slept))
                slept += 5
            continue

        lag_minutes = (now - last_sync_end).total_seconds() / 60
        mode = "BACKLOG" if window_size_minutes == CONFIG["windowSizeMinutes"]["backlog"] else "LIVE"
        logger.info(
            f"[{entity}] Mode={mode}, lag={lag_minutes:.1f}min, window={window_size_minutes}min"
        )

        try:
            count = await process_window(
                sink, token, window_start, window_end, entity, attributes
            )
            total_processed += count
            last_sync_end = window_end
            if sleep_between > 0:
                await asyncio.sleep(sleep_between)
        except Exception as e:
            logger.error(f"[{entity}] Window processing failed, will retry: {e}")
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
    sink_type = (CONFIG.get("sink", {}).get("type") or "snowflake").lower()

    logger.info("=" * 70)
    logger.info(f"Dataverse Audit Sync - Unified Deployment")
    logger.info(f"Invocation source: {invocation_source}")
    logger.info(f"Sink: {sink_type}")
    logger.info(f"Auto-detect mode: {CONFIG.get('modeAutoDetect', {}).get('enabled', True)}")
    logger.info(f"Exit when caught up: {features.get('exit_when_caught_up', False)}")
    logger.info(f"Dry run: {features.get('dry_run', False)}")
    logger.info(f"Max concurrent entities: {max_concurrent}")
    logger.info("=" * 70)

    if features.get("dry_run", False):
        logger.warning(f"DRY RUN MODE - No changes will be written to {sink_type}")

    # Initialise sink (creates DB/container/filesystem if missing)
    sink = get_sink(CONFIG)
    sink.initialize()

    # Get OAuth token
    try:
        token = await get_dataverse_token()
        logger.info("OAuth token acquired successfully")
    except Exception as e:
        logger.error(f"Failed to acquire OAuth token: {e}")
        sink.close()
        raise

    # Determine which entities to process
    if ENTITY_FILTER:
        entities_to_process = [e for e in CONFIG["entities"] if e["name"] == ENTITY_FILTER]
        if not entities_to_process:
            logger.error(f"Entity '{ENTITY_FILTER}' not found in config.json")
            sink.close()
            sys.exit(1)
        logger.info(f"Filtering to single entity: {ENTITY_FILTER}")
    else:
        entities_to_process = CONFIG["entities"]
        logger.info(f"Processing {len(entities_to_process)} entities from config")

    try:
        if max_concurrent > 1 and len(entities_to_process) > 1:
            logger.info(f"Running entities concurrently (max {max_concurrent} at a time)")
            semaphore = asyncio.Semaphore(max_concurrent)

            async def process_with_limit(entity_config):
                async with semaphore:
                    return await process_entity_continuous(
                        sink, token, entity_config["name"], entity_config["attributes"]
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
                        sink, token, entity_config["name"], entity_config["attributes"]
                    )
                    total += count
                except Exception as e:
                    logger.error(f"[{entity_config['name']}] Failed: {e}")
                    continue
    finally:
        sink.close()

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
