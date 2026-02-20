# Dataverse Audit Sync - Architecture & Design Document

---

## System Architecture

### High-Level Flow

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                           │
│   DATAVERSE                                                               │
│   ├─ Audit Log (Immutable, ~100,000+ records/day)                        │
│   ├─ RetrieveAuditDetails API (Field-level change tracking)              │
│   └─ Query Audits API (Paginated, 5000 records/page)                     │
│                                                                           │
└────────────────────────────────┬────────────────────────────────────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                │                │
        ┌───────▼─────┐   ┌──────▼──────┐   ┌────▼────────┐
        │   PHASE 1   │   │  PHASE 1B   │   │   PHASE 2   │
        │  BACKLOG    │   │  CATCH-UP   │   │ CONTINUOUS  │
        │  (Initial)  │   │  (Optional) │   │  (Forever)  │
        └───────┬─────┘   └──────┬──────┘   └────┬────────┘
                │                │              │
        ┌───────▼─────────────────▼──────┐      │
        │  Docker Containers (Python)    │      │
        │  • Account Container           │      │
        │  • Contact Container           │      │      ┌────────────────────┐
        │  • Case Container              │      │      │ Azure Functions    │
        │  • (Parallel processing)       │      ├─────▶│ Timer Trigger      │
        │  • 60-min windows              │      │      │ (Python)           │
        │  • 1-2 week duration           │      │      │ • async_main()     │
        │  • High throughput             │      │      │ • Every 10 min     │
        └───────┬────────────────────────┘      │      │ • Real-time data   │
                │                               │      └────────┬───────────┘
                │                               │              │
                └───────────────┬───────────────┴──────────────┤
                                │
                        ┌───────▼──────────┐
                        │    SNOWFLAKE     │
                        │                  │
                        │ Tables:          │
                        │  • audit_logs    │
                        │  • audit_details │
                        │  • sync_state    │
                        │  • metrics       │
                        └──────────────────┘
```

---

## Phase 1: Backlog Container Deployment

### Purpose
Catch up on historical audits (1-2 weeks of backlog) at high throughput.

### Architecture

```
┌─────────────────────────────────────────────────────────────┐
│  Docker Compose (Local or Azure Container Registry)         │
│                                                              │
│  ┌──────────────────┐  ┌──────────────────┐  ┌────────────┐│
│  │ Account          │  │ Contact          │  │ Case       ││
│  │ Container        │  │ Container        │  │ Container  ││
│  │                  │  │                  │  │            ││
│  │ main.py          │  │ main.py          │  │ main.py    ││
│  │ config.json      │  │ config.json      │  │ config.json││
│  │ ENTITY=Account   │  │ ENTITY=Contact   │  │ ENTITY=Case││
│  │ BACKLOG_MODE=true│  │ BACKLOG_MODE=true│  │ BACKLOG_.. ││
│  └────────┬─────────┘  └────────┬─────────┘  └────────┬───┘│
│           │                     │                    │      │
│  ┌────────▼─────────────────────▼────────────────────▼────┐ │
│  │ Shared Environment (Docker Network)                    │ │
│  │ • DATAVERSE_ORG_URL                                    │ │
│  │ • CLIENT_ID / CLIENT_SECRET (OAuth App)              │ │
│  │ • SNOWFLAKE_ACCOUNT / USER / PASSWORD                │ │
│  └────────────────────────────────────────────────────────┘ │
│                                                              │
└──────────────────────────────────────────────────────────────┘
```

### Execution Timeline (Per Container)

```
Container starts (e.g., Account)
    ↓
Load config.json (window: 60 min, entity: Account, attributes: [...])
    ↓
Get OAuth token from Entra ID (cached 55 min)
    ↓
Loop: while window_end < current_time
    ├─ Query Audits (FetchXml)
    │  └─ GET /audits?$filter=createdon ge 2026-01-20 and lt 2026-01-21
    │     → 5000 audit IDs (paginated)
    │
    ├─ Batch-Fetch Audit Details (Concurrent)
    │  └─ POST /RetrieveAuditDetails for audit IDs
    │     • Batch 1: audits 1-5 (concurrent)
    │     • Batch 2: audits 6-10 (concurrent)
    │     • Batch 3-1000: ...
    │     • Exponential backoff on 429/timeout
    │
    ├─ Atomic Insert to Snowflake
    │  └─ INSERT INTO audit_logs (audit_id, entity, changes, ...)
    │     COMMIT all or nothing
    │
    ├─ Atomic State Update
    │  └─ UPDATE sync_state SET last_sync_end = 2026-01-21
    │     WHERE entity = 'Account'
    │
    └─ Move window: window_start = 2026-01-21, window_end = 2026-01-22
       (Sleep 100ms before next batch)
       
After reaching current time:
    ↓
Container logs "Backlog complete, exiting"
    ↓
Container stops
```

### Parallelism Strategy

**Three containers run simultaneously:**
```
Account Container                Contact Container             Case Container
(Process audits 2026-01-20..22)  (Process audits ..22)        (Process audits..)

Window 1: Query + Fetch + Insert  Window 1: Query + Fetch      Window 1: Query + Fetch
(5 min)                           (3 min)                      (2 min)
         ↓                                ↓                             ↓
Window 2: Query + Fetch + Insert  Window 2: Query + Fetch      Window 2: Query + Fetch
(5 min)                           (3 min)                      (2 min)
         ↓                                ↓                             ↓
...                               ...                           ...
         │                                │                             │
         ↓                                ↓                             ↓
Complete (1-2 weeks)              Complete (1-2 weeks)         Complete (1-2 weeks)
```

**Key Detail:** Containers don't communicate; each manages its own entity independently.

---

## Phase 2: Continuous Azure Functions Deployment

### Purpose
Capture new audits in real-time (every 10 minutes, forever).

### Architecture

```
┌──────────────────────────────────────────────────┐
│         Azure Functions (Managed Service)        │
│                                                  │
│  ┌──────────────────────────────────────┐       │
│  │ Timer Trigger (Cron: 0 */10 * * * *) │       │
│  │ Fires every 10 minutes                │       │
│  └────────────┬─────────────────────────┘       │
│               │                                  │
│  ┌────────────▼──────────────────────────┐      │
│  │ function_app.py (Main Entry Point)    │      │
│  │                                        │      │
│  │ def main(mytimer: TimerRequest):      │      │
│  │   asyncio.run(async_main())           │      │
│  └────────────┬──────────────────────────┘      │
│               │                                  │
│  ┌────────────▼──────────────────────────┐      │
│  │ async_main()                           │      │
│  │                                        │      │
│  │ for entity in config["entities"]:     │      │
│  │   await process_entity(entity)        │      │
│  └────────────┬──────────────────────────┘      │
│               │                                  │
│  ┌────────────▼──────────────────────────┐      │
│  │ process_entity (Per-Entity Loop)      │      │
│  │                                        │      │
│  │ • Get lastSyncEnd from Snowflake      │      │
│  │ • Query audits (last 10 min)          │      │
│  │ • Fetch details (concurrent 5-batch)  │      │
│  │ • Insert to Snowflake                 │      │
│  │ • Update sync_state.lastSyncEnd       │      │
│  └────────────┬──────────────────────────┘      │
│               │                                  │
│  ┌────────────▼──────────────────────────┐      │
│  │ Logging to Application Insights       │      │
│  └──────────────────────────────────────┘      │
│                                                  │
└──────────────────────────────────────────────────┘
```

### Execution Timeline (Per 10-minute Cycle)

```
Timer fires (14:30:00 UTC)
    ↓
[14:30:01] Load config.json (window: 10 min, entities: Account, Contact, Case)
    ↓
[14:30:02] Load logging configuration
    ↓
[14:30:03] Get OAuth token
    ↓
[14:30:05] For Account:
    ├─ Get lastSyncEnd from Snowflake: 2026-02-20 14:20:00
    ├─ Query audits from 14:20:00 to 14:30:00 (10 min window)
    ├─ Find 500 audits
    ├─ Fetch details (batches of 5)
    ├─ Insert 500 records to Snowflake
    └─ Update lastSyncEnd = 14:30:00
    
[14:32:15] For Contact:
    ├─ Similar process, +300 audits
    
[14:34:00] For Case:
    ├─ Similar process, +200 audits
    
[14:34:30] Log: "Continuous sync completed: 1000 total records processed"
    ↓
[14:34:31] Function returns success
    ↓
[14:40:00] Timer fires again (same process)
```

**Duration:** ~4-5 minutes per cycle (well within 10-minute Azure Functions timeout)

---

## Dataverse Integration

### APIs Used

#### 1. Query Audits (Web API)

```
GET /api/data/v9.2/audits?$filter=createdon ge {start} and lt {end}&$select=auditid&$top=5000

Response:
{
  "value": [
    {"auditid": "550e8400-e29b-41d4-a716-446655440000"},
    {"auditid": "550e8400-e29b-41d4-a716-446655440001"},
    ...
  ],
  "@odata.nextLink": "..."  // Paging if > 5000
}
```

**Why:** Get all audit records for a time window

#### 2. RetrieveAuditDetails (Action)

```
POST /api/data/v9.2/RetrieveAuditDetails

{
  "auditId": "550e8400-e29b-41d4-a716-446655440000",
  "propertySet": ["name", "telephone1", "address1_city"]
}

Response:
{
  "AuditRecord": {
    "auditid": "550e8400-e29b-41d4-a716-446655440000",
    "objectid": "contact-id",
    "operation": 1,  // 1=Create, 2=Update, 3=Delete, 4=Other
    "createdon": "2026-02-20T14:30:00Z",
    "changes": {
      "name": {"OldValue": null, "NewValue": "John Doe"},
      "telephone1": {"OldValue": "555-1234", "NewValue": "555-5678"}
    }
  }
}
```

**Why:** Get field-level change details for each audit

### Resilience Patterns

#### Rate Limiting (429 Too Many Requests)

```python
for attempt in range(1, max_retries + 1):
    try:
        response = await session.post(url, ...)
        if response.status == 429:
            delay = retry_delay * (backoff_multiplier ** (attempt - 1))
            await asyncio.sleep(delay)  # Exponential backoff
            continue
```

**Dataverse limit:** ~6000 requests/min per org (can be lower depending on throttling policies)

#### Timeout Handling

```python
async with session.post(url, timeout=aiohttp.ClientTimeout(total=30)):
    # If no response in 30 sec, raise TimeoutError
    # Caught by retry loop
```

**Why:** Slow network or overloaded service

---

## Snowflake Integration

### Tables

#### 1. `audit_logs`
Core audit records.

```sql
CREATE TABLE audit_logs (
  audit_id CHAR(36) NOT NULL PRIMARY KEY,
  entity VARCHAR(50),
  changes VARIANT,  -- JSON details
  processed_at TIMESTAMP,
  run_id CHAR(36)
);
```

#### 2. `sync_state`
Per-entity progress tracking (crash recovery).

```sql
CREATE TABLE sync_state (
  entity VARCHAR(50) PRIMARY KEY,
  last_sync_end TIMESTAMP,
  record_count INT,
  updated_at TIMESTAMP
);

-- After Account finishes window 2026-02-20 14:30-15:30:
INSERT OR UPDATE sync_state SET
  last_sync_end = '2026-02-20 15:30:00',
  record_count = 5000,
  updated_at = CURRENT_TIMESTAMP();
```

**Why:** Enables resumption from exact point if container/function crashes

#### 3. `metrics` (Optional)
Performance metrics for monitoring.

```sql
CREATE TABLE metrics (
  timestamp TIMESTAMP,
  entity VARCHAR(50),
  window_minutes INT,
  record_count INT,
  fetch_time_seconds FLOAT,
  insert_time_seconds FLOAT
);
```

### Idempotent Upserts

**Problem:** If a container crashes and restarts, it reprocesses the same window. This could create duplicates in Snowflake.

**Solution:** Use MERGE (idempotent upsert)

```sql
MERGE INTO audit_logs a
USING new_audits n
ON a.audit_id = n.audit_id
WHEN MATCHED THEN UPDATE SET changes = n.changes, processed_at = n.processed_at
WHEN NOT MATCHED THEN INSERT (audit_id, entity, changes, processed_at, run_id)
  VALUES (n.audit_id, n.entity, n.changes, n.processed_at, n.run_id);
```

**Result:** Reprocessing same window = same Snowflake state (no duplicates)

---

## Configuration System

### How Config is Loaded

```python
def load_config() -> Dict:
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    
    try:
        with open(config_path, 'r') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("ERROR: config.json not found")
        sys.exit(1)
    
    return config

# Global config
CONFIG = load_config()

# Later, in code:
window_size = CONFIG["windowSizeMinutes"]["backlog"]
concurrent_fetch = CONFIG["dataverse"]["query"]["concurrent_audit_fetch"]
batch_size = CONFIG["snowflake"]["query"]["batch_insert_size"]
```

### Configuration Precedence

```
Default Values (Hardcoded)
    ↑
    │ (Overridden by)
    │
config.json Values
    ↑
    │ (Overridden by)
    │
Environment Variables
    ↑ (Single entity override)
    │
ENTITY=Account  (e.g., "Account" to process only Account)
```

---

## Data Flow Diagram

### Single Entity Processing

```
Query Audits (FetchXml)
    │
    ├─ Window: 14:00 - 15:00
    ├─ Filter: entity = Account
    ├─ Response: [audit_id_1, audit_id_2, audit_id_3, ...]  (5000 total)
    │
    ↓
Batch-Fetch Details (RetrieveAuditDetails)
    │
    ├─ Batch 1: audits 1-5 (fetch concurrently)
    ├─ Batch 2: audits 6-10 (fetch concurrently)
    ├─ ...
    └─ Batch 1000: audits 4996-5000
    
        Each audit → 
          {
            "audit_id": "xyz",
            "operation": 2,  // Update
            "changes": {
              "name": {"old": "...", "new": "..."},
              "telephone1": {"old": "...", "new": "..."}
            }
          }
    │
    ↓
Insert to Snowflake
    │
    ├─ FOR each detail:
    │     INSERT INTO audit_logs (...)
    │
    ├─ COMMIT (atomic)
    │
    └─ Result: 5000 new rows in Snowflake
    │
    ↓
Update sync_state
    │
    ├─ UPDATE sync_state SET last_sync_end = '15:00:00'
    │
    └─ Next window starts from 15:00:00
```

---

## Concurrency Model

### Async/Await Pattern (Python asyncio)

```
main()
    ├─ asyncio.run(main())  ← Starts event loop
    │
    ├─ async with aiohttp.ClientSession() as session:
    │   └─ Uses single session for connection pooling
    │
    ├─ for entity in entities:
    │   └─ await process_window(token, window_start, window_end, entity)
    │
    └─ Event Loop Handles:
        ├─ pause/resume on network I/O
        ├─ multiple concurrent requests
        └─ minimal CPU overhead
```

### Batch Concurrency

```
Audit IDs: [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, ...]

Batch 1:
  Task 1: Fetch audit 1 (start, wait for response)
  Task 2: Fetch audit 2 (start, wait for response)
  Task 3: Fetch audit 3 (start, wait for response)
  Task 4: Fetch audit 4 (start, wait for response)
  Task 5: Fetch audit 5 (start, wait for response)
  
  All 5 tasks run concurrently (not sequentially)
  Max time = max(Task1, Task2, Task3, Task4, Task5) ≈ 500ms
  (vs 5 × 500ms = 2500ms if sequential)
  
  ↓
  sleep(100ms)  ← Brief pause before next batch
  ↓

Batch 2:
  Task 6: Fetch audit 6
  Task 7: Fetch audit 7
  ...
```

### Entity Concurrency

```
If max_concurrent_entities = 3:

main()
    ├─ asyncio.run(async_main())
    │
    └─ Event Loop:
        ├─ Start Account processing (event 1, awaiting I/O)
        ├─ Start Contact processing (event 2, awaiting I/O)
        ├─ Start Case processing (event 3, awaiting I/O)
        │
        ├─ Account responds with 5000 audits (event 1 resumable)
        ├─ Contact responds with 3000 audits (event 2 resumable)
        │
        ├─ (Contact finishes, no more events)
        ├─ (Event 2 removed, wait for Event 1, 3)
        │
        ├─ Case finishes (event 3)
        │
        └─ (Wait for Account to finish)
```

---

## Failure & Recovery

### Crash Scenarios

#### Scenario 1: Container Crashes During Insert

```
Container A (Account):
    ├─ Fetch 5000 audits from 14:00-15:00
    ├─ Fetch details for all 5000
    ├─ Start INSERT into Snowflake
    ├─ Insert rows 1-2000 successfully
    ├─ CRASH (connection lost)
    └─ Rows 2001-5000 NOT inserted (rollback on transaction failure)

After Restart:
    ├─ Load config & get lastSyncEnd
    ├─ Read from sync_state: lastSyncEnd = 14:00:00 (unchanged, because INSERT wasn't committed)
    ├─ Process same window 14:00-15:00 again
    ├─ Fetch audits again
    ├─ Fetch details again
    ├─ INSERT all 5000 (rows 1-2000 are duplicates, handled by MERGE)
    └─ UPDATE lastSyncEnd = 15:00:00 (succeeds this time)
```

**Key:** Atomic transactions prevent partial inserts.

#### Scenario 2: Container Crashes After Insert But Before State Update

```
Container A:
    ├─ INSERT 5000 records to Snowflake (committed)
    ├─ About to UPDATE sync_state
    ├─ CRASH
    └─ sync_state still shows lastSyncEnd = 14:00:00

After Restart:
    ├─ Read sync_state: lastSyncEnd = 14:00:00
    ├─ Process window 14:00-15:00 again
    ├─ Fetch 5000 audits again
    ├─ Fetch details again
    ├─ MERGE INSERT: 5000 records (duplicates handled by MERGE)
    ├─ UPDATE sync_state = 15:00:00
    └─ Result: Snowflake has 5000 unique records (not 10000), state is correct
```

---

## OAuth & Authentication Flow

### Initial Token Acquisition

```python
app = msal.PublicClientApplication(
    client_id="d1234567-89ab-cdef-0123-456789abcdef",
    authority="https://login.microsoftonline.com/common"
)

token_response = app.acquire_token_by_username_password(
    username="d1234567-89ab-cdef-0123-456789abcdef",  # App ID
    password="secret_xyz",  # Client Secret
    scopes=["https://org.dynamics.com/.default"]
)

token = token_response["access_token"]
```

### Token Usage

```
GET /api/data/v9.2/audits
Header: Authorization: Bearer eyJ0eXAiOi... (token)

Response: 
{
  "value": [...]
}
```

### Token Refresh

```
Token issued: 14:30:00
Token expiry: 15:30:00 (60 min)

By design, container/function runs < 60 min, so same token reused
If longer processing needed, token refreshed automatically by MSAL
```

---

## Logging Architecture

### Multi-Level Logging

```python
logger = logging.getLogger("sync")  # Component logger

# Different levels for different situations
logger.debug("Fetching audit details for ID xyz")      # Verbose, only if DEBUG enabled
logger.info("Processed 1000 audits for Account")       # Normal operations
logger.warning("Retry 2/3 for audit xyz due to timeout") # Something unexpected
logger.error("Failed to connect to Snowflake after 3 retries") # Serious issue
logger.critical("OAuth token acquisition failed")     # Can't continue
```

### Output Destinations

```
┌─ Console Output ────────────────────────────┐     ┌─ File Output (Container) ─┐
│ Container logs (docker logs <container>)   │     │ /var/log/audit-sync.log   │
│ Function logs (Application Insights)       │     │ audit-sync.log.1          │
│                                             │     │ audit-sync.log.2 (rotated)│
│ [14:30:45] INFO: Fetched 5000 audits      │     │                           │
│ [14:31:00] WARNING: Retry 1/3...          │     └───────────────────────────┘
│ [14:31:05] INFO: Inserted 5000 records    │
└─────────────────────────────────────────────┘
```

---

## Performance Characteristics

### Throughput Estimates

| Scenario | Entities | Window | Audits/Window | Time/Window | Audits/Hour |
|----------|----------|--------|---------------|-------------|-------------|
| Conservative Continuous | 1 | 10 min | 50 | 2 min | 300 |
| Moderate Continuous | 3 | 10 min | 1500 | 4 min | 9000 |
| Aggressive Backlog | 3 | 60 min | 30000 | 10 min | 180000 |

### Latency Profile (Per Window)

```
Conservative (10-min window, 50 audits):
  Query audits:     100 ms
  Fetch details:    800 ms (5 concurrent, 160ms each)
  Insert to SF:     300 ms
  Update state:     100 ms
  ───────────────
  Total:            1.3 sec
  
Aggressive (60-min window, 30000 audits):
  Query audits:     300 ms (pagination)
  Fetch details:   30 sec (6000 concurrent audits / 5 concurrent = 1200 batches)
  Insert to SF:    15 sec (300 batch inserts)
  Update state:    100 ms
  ───────────────
  Total:           ~45 sec
```

---

## Cost Analysis

### Container Deployment (Phase 1)

**7-day backlog catch-up scenario:**

```
Compute:  Azure Container Instances
  • 3 containers × 7 days × 24 hours = 504 container-hours
  • ~$0.50/hour per container = $252 for 3 containers

Storage: Snowflake data warehouse
  • 7 days backlog × 100K audits/day = 700K audits
  • × ~2 KB per audit = 1.4 GB data
  • Snowflake: ~$5 per TB per month, so ~$0.01 for 1.4 GB

Total Phase 1: ~$250-300
```

### Function Deployment (Phase 2)

**Monthly continuous operation:**

```
Compute: Azure Functions
  • 10-minute intervals = 4,320 executions/month
  • ~100ms per execution = 432 seconds = 0.12 hours/month
  • Azure Functions: $0.20 per 1M executions = ~$0.0009

Storage: Snowflake
  • 100K audits/day × ~2 KB = ~200 MB/day
  • 30 days = 6 GB/month
  • Snowflake: ~$0.03 per month

Total Phase 2: ~$0.03/month (negligible)
```

---

## Summary Table

| Aspect | Details |
|--------|---------|
| **Architecture** | Microservices: Containers (backlog) + Functions (continuous) |
| **Language** | Python 3.11 with asyncio for concurrency |
| **Dataverse APIs** | Query Audits (Web API) + RetrieveAuditDetails (Action) |
| **Concurrency Model** | Async/await with batch concurrency (5 concurrent audit fetches) |
| **State Management** | Per-entity lastSyncEnd in sync_state table (enables crash recovery) |
| **Data Consistency** | Atomic inserts + idempotent upserts (MERGE) prevents duplicates |
| **Configurability** | All parameters via config.json (connections, logging, performance) |
| **Resilience** | Exponential backoff retry, transient error handling, atomic transactions |
| **Monitoring** | Component-level logging, metrics, dry-run mode for testing |
| **Cost** | ~$250-300 for backlog, < $0.05/month for continuous |

