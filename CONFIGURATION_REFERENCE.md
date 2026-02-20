# Dataverse Audit Sync - Configuration & Architecture Reference

**Last Updated:** February 2026  
**Version:** 2.0 (Python + Snowflake with Full Configurability)

---

## Table of Contents
1. [Configuration Schema](#configuration-schema)
2. [Multi-Entity Processing](#multi-entity-processing)
3. [Architecture & Job Execution](#architecture--job-execution)
4. [Configuration Examples](#configuration-examples)
5. [Performance Tuning Guide](#performance-tuning-guide)
6. [Deployment Modes](#deployment-modes)

---

## Configuration Schema

### Complete config.json Structure

```json
{
  "windowSizeMinutes": {
    "backlog": 60,
    "continuous": 10
  },
  "entities": [
    {
      "name": "Account",
      "attributes": ["name", "telephone1", "address1_city"]
    }
  ],
  "dataverse": {
    "api": {
      "version": "v9.2",
      "timeout_seconds": 30,
      "max_retries": 3,
      "retry_delay_seconds": 1
    },
    "auth": {
      "authority_url": "https://login.microsoftonline.com/common",
      "token_cache_minutes": 55,
      "scope": "https://org.dynamics.com/.default"
    },
    "query": {
      "page_size": 5000,
      "concurrent_audit_fetch": 5,
      "batch_size_details": 5
    }
  },
  "snowflake": {
    "connection": {
      "timeout_seconds": 30,
      "max_retries": 3,
      "retry_delay_seconds": 2
    },
    "query": {
      "batch_insert_size": 100,
      "max_pool_size": 10,
      "min_pool_size": 1
    }
  },
  "performance": {
    "max_concurrent_entities": 3,
    "thread_pool_size": 10,
    "rate_limit_requests_per_second": 100,
    "backoff_multiplier": 2.0,
    "max_backoff_seconds": 30
  },
  "logging": {
    "level": "INFO",
    "format": "[%(asctime)s] %(levelname)s: %(message)s",
    "date_format": "%Y-%m-%d %H:%M:%S",
    "output": {
      "console": true,
      "file": {
        "enabled": true,
        "path": "/var/log/audit-sync.log",
        "max_size_mb": 100,
        "backup_count": 5,
        "level": "DEBUG"
      }
    },
    "components": {
      "dataverse": "INFO",
      "snowflake": "INFO",
      "auth": "INFO",
      "sync": "DEBUG"
    }
  },
  "features": {
    "enable_state_tracking": true,
    "enable_idempotent_upserts": true,
    "enable_crash_recovery": true,
    "enable_metrics": true,
    "dry_run": false
  },
  "monitoring": {
    "emit_metrics_interval_seconds": 60,
    "log_progress_every_records": 1000,
    "health_check_interval_seconds": 300
  }
}
```

---

## Configuration Details

### `windowSizeMinutes` Section
Controls how much historical data is fetched in each processing window.

| Setting | Backlog | Continuous | Purpose |
|---------|---------|-----------|---------|
| `backlog` | 60 min | - | **Phase 1**: Process 1-2 weeks of historical audits in 60-min chunks |
| `continuous` | - | 10 min | **Phase 2**: Process only new audits every 10 minutes |

**Why different windows?**
- **Backlog (60 min)**: Larger windows = fewer API calls, but can handle slower catch-up from 1-2 weeks ago
- **Continuous (10 min)**: Smaller windows = more frequent, real-time audits with minimal latency

---

### `entities` Section
Defines which Dataverse entities to track and which attributes to capture.

```json
"entities": [
  {
    "name": "Account",
    "attributes": ["name", "telephone1", "address1_city", "address1_country"]
  },
  {
    "name": "Contact",
    "attributes": ["fullname", "emailaddress1", "mobilephone", "address1_city"]
  },
  {
    "name": "Case",
    "attributes": ["title", "description", "prioritycode", "statuscode"]
  }
]
```

**Key Points:**
- **Multiple entities**: Process **3, 5, 10, or more** entities in the same job
- **Per-entity attributes**: Each entity can track different fields
- **Order**: Entities are processed sequentially by default (or concurrently if `max_concurrent_entities > 1`)

---

### `dataverse` Section

#### `api` Subsection
Dataverse Web API v9.2 connection settings.

| Setting | Default | Notes |
|---------|---------|-------|
| `version` | `v9.2` | Dataverse API version (e.g., v8.2, v9.0, v9.2) |
| `timeout_seconds` | 30 | HTTP request timeout; increase for slow networks |
| `max_retries` | 3 | Retry attempts for transient failures (429, 500, etc.) |
| `retry_delay_seconds` | 1 | Initial delay before first retry (multiplied by backoff_multiplier) |

**Example: Aggressive Retry for Unreliable Networks**
```json
"api": {
  "timeout_seconds": 60,
  "max_retries": 5,
  "retry_delay_seconds": 2
}
```

#### `auth` Subsection
OAuth 2.0 token acquisition settings.

| Setting | Default | Notes |
|---------|---------|-------|
| `authority_url` | `https://login.microsoftonline.com/common` | Microsoft Entra ID endpoint |
| `token_cache_minutes` | 55 | Token validity; recommend 55 (tokens valid 60 min) |
| `scope` | `https://org.dynamics.com/.default` | Dataverse API scope |

**Why 55 minutes?**
- Azure tokens are valid for 60 minutes
- Refreshing at 55 min ensures no mid-operation token expiration

#### `query` Subsection
Dataverse query behavior.

| Setting | Default | Notes |
|---------|---------|-------|
| `page_size` | 5000 | Max records per Web API page; Dataverse max is 5000 |
| `concurrent_audit_fetch` | 5 | How many audit details to fetch simultaneously (per batch) |
| `batch_size_details` | 5 | *(Informational)* - documented concurrency level |

**How Batches Work:**
```
Query Audits → [5000 audit IDs]
                    ↓
              Batch 1: fetch details for audits 1-5 (concurrently)
              Batch 2: fetch details for audits 6-10 (concurrently)
              Batch 3: fetch details for audits 11-15 (concurrently)
              ...
```

---

### `snowflake` Section

#### `connection` Subsection
Snowflake connection pooling and timeouts.

| Setting | Default | Notes |
|---------|---------|-------|
| `timeout_seconds` | 30 | Connection timeout |
| `max_retries` | 3 | Connection retry attempts |
| `retry_delay_seconds` | 2 | Initial delay before retry |

#### `query` Subsection
Snowflake bulk insert and pooling behavior.

| Setting | Default | Notes |
|---------|---------|-------|
| `batch_insert_size` | 100 | Records per INSERT statement (Snowflake statement limit >10k) |
| `max_pool_size` | 10 | Max idle connections in pool |
| `min_pool_size` | 1 | Min idle connections in pool |

**Tuning for High Volume:**
```json
"query": {
  "batch_insert_size": 500,
  "max_pool_size": 20,
  "min_pool_size": 5
}
```

---

### `performance` Section
Controls concurrency, rate limiting, and backoff behavior.

| Setting | Default | Purpose |
|---------|---------|---------|
| `max_concurrent_entities` | 3 | Max entities processed simultaneously |
| `thread_pool_size` | 10 | Max async tasks in flight |
| `rate_limit_requests_per_second` | 100 | Requests/sec throttle (if needed) |
| `backoff_multiplier` | 2.0 | Exponential backoff multiplier (1s → 2s → 4s → 8s) |
| `max_backoff_seconds` | 30 | Cap on individual retry delay |

**Exponential Backoff Calculation:**
```
Attempt 1: delay = 1 × 2^0 = 1 sec
Attempt 2: delay = 1 × 2^1 = 2 sec
Attempt 3: delay = 1 × 2^2 = 4 sec (capped at 30 sec if needed)
```

**For Conservative Continuous Sync (Azure Functions):**
```json
"performance": {
  "max_concurrent_entities": 1,
  "thread_pool_size": 3,
  "rate_limit_requests_per_second": 50,
  "backoff_multiplier": 1.5,
  "max_backoff_seconds": 15
}
```

**For Aggressive Backlog (Container):**
```json
"performance": {
  "max_concurrent_entities": 3,
  "thread_pool_size": 15,
  "rate_limit_requests_per_second": 200,
  "backoff_multiplier": 2.0,
  "max_backoff_seconds": 30
}
```

---

### `logging` Section

#### Global Logging Configuration
```json
"logging": {
  "level": "INFO",
  "format": "[%(asctime)s] %(levelname)s: %(message)s",
  "date_format": "%Y-%m-%d %H:%M:%S"
}
```

| Setting | Options | Purpose |
|---------|---------|---------|
| `level` | DEBUG, INFO, WARNING, ERROR, CRITICAL | Global log level |
| `format` | Python format string | Log message format |
| `date_format` | Python date format | Timestamp format |

#### Console Output
```json
"output": {
  "console": true
}
```
- **true**: Logs to stdout/stderr (visible in container logs, Azure Functions portal)
- **false**: Disable console logging

#### File Logging with Rotation
```json
"output": {
  "file": {
    "enabled": true,
    "path": "/var/log/audit-sync.log",
    "max_size_mb": 100,
    "backup_count": 5,
    "level": "DEBUG"
  }
}
```

**File Rotation Behavior:**
```
Day 1: audit-sync.log (grows to 100 MB)
       ↓ (rotates)
Day 2: audit-sync.log (new file)
       audit-sync.log.1 (backup 1)
       +
Day 5: audit-sync.log (new file)
       audit-sync.log.1 (backups rotate)
       audit-sync.log.2
       audit-sync.log.3
       audit-sync.log.4
       audit-sync.log.5
       (older backups deleted)
```

#### Component-Level Logging
```json
"components": {
  "dataverse": "INFO",
  "snowflake": "INFO",
  "auth": "INFO",
  "sync": "DEBUG"
}
```

**Use Case:** Set individual components to DEBUG for troubleshooting:
```json
"components": {
  "dataverse": "DEBUG",  ← Verbose Dataverse API logs
  "snowflake": "INFO",   ← Normal Snowflake logs
  "auth": "INFO",
  "sync": "DEBUG"
}
```

---

### `features` Section
Feature flags for advanced behavior.

| Setting | Default | Purpose |
|---------|---------|---------|
| `enable_state_tracking` | true | Track lastSyncEnd per entity (enables crash recovery) |
| `enable_idempotent_upserts` | true | Use MERGE on Snowflake (prevents duplicates) |
| `enable_crash_recovery` | true | Resume from last successful sync time on restart |
| `enable_metrics` | true | Emit performance metrics to logs |
| `dry_run` | false | Log what would happen without writing to Snowflake |

**Dry-Run Mode Example:**
```json
"features": {
  "dry_run": true
}
```
- Fetches audits from Dataverse
- Logs "Would insert X records to Snowflake"
- Does NOT actually write to Snowflake
- Useful for testing configuration before production

---

### `monitoring` Section
Observability and metrics configuration.

| Setting | Default | Purpose |
|---------|---------|---------|
| `emit_metrics_interval_seconds` | 60 | How often to log performance metrics |
| `log_progress_every_records` | 1000 | Log progress after every N records processed |
| `health_check_interval_seconds` | 300 | Health check interval (if implemented) |

**Example Log Output with Progress Monitoring:**
```
[2026-02-20 14:30:00] INFO: [Account] Processing window 2026-02-19 14:00:00 to 2026-02-19 15:00:00
[2026-02-20 14:30:15] INFO: [Account] Fetched 5000 audits for window
[2026-02-20 14:30:45] INFO: [Account] Progress: 1000 audits fetched
[2026-02-20 14:31:15] INFO: [Account] Progress: 2000 audits fetched
[2026-02-20 14:31:45] INFO: [Account] Progress: 3000 audits fetched
[2026-02-20 14:32:15] INFO: [Account] Progress: 4000 audits fetched
[2026-02-20 14:32:45] INFO: [Account] Progress: 5000 audits fetched
[2026-02-20 14:33:00] INFO: [Account] Inserted 5000 audits to Snowflake
[2026-02-20 14:33:05] INFO: [Account] State updated: lastSyncEnd=2026-02-19 15:00:00
```

---

## Multi-Entity Processing

### How Multiple Entities Are Configured

**Example: 3 Entities with Different Attributes**

```json
"entities": [
  {
    "name": "Account",
    "attributes": ["name", "telephone1", "address1_city", "parentaccountid"]
  },
  {
    "name": "Contact",
    "attributes": ["fullname", "emailaddress1", "mobilephone", "accountid"]
  },
  {
    "name": "Case",
    "attributes": ["title", "description", "prioritycode", "statuscode", "caseorigincode"]
  }
]
```

### Execution Flow: Sequential vs. Concurrent

#### Sequential Mode (default, `max_concurrent_entities: 1`)

```
Start Job
    ↓
Process Account (60-min window)
    ├─ Query audits: 5000
    ├─ Fetch details: 5 concurrent batches
    ├─ Insert to Snowflake: 5000 records
    ├─ Update state
    └─ Complete: 5 min
    ↓
Process Contact (60-min window)
    ├─ Query audits: 3000
    ├─ Fetch details: 600 concurrent batches (3000/5 batches)
    ├─ Insert to Snowflake: 3000 records
    └─ Complete: 3 min
    ↓
Process Case (60-min window)
    ├─ Query audits: 2000
    ├─ Fetch details: 400 concurrent batches
    ├─ Insert to Snowflake: 2000 records
    └─ Complete: 2 min
    ↓
Total Time: ~10 min
Job End
```

#### Concurrent Mode (`max_concurrent_entities: 3`)

```
Start Job
    ↓
Concurrent Processing:
  Account          Contact           Case
  (5 min)          (3 min)           (2 min)
                                         ↓
                                    Complete (earliest)
                                        ↓
                                      (wait for others)
                                        ↓
                                Wait (Contact finishes at 3 min)
                                        ↓
                                Wait (Account finishes at 5 min)
                                        ↓
Total Time: ~5 min (max of individual times)
Job End
```

**Key Insight:** With `max_concurrent_entities: 3`, three entities are processed *simultaneously*, reducing total runtime from 10 min → 5 min.

### Entity-Level State Tracking

Each entity maintains its own `lastSyncEnd` in Snowflake's `sync_state` table:

```sql
SELECT * FROM sync_state;

| entity  | last_sync_end           | record_count | updated_at          |
|---------|-------------------------|--------------|---------------------|
| Account | 2026-02-19 20:00:00 UTC | 5000         | 2026-02-19 20:05:00 |
| Contact | 2026-02-19 20:00:00 UTC | 3000         | 2026-02-19 20:03:00 |
| Case    | 2026-02-19 20:00:00 UTC | 2000         | 2026-02-19 20:02:00 |
```

**Benefit:** If the sync crashes:
- Account restarts from 20:00 UTC (atomic checkpoint)
- Contact and Case similarly resume independently
- No duplicate records thanks to idempotent upserts

---

## Architecture & Job Execution

### Two-Phase Architecture Overview

```
┌─────────────────────────────────────────────────────┐
│                    DATAVERSE AUDITS                  │
│         (Historical + Continuous Stream)             │
└──────────────────────┬──────────────────────────────┘
                       │
          ┌────────────┼────────────┐
          │                         │
    ┌─────▼──────┐           ┌──────▼──────┐
    │  PHASE 1   │           │  PHASE 2    │
    │  BACKLOG   │           │ CONTINUOUS  │
    │  (1-2 wks) │           │  (Forever)  │
    └─────┬──────┘           └──────┬──────┘
          │                         │
    ┌─────▼──────────────┐    ┌──────▼──────────────┐
    │ Azure Container    │    │ Azure Functions     │
    │ Instance (ACI)     │    │ Timer Trigger       │
    │                    │    │                     │
    │ • 60-min windows   │    │ • 10-min frequency  │
    │ • 3-5 containers   │    │ • Single instance   │
    │ • Parallel by      │    │ • Real-time capture │
    │   entity           │    │ • Lightweight       │
    │ • Runs 1-2 weeks   │    │ • Runs forever      │
    └─────┬──────────────┘    └──────┬──────────────┘
          │                         │
          └────────────┬────────────┘
                       │
                ┌──────▼───────┐
                │  SNOWFLAKE   │
                │              │
                │ • audit_logs │
                │ • sync_state │
                │ • metrics    │
                └──────────────┘
```

### Phase 1: Backlog (Container Deployment)

**When?** One-time, when implementing audit sync for the first time
**Duration:** 1-2 weeks (depending on historical data depth)
**Scale:** 3-5 parallel Docker containers (one per entity)

**How It Works:**

```
container-deployment/
├── main.py (520 lines)
├── requirements.txt
├── Dockerfile (Python 3.11)
├── docker-compose.yml (3 services: Account, Contact, Case)
├── config.json
└── .env (secrets: CLIENT_ID, CLIENT_SECRET, SNOWFLAKE_*)

Execution Flow (backlog, BACKLOG_MODE=true):
    1. Docker Compose starts 3 containers
    2. Account container:
       - WINDOW_SIZE_MINUTES = 60 (from config)
       - Loop until now:
         - Fetch audits from 2026-01-20 to 2026-01-21
         - Fetch details for each audit (concurrent 5-batch)
         - Insert to Snowflake
         - Update sync_state.last_sync_end = 2026-01-21
         - Sleep briefly
    3. Contact & Case containers run similarly and independently
    4. All three containers run in parallel (same network)
    5. After 1-2 weeks, all containers stop (no more historical data)
```

**Why Containers?**
- Parallel processing by entity (Account, Contact, Case run simultaneously)
- Can deploy to multiple machines if needed
- High throughput needed to catch up on historical data
- Run locally in Docker Compose for testing, or in Azure Container Instances for production

**Configuration for Backlog:**
```json
{
  "windowSizeMinutes": {
    "backlog": 60
  },
  "performance": {
    "max_concurrent_entities": 3,
    "thread_pool_size": 15,
    "backoff_multiplier": 2.0
  }
}
```

Environment Variable: `BACKLOG_MODE=true`

### Phase 2: Continuous (Azure Functions)

**When?** After backlog is complete (or in parallel on a second schedule)
**Duration:** Forever (runs indefinitely)
**Scale:** Single lightweight Azure Functions instance

**How It Works:**

```
function-app-deployment/
├── function_app.py (375 lines)
├── audit_sync_timer.py (timer trigger)
├── requirements.txt
├── host.json (timeout: 10 min)
├── local.settings.json
├── config.json
└── (Deployed to Azure Functions)

Execution Flow (continuous, BACKLOG_MODE=false):
    1. Timer trigger fires every 10 minutes (cron: 0 */10 * * * *)
    2. async_main() called:
       - WINDOW_SIZE_MINUTES = 10 (from config)
       - For each entity in config["entities"]:
         - Get lastSyncEnd from Snowflake (e.g., 2026-02-20 14:30)
         - Fetch audits from 2026-02-20 14:30 to 2026-02-20 14:40 (10 min window)
         - Fetch details concurrently (5-batch)
         - Insert to Snowflake
         - Update sync_state.last_sync_end = 2026-02-20 14:40
       - Log: "Continuous sync completed: N total records"
    3. Function completes
    4. Azure Functions waits 10 minutes
    5. Timer fires again, repeat
```

**Why Azure Functions?**
- Fully managed, no infrastructure to maintain
- Serverless: pay only for execution time
- Lightweight: ~100ms overhead per 10-min interval
- Built-in logging to Application Insights
- Auto-scales (though usually just 1 instance needed for continuous)

**Configuration for Continuous:**
```json
{
  "windowSizeMinutes": {
    "continuous": 10
  },
  "performance": {
    "max_concurrent_entities": 3,
    "thread_pool_size": 5,
    "backoff_multiplier": 1.5
  }
}
```

Environment Variable: `BACKLOG_MODE=false`

---

### Single Job Processing Flow (Any Mode)

Here's the internal flow for processing ONE entity in ONE window:

```
process_window(token, window_start, window_end, entity, attributes)
    ↓
1. Log window start
   [Account] Processing window 2026-02-20 14:30:00 to 2026-02-20 15:30:00
   ↓
2. Query Audits (FetchXml)
   GET /api/data/v9.2/audits?$filter=createdon ge ... AND createdon lt ...
   Response: 5000 audit IDs
   ↓
3. Batch-Fetch Audit Details
   Batch 1: Fetch details for audit IDs 1-5 (concurrent)
   Batch 2: Fetch details for audit IDs 6-10 (concurrent)
   ...
   POST /api/data/v9.2/RetrieveAuditDetails for each audit
   → Implement exponential backoff on 429, 500, timeout
   ↓
4. Insert to Snowflake (Atomic Transaction)
   FOR each detail:
     INSERT INTO audits (audit_id, entity, changes, processed_at, run_id)
   COMMIT (all or nothing)
   ↓
5. Update Sync State (Atomic)
   MERGE INTO sync_state SET last_sync_end = window_end, record_count = 5000
   ↓
6. Log completion
   [Account] Inserted 5000 audits to Snowflake
   [Account] State updated: lastSyncEnd=2026-02-20 15:30:00
```

**Key Properties:**

| Property | Benefit |
|----------|---------|
| **Atomic Insert** | Either all 5000 records inserted or none (no partial writes) |
| **Atomic State Update** | State only updated AFTER successful Snowflake insert |
| **Crash Resilient** | If container crashes mid-insert, next run fetches same window again |
| **Idempotent** | Replaying same window = same Snowflake state (MERGE handles duplicates) |
| **Concurrent Batches** | 5 audit details fetched simultaneously (not 5000 sequential) |

---

## Configuration Examples

### Example 1: Conservative Continuous (Azure Functions)

Minimal resource usage, gradual backlog catch-up:

```json
{
  "windowSizeMinutes": {
    "backlog": 30,
    "continuous": 10
  },
  "entities": [
    {"name": "Account", "attributes": ["name", "telephone1"]},
    {"name": "Contact", "attributes": ["fullname", "emailaddress1"]}
  ],
  "dataverse": {
    "api": {
      "version": "v9.2",
      "timeout_seconds": 30,
      "max_retries": 3,
      "retry_delay_seconds": 1
    },
    "query": {
      "page_size": 5000,
      "concurrent_audit_fetch": 2
    }
  },
  "snowflake": {
    "connection": {
      "timeout_seconds": 30,
      "max_retries": 2,
      "retry_delay_seconds": 2
    },
    "query": {
      "batch_insert_size": 50,
      "max_pool_size": 2,
      "min_pool_size": 1
    }
  },
  "performance": {
    "max_concurrent_entities": 1,
    "thread_pool_size": 3,
    "backoff_multiplier": 1.5,
    "max_backoff_seconds": 15
  },
  "logging": {
    "level": "INFO",
    "output": {
      "console": true,
      "file": {"enabled": false}
    }
  },
  "features": {
    "dry_run": false
  }
}
```

**Behavior:**
- 1 entity at a time (sequential)
- 2 concurrent audit detail fetches (vs 5)
- 10-minute windows for continuous
- Minimal logging (INFO level only)
- Lightweight Snowflake usage

### Example 2: Aggressive Backlog (Container)

Fast catch-up, high throughput:

```json
{
  "windowSizeMinutes": {
    "backlog": 60,
    "continuous": 10
  },
  "entities": [
    {"name": "Account", "attributes": ["name", "telephone1", "address1_city", "parentaccountid"]},
    {"name": "Contact", "attributes": ["fullname", "emailaddress1", "mobilephone", "accountid"]},
    {"name": "Case", "attributes": ["title", "description", "prioritycode", "statuscode", "caseorigincode"]},
    {"name": "Opportunity", "attributes": ["name", "value", "stagecode", "accountid"]}
  ],
  "dataverse": {
    "api": {
      "version": "v9.2",
      "timeout_seconds": 60,
      "max_retries": 5,
      "retry_delay_seconds": 2
    },
    "query": {
      "page_size": 5000,
      "concurrent_audit_fetch": 10
    }
  },
  "snowflake": {
    "connection": {
      "timeout_seconds": 45,
      "max_retries": 3,
      "retry_delay_seconds": 3
    },
    "query": {
      "batch_insert_size": 500,
      "max_pool_size": 20,
      "min_pool_size": 5
    }
  },
  "performance": {
    "max_concurrent_entities": 4,
    "thread_pool_size": 20,
    "rate_limit_requests_per_second": 200,
    "backoff_multiplier": 2.0,
    "max_backoff_seconds": 30
  },
  "logging": {
    "level": "DEBUG",
    "output": {
      "console": true,
      "file": {
        "enabled": true,
        "path": "/var/log/audit-sync.log",
        "max_size_mb": 200,
        "backup_count": 10,
        "level": "DEBUG"
      }
    },
    "components": {
      "dataverse": "DEBUG",
      "snowflake": "DEBUG"
    }
  },
  "monitoring": {
    "emit_metrics_interval_seconds": 30,
    "log_progress_every_records": 500
  },
  "features": {
    "dry_run": false
  }
}
```

**Behavior:**
- 4 entities processed simultaneously
- 10 concurrent audit detail fetches (aggressive)
- 60-minute windows (catch up fast on backlog)
- Full DEBUG logging to console and file
- High Snowflake usage but fast throughput

### Example 3: Testing with Dry-Run

Validate configuration before production:

```json
{
  "windowSizeMinutes": {
    "backlog": 10,
    "continuous": 5
  },
  "entities": [
    {"name": "Account", "attributes": ["name"]},
    {"name": "Contact", "attributes": ["fullname"]}
  ],
  "dataverse": {
    "api": {
      "timeout_seconds": 30,
      "max_retries": 3,
      "retry_delay_seconds": 1
    }
  },
  "features": {
    "dry_run": true
  }
}
```

**Behavior:**
- Queries Dataverse normally
- Fetches audit details normally
- **SKIPS** Snowflake inserts (only logs "Would insert X records")
- Useful for validating configuration and Dataverse connectivity

---

## Performance Tuning Guide

### Scenario 1: High Volume, Many Audits per Window

**Problem:** Processing 10,000+ audits per entity per window takes too long

**Tuning:**
```json
"dataverse": {
  "query": {
    "concurrent_audit_fetch": 15
  }
},
"performance": {
  "max_concurrent_entities": 3,
  "thread_pool_size": 30,
  "backoff_multiplier": 2.0
},
"snowflake": {
  "query": {
    "batch_insert_size": 1000,
    "max_pool_size": 15
  }
}
```

**Impact:**
- 15 concurrent audit fetches (vs default 5) = 3x faster detail fetching
- 30 thread pool (vs default 10) = handles more concurrent work
- 1000-record batches (vs 100) = fewer INSERT calls

### Scenario 2: Transient Network Errors (429, timeout)

**Problem:** Frequent "too many requests" or timeout errors

**Tuning:**
```json
"dataverse": {
  "api": {
    "max_retries": 7,
    "retry_delay_seconds": 2
  }
},
"performance": {
  "backoff_multiplier": 2.5,
  "max_backoff_seconds": 60
}
```

**Impact:**
- Retry 7 times (vs 3) with slower backoff (1s → 2s → 4s → 8s → 16s → 32s → 60s)
- Total: Up to ~3.5 minutes of retry window (vs 7 seconds)

### Scenario 3: Snowflake Throttling

**Problem:** Snowflake rejects inserts: "Resource exhausted"

**Tuning:**
```json
"performance": {
  "rate_limit_requests_per_second": 25
},
"snowflake": {
  "connection": {
    "max_retries": 5
  },
  "query": {
    "batch_insert_size": 50,
    "max_pool_size": 3
  }
}
```

**Impact:**
- Limit Dataverse API calls to 25/sec (vs 100)
- Smaller batches (50 vs 100) = less Snowflake load per transaction
- Smaller connection pool = fewer simultaneous connections

### Scenario 4: Minimize Azure Functions Runtime (Cost)

**Problem:** Azure Functions running too long, costing money

**Tuning:**
```json
"windowSizeMinutes": {
  "continuous": 15
},
"entities": [
  {"name": "Account", "attributes": ["name"]}
],
"dataverse": {
  "query": {
    "concurrent_audit_fetch": 10
  }
},
"snowflake": {
  "query": {
    "batch_insert_size": 200
  }
}
```

**Impact:**
- Only 1 entity (vs 3) = 3x shorter execution
- 15-min window (vs 10) = fewer function invocations, still real-time
- Aggressive concurrency = finish faster

---

## Deployment Modes

### Local Docker Compose (Development)

```bash
docker-compose up -d
```

Runs 3 containers (Account, Contact, Case) locally. Useful for testing config changes.

```yaml
services:
  account:
    environment:
      ENTITY: Account
      BACKLOG_MODE: "true"
  contact:
    environment:
      ENTITY: Contact
      BACKLOG_MODE: "true"
  case:
    environment:
      ENTITY: Case
      BACKLOG_MODE: "true"
```

### Azure Container Instances (Backlog Production)

```bash
az container create \
  --resource-group mygroup \
  --name audit-sync-account \
  --image myregistry.azurecr.io/audit-sync:latest \
  --environment-variables ENTITY=Account BACKLOG_MODE=true \
  --cpu 2 --memory 1
```

Runs in Azure, can be scaled to multiple containers (one per entity).

### Azure Functions (Continuous Production)

```bash
func azure functionapp publish myauditsyncrction
```

Deploys to Azure Functions. Timer trigger automatically runs every 10 minutes (configurable).

---

## Troubleshooting

### Q: How do I run a single entity instead of all?

**A:** Set environment variable `ENTITY`:
```bash
export ENTITY=Account
python main.py
```
Config.json can still list all entities; only the specified entity will run.

### Q: How do I test configuration before production?

**A:** Use `dry_run` mode:
```json
"features": {
  "dry_run": true
}
```

### Q: Why is sync slow?

**A:** Check:
1. **Audit volume:** Check Snowflake `SELECT COUNT(*) FROM audits` for recent window
2. **Concurrent fetches:** Increase `concurrent_audit_fetch` in config
3. **Thread pool:** Increase `thread_pool_size` if CPU is available
4. **Batch size:** Increase `batch_insert_size` for Snowflake

### Q: Why do I see duplicate records?

**A:** Shouldn't happen if `enable_idempotent_upserts: true`. Check Snowflake for duplicate `audit_id` values; if so, audit table may need `MERGE` logic instead of `INSERT`.

### Q: Can I modify config without restarting?

**A:** Container: No, must restart container
Function: No, must redeploy (or manually update `config.json` in deployment package, requires restart)

---

## Summary

| Aspect | Details |
|--------|---------|
| **Multiple Entities** | Yes, 3-10+ entities per config, process concurrently or sequentially |
| **Same Job on Multiple Entities** | Yes, config["entities"] loop processes each; concurrent if max_concurrent_entities > 1 |
| **Built With** | Python 3.11, aiohttp (async), Snowflake connector, MSAL (OAuth) |
| **Architecture** | Two-phase: Phase 1 (backlog, containers) + Phase 2 (continuous, functions) |
| **State Tracking** | Per-entity lastSyncEnd in Snowflake, enables crash recovery |
| **Configurability** | All parameters tunable via config.json (connections, logging, performance) |

---

**For more information, see:**
- [container-deployment-python/README.md](container-deployment-python/README.md) - Container deployment guide
- [function-app-deployment-python/README.md](function-app-deployment-python/README.md) - Azure Functions deployment guide
- [LICENSE.md](LICENSE.md) - MIT License and disclaimer
