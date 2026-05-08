# Dataverse Audit Sync - Unified Deployment

**One codebase. Three deployment options. Zero rework.**

This is a single, adaptive sync solution that:
- Catches up on backlog (history) automatically when first started
- Switches to live mode once current
- Runs continuously, indefinitely
- Can be hosted **any way you like** — pick what fits your environment

---

## Pick Your Deployment

### Option 1: Console / Background Job (simplest)
```bash
pip install -r requirements.txt
cp .env.example .env   # fill in your secrets
python main.py
```
Use when: you have a VM, server, or systemd you want to run it on.

### Option 2: Docker Container
```bash
cp .env.example .env   # fill in your secrets
docker compose up -d
```
Use when: you want isolated, portable, restartable container. Works locally or in Azure Container Apps / Container Instances / Kubernetes.

### Option 3: Azure Function App
```bash
func azure functionapp publish <your-function-app-name>
```
Use when: you want fully managed, serverless, pay-per-execution. Triggered every 10 min by default.

**All three options run the EXACT SAME `main.py` code.** The only difference is the host.

---

## How It Works (Adaptive Mode)

The sync inspects `sync_state` in Snowflake on startup and decides what to do:

```
Last sync was 7 days ago?  →  BACKLOG mode (60-min windows, fast catch-up)
Last sync was 5 minutes ago? →  LIVE mode (10-min windows, gentle)
No prior sync?             →  Backfill from OVERRIDE_START_TIME or last 1 hour
```

This means **one deployment** handles everything:
- Initial backfill of weeks/months of history
- Smooth transition to real-time as it catches up
- Continuous live sync once current

You don't need separate "backlog" and "continuous" jobs anymore.

---

## Visual: Same Code, Three Hosts

```
                ┌──────────────────┐
                │     main.py      │  ← The sync logic (single source of truth)
                │   (config.json)  │
                └────────┬─────────┘
                         │
        ┌────────────────┼────────────────┐
        │                │                │
   ┌────▼─────┐    ┌─────▼──────┐    ┌────▼──────┐
   │ Console  │    │  Docker    │    │ Azure     │
   │   Job    │    │ Container  │    │ Function  │
   │          │    │            │    │           │
   │ python   │    │ docker run │    │ Timer     │
   │ main.py  │    │            │    │ trigger   │
   │          │    │            │    │           │
   │ Runs     │    │ Runs       │    │ Runs      │
   │ forever  │    │ forever    │    │ every 10m │
   │          │    │            │    │ exits     │
   └──────────┘    └────────────┘    └───────────┘
```

The code adapts:
- **Long-running hosts** (console, container): catches up backlog → loops in live mode
- **Function app**: catches up what it can in 10 min → exits → next trigger continues

---

## Files in This Folder

| File | Purpose |
|------|---------|
| `main.py` | The unified sync logic — works for ALL hosts |
| `config.json` | All tunable parameters (entities, windows, performance, logging) |
| `function_app.py` | Thin Azure Functions wrapper (re-uses main.py) |
| `requirements.txt` | Python dependencies |
| `Dockerfile` | Container image build (Python 3.11 slim) |
| `docker-compose.yml` | Local Docker orchestration |
| `host.json` | Azure Functions host config |
| `local.settings.json` | Azure Functions local secrets |
| `.env.example` | Console/container secrets template |

---

## Setup (Once)

### 1. Create Snowflake tables
```sql
CREATE TABLE IF NOT EXISTS audit_logs (
    audit_id VARCHAR(36) PRIMARY KEY,
    entity VARCHAR(50),
    changes VARIANT,
    processed_at TIMESTAMP_NTZ,
    run_id VARCHAR(36)
);

CREATE TABLE IF NOT EXISTS sync_state (
    entity VARCHAR(50) PRIMARY KEY,
    last_sync_end TIMESTAMP_NTZ,
    record_count NUMBER,
    updated_at TIMESTAMP_NTZ
);
```

### 2. Register an Azure Entra ID app
- Grant it the `Dynamics CRM` `user_impersonation` permission
- Create a client secret
- Note the `client_id` and `client_secret`

### 3. Edit `config.json`
- List the entities you want to track
- Specify which attributes per entity
- Tune performance parameters (defaults are fine for most cases)

### 4. Set environment variables
- Copy `.env.example` to `.env` (for console/container)
- OR edit `local.settings.json` (for Azure Functions local testing)
- OR set in Azure Portal Configuration (for deployed Function App)

---

## Run It

### As a console job
```bash
python main.py
```
Output:
```
[2026-05-08 10:00:00] INFO: Dataverse Audit Sync - Unified Deployment
[2026-05-08 10:00:01] INFO: OAuth token acquired successfully
[2026-05-08 10:00:02] INFO: [Account] Starting from lastSyncEnd=2026-04-25T00:00:00
[2026-05-08 10:00:02] INFO: [Account] Mode=BACKLOG, lag=18720.0min, window=60min
[2026-05-08 10:00:15] INFO: [Account] Fetched 5000 audits for window...
[2026-05-08 10:00:45] INFO: [Account] Inserted 5000 records to Snowflake
...continues catching up, then switches to LIVE mode automatically...
[2026-05-08 14:30:00] INFO: [Account] Mode=LIVE, lag=8.0min, window=10min
[2026-05-08 14:30:05] INFO: [Account] Caught up to current time. Sleeping 60s before next check...
```

Press `Ctrl+C` for graceful shutdown.

### As a Docker container
```bash
docker compose up -d
docker compose logs -f audit-sync
```

### As an Azure Function
```bash
# Test locally
func start

# Deploy
func azure functionapp publish <your-function-app-name>
```

---

## Choosing the Right Deployment

| Scenario | Recommended Host | Why |
|----------|------------------|-----|
| You have an existing VM / server | Console job | Simplest, no extra infrastructure |
| You want isolation + portability | Docker container | Works anywhere Docker runs |
| You want zero-ops, auto-scaling | Azure Function | Fully managed, pay-per-use |
| You want highest throughput on backlog | Multiple containers, one per entity | Set `ENTITY=Account` per container |
| You're cost-sensitive in steady-state | Azure Function | Cheapest at low volume |
| You have very high audit volume | Container Apps with multiple replicas | Horizontal scaling |

---

## Key Configuration Knobs

### Window sizes (`config.json`)
```json
"windowSizeMinutes": {
    "backlog": 60,
    "continuous": 10
}
```

### Auto-detect threshold
```json
"modeAutoDetect": {
    "enabled": true,
    "backlog_threshold_minutes": 60
}
```
If `lastSyncEnd` is more than 60 min behind, use 60-min windows; otherwise 10-min.

### Loop forever vs exit when caught up
```json
"features": {
    "exit_when_caught_up": false
}
```
- `false` → console/container mode (loop forever)
- `true` → function/batch mode (process and exit)

### Concurrency
```json
"performance": {
    "max_concurrent_entities": 3
}
```
Process up to 3 entities in parallel within a single host.

### Run a single entity (for parallel containers)
Set env var: `ENTITY=Account`

### Backfill from a specific date
Set env var: `OVERRIDE_START_TIME=2026-01-01T00:00:00`

---

## Customization

People should be able to adapt this easily. Here's what's straightforward to change:

| What you want to change | Where |
|-------------------------|-------|
| Add a new entity | `config.json` → `entities` array |
| Change tracked attributes | `config.json` → `entities[].attributes` |
| Speed up or slow down | `config.json` → `performance` and `windowSizeMinutes` |
| Change Snowflake schema/database | `.env` → `SNOWFLAKE_DATABASE` / `SNOWFLAKE_SCHEMA` |
| Change Dataverse API version | `config.json` → `dataverse.api.version` |
| Add file logging | `config.json` → `logging.output.file.enabled = true` |
| Test without writing | `config.json` → `features.dry_run = true` |
| Change function trigger frequency | `function_app.py` → `@app.schedule(schedule="...")` |

---

## Documentation

- [CONFIGURATION_REFERENCE.md](../CONFIGURATION_REFERENCE.md) — every config option explained
- [ARCHITECTURE.md](../ARCHITECTURE.md) — full system design
- [LICENSE.md](../LICENSE.md) — MIT license + disclaimer

---

## Summary

✅ **One codebase** — `main.py` is the only sync logic  
✅ **Three deployment options** — console, container, function  
✅ **Auto-adaptive** — backlog catch-up → live mode automatically  
✅ **Easy to fork & customize** — change one file, redeploy anywhere  
✅ **No separate "backlog vs continuous" jobs** — it figures it out itself  
