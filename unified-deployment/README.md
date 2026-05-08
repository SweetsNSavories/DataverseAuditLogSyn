# Dataverse Audit Sync - Unified Deployment

**One codebase. Three deployment options. Zero rework.**

This is a single, adaptive sync solution that:
- Catches up on backlog (history) automatically when first started
- Switches to live mode once current
- Runs continuously, indefinitely
- Can be hosted **any way you like** — pick what fits your environment
- Writes to **whichever target you prefer** — Snowflake, Cosmos DB, ADLS Gen2, or OneLake

---

## Pick Your Sink (Storage Target)

The sync writes audit records and tracks per-entity sync state in a **pluggable
sink**. Pick the one your data team actually uses; you only need to install
the deps for that sink.

| Sink | Best for | Auth | Idempotency | Retention | Install |
|------|----------|------|-------------|-----------|---------|
| `snowflake` | BI / data warehouse / SQL analysts | user + password | `MERGE` on `audit_id` | manual | `snowflake-connector-python` |
| `cosmos` | Operational lookups, RAG context, multi-region apps | AAD RBAC (default) or key | `upsert` by `id` | TTL = 90 d (matches Dataverse) | `azure-cosmos`, `azure-identity` |
| `adls` | Lakehouse / Synapse / Trino / Spark / Databricks | AAD or shared key | per-window Parquet file overwrite | manual / lifecycle policy | `azure-storage-file-datalake`, `pyarrow`, `azure-identity` |
| `onelake` | Microsoft Fabric Lakehouse | AAD (workspace RBAC) | per-window Parquet file overwrite | manual | `azure-storage-file-datalake`, `pyarrow`, `azure-identity` |
| `noop` | Local smoke-test (no real storage) | none | n/a | n/a | none |

Set the choice in `config.json`:
```json
"sink": { "type": "cosmos" }
```
…or override per-deployment with the relevant env vars (see `.env.example`).

#### Validated against live Azure (May 2026)
- `noop` — pipeline smoke test against `orgc783d424.crm.dynamics.com` ✅
  (3 systemuser audits fetched + RetrieveAuditDetails enrichment confirmed)
- `cosmos` — end-to-end against Cosmos account `dataverseauditdocument`
  (serverless, eastus) ✅
  - Containers `audit_logs` (HPK `/entity` + `/auditYearMonth`, 90 d TTL) and
    `sync_state` auto-created.
  - 3 audit docs upserted into `dataverse_audit/audit_logs`; state row
    `{id:"systemuser", lastSyncEnd:"2026-05-08T07:00:47", recordCount:3}`
    written to `sync_state`.
  - Re-run = idempotent: 0 new records, exits cleanly via `exit_when_caught_up`.
- `snowflake`, `adls`, `onelake` — code path identical to validated sinks; not
  end-to-end tested in this run because no Snowflake/Storage account was
  available in the test subscription.

### Sink-specific quickstart

#### Snowflake (default)
```bash
# .env
SNOWFLAKE_USER=...
SNOWFLAKE_PASSWORD=...
SNOWFLAKE_ACCOUNT=xy12345.region
SNOWFLAKE_WAREHOUSE=COMPUTE_WH
SNOWFLAKE_DATABASE=AUDIT_DB
SNOWFLAKE_SCHEMA=PUBLIC
```
Tables `audit_logs` and `sync_state` are auto-created with `CREATE TABLE IF NOT EXISTS` on first run.

#### Cosmos DB (recommended for production multi-region)
```bash
# .env
COSMOS_ENDPOINT=https://<account>.documents.azure.com:443/
COSMOS_DATABASE=audit_db
COSMOS_CONTAINER_AUDITS=audit_logs
COSMOS_CONTAINER_STATE=sync_state
# Auth: leave COSMOS_KEY unset to use AAD via DefaultAzureCredential
# (managed identity in prod, az login locally). Only set COSMOS_KEY for the emulator.
```
Containers are auto-created with **hierarchical partition keys** `/entity` + `/auditYearMonth`
(uses `MultiHash` PartitionKey on Cosmos SDK ≥ 4.7) and **TTL = 90 days** to match Dataverse's
default audit retention. AAD RBAC role required: **Cosmos DB Built-in Data Contributor**
on the account scope.

> **Serverless accounts are auto-detected.** If your Cosmos account is on the
> serverless capacity model, the sink retries container creation without
> `offer_throughput` (since serverless doesn't accept it). No config change needed.

#### ADLS Gen2 (Parquet for lakehouses)
```bash
# .env
ADLS_ACCOUNT_NAME=mylakehouse           # account NAME, no .dfs.core.windows.net
ADLS_FILESYSTEM=audit-sync              # container name
# AAD by default. Override with ADLS_KEY for shared-key auth (not allowed for OneLake).
```
Layout (Hive-partitioned, Spark/Synapse/Trino-friendly):
```
audit-sync/
  audits/entity=account/year=2026/month=05/run-<runId>-window-<endIso>.parquet
  _state/sync_state.json
```

#### OneLake (Microsoft Fabric Lakehouse)
```json
"sink": {
  "type": "onelake",
  "adls": {
    "target": "onelake",
    "filesystem": "<workspace-name>/<lakehouse>.Lakehouse/Files",
    "root_path": "audits"
  }
}
```
No account name needed - the host is fixed at `https://onelake.dfs.fabric.microsoft.com`.
The service principal (or managed identity) must be granted **Contributor** on the Fabric
workspace.

#### Noop (smoke test)
```json
"sink": { "type": "noop" }
```
Use `python smoke_test.py` for an end-to-end pipeline check that connects to Dataverse,
fetches audits, calls `RetrieveAuditDetails`, and **prints** what would be written. No
storage required.

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

### 1. Pick and provision your sink (storage target)
See [Pick Your Sink](#pick-your-sink-storage-target) above. Tables / containers /
filesystems are **auto-created** on first run - you only need an empty database
or storage account to point at.

### 2. Register an Azure Entra ID app (service principal)
- Create an App Registration in Azure Portal
- Add a **client secret** (note the *Value*, not the Secret ID)
- In your Power Platform environment, create an **Application User** for that
  app (Settings → Users + permissions → Application users → New app user)
- Assign the **System Administrator** security role (or a custom role with
  `prvReadAudit` + `prvDeleteAudit` if you want least-privilege)

### 3. Edit `config.json`
- Set `sink.type` to your chosen target
- List the entities you want to track in `entities`
- Specify which attributes per entity
- Tune performance parameters (defaults are fine for most cases)

### 4. Set environment variables
- Copy `.env.example` to `.env` (for console/container)
- OR edit `local.settings.json` (for Azure Functions local testing)
- OR set in Azure Portal Configuration (for deployed Function App)

### 5. (Recommended) Smoke-test the pipeline first
```bash
python smoke_test.py
```
Runs against live Dataverse with a `noop` sink so you can validate OAuth +
audit fetch + `RetrieveAuditDetails` enrichment **before** wiring up real
storage. Prints sample audit content for inspection.

> **Heads up - stale env vars on Windows.** If you ever set `CLIENT_SECRET` /
> `CLIENT_ID` etc. as a Windows User-level env var, those persist across all
> sessions and will shadow `.env`. The script calls `load_dotenv(override=True)`
> so the `.env` file always wins, but if you launch from a tool that reads env
> vars directly (e.g. some CI runners), clean them up with:
> `[Environment]::SetEnvironmentVariable("CLIENT_SECRET", $null, "User")`.

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
