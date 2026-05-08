# Archiving Dataverse Audit Logs Before They Roll Off — A Pragmatic, Open-Source Pattern

> **A note up-front.** This post documents an architectural choice — not a Microsoft product, not a Microsoft offering, and not officially supported. The first-class option for moving Dataverse data out of the platform remains [Azure Synapse Link for Dataverse](https://learn.microsoft.com/power-apps/maker/data-platform/azure-synapse-link-synapse) (and, for newer scenarios, [Microsoft Fabric Link for Dataverse](https://learn.microsoft.com/power-apps/maker/data-platform/azure-synapse-link-view-in-fabric)). If you can adopt either, you should — they handle change-feed plumbing, schema evolution, and incremental refresh for you.
>
> What follows is for the cases where those options aren't on the table — small ISVs, sandbox tenants, regulated environments where Synapse Link isn't yet approved, or admins who simply need a portable, scriptable way to copy audit history out before Dataverse's retention window reclaims it. Treat this as a reference implementation you can adapt, fork, or discard.

---

## Why the audit table is special

Most of what Dataverse exports through Synapse Link is *current state*. The **`audit` table** is different — it's an immutable, append-only record of *who changed what, when, and from where*. That makes it the single most useful table for:

- Regulatory and compliance investigations
- "Why did this opportunity status change last Tuesday?" forensic queries
- Internal analytics on user behaviour and process adoption

It's also the table that grows the fastest. By default, Dataverse keeps audits for 90 days and the platform will start cleaning them up to manage storage. Once a row is gone, it's gone — there is no Synapse Link for the audit table at the row-detail level, and `RetrieveAuditDetails` only works while the source row still exists.

So if you want a defensible long-term audit trail, you have two practical choices:

1. **Buy storage.** Increase your Dataverse capacity entitlement and extend retention.
2. **Copy out, then let Dataverse roll the old rows off.** Cheap storage tier on Azure (or anywhere) holds the cold history; Dataverse keeps the hot 90 days.

Most customers eventually pick option 2 — and that's what this pattern addresses.

---

## What "good" looks like for an external audit copy

Before showing any code, here's the rubric I held this design to. If you build your own, hold yours to the same rubric:

| Property                          | Why it matters                                                                                          |
| --------------------------------- | ------------------------------------------------------------------------------------------------------- |
| **Idempotent**                    | Re-running the same time window must not duplicate rows. Network blips happen.                           |
| **Crash-safe (exactly-once-effective)** | If the process dies mid-window, the next run must replay the same window cleanly. The watermark advances *only* after the data is durable in the destination. |
| **Bounded memory**                | A backlog of millions of audits cannot be loaded all at once.                                            |
| **Backpressure-aware**            | Dataverse rate-limits aggressively. Throttle responses must not drop rows.                               |
| **Observable**                    | Every window logs `[entity] mode=BACKLOG/LIVE, lag=Nmin, window=10min, records=N` so you can watch it work. |
| **Sink-agnostic**                 | The "where does it land" decision is config, not code. Storage choices change; the orchestrator shouldn't. |
| **Field-level discretion**        | Audit details can carry PII. The pattern should let admins narrow which attributes leave the platform.    |

---

## The pattern

The pipeline is conceptually four stages, repeated per entity, per time window:

```
┌────────────────────────────────────────────────────────────────────────┐
│  Per entity (account, contact, …):                                     │
│  ┌──────────────────────────────────────────────────────────────────┐  │
│  │  1.  Read watermark (lastSyncEnd) from sink's state container    │  │
│  │  2.  Compute next time window: [lastSyncEnd, lastSyncEnd + N min)│  │
│  │  3.  Fetch audit headers from Dataverse Web API:                 │  │
│  │        $filter=createdon ge <start> and createdon lt <end>       │  │
│  │  4.  For each header → call RetrieveAuditDetails (bound function)│  │
│  │  5.  (Optional) Project to allow-listed attributes only          │  │
│  │  6.  Upsert each audit doc into sink (id = auditid, idempotent)  │  │
│  │  7.  Advance watermark — ONLY if step 6 fully succeeded          │  │
│  └──────────────────────────────────────────────────────────────────┘  │
│  Loop until caught up; then either exit (function) or sleep (daemon)   │
└────────────────────────────────────────────────────────────────────────┘
```

Three details in this picture do most of the resiliency work. They are deceptively simple:

### Detail 1: Half-open time windows (`ge` / `lt`)

```
[09:00:00, 09:10:00)  → window 1
[09:10:00, 09:20:00)  → window 2
```

The boundary moment (09:10:00.000) belongs to window 2, not window 1. So adjacent windows never overlap and never gap, no matter how many times you replay. This is the same trick Kafka uses for offsets — it's why you can run the loop with confidence.

### Detail 2: The destination document key is the Dataverse `auditid` GUID

Dataverse already assigns a globally unique GUID to every audit row. That GUID becomes the document `id` in the sink. So when you upsert the same audit twice, the second write is a no-op overwrite of the first — idempotency for free, no client-side dedupe table to maintain.

### Detail 3: The watermark moves *after* the write, not with it

The naive version of this pipeline does:

```python
records = fetch_window(...)
sink.write(records)             # 950/1000 succeed; 50 fail with 429
sink.update_watermark(window_end)  # ← BUG: window now "done", 50 rows lost forever
```

The resilient version raises a typed exception when *any* record fails, and the watermark update is conditional on a clean write:

```python
try:
    written = sink.write(records)
    sink.update_watermark(window_end)
except SinkPartialWriteError:
    # Watermark stays put. Next loop iteration replays the same window.
    # Rows that already landed become no-op upserts. Rows that failed get retried.
    raise
```

This single change is the difference between "best effort" and "exactly-once-effective." It's also the mistake I've seen most often when people roll their own.

---

## Choosing where it lands

The reference implementation ships with five sinks. None of them is *the right answer* — they are choices that suit different teams:

| Sink           | When to consider it                                                                              |
| -------------- | ------------------------------------------------------------------------------------------------ |
| **Cosmos DB (NoSQL)** | Operational lookups, "show me everything user X did to record Y last month" in milliseconds. Hierarchical partition keys (`/entity` + `/auditYearMonth`) keep partitions small as data grows. Pair with TTL = your retention policy. |
| **ADLS Gen2 (Parquet)** | Cheap cold storage, Synapse Serverless / Databricks / Fabric notebook analytics. One Parquet file per (entity, window) keeps reads selective. |
| **OneLake (Parquet)** | Same pattern as ADLS; lands directly in a Fabric Lakehouse so Power BI / SQL endpoint / notebooks pick it up natively. |
| **Snowflake (MERGE)** | Existing Snowflake estate; one warehouse / one schema policy. `MERGE INTO ... ON audit_id` keeps semantics identical to the Cosmos upsert. |
| **No-op (logs only)** | First-day connectivity testing. Confirms the Dataverse side works before you provision anything. |

The orchestrator doesn't know which one it's using — it talks to a single `AuditSink` interface (`get_state`, `update_state`, `write_audits`). Adding a sixth sink (e.g., a customer's existing data lake) is roughly 100 lines of Python and one factory entry.

---

## What it looks like running

A clean run against a real (small) tenant — 90 days of backlog, 7 entities, 10-minute windows, 3 entities processed in parallel:

```
$ python full_backlog_cosmos.py 90
[systemuser] Mode=BACKLOG, lag=129599.8m, window=10min, records=0
[contact]    Mode=BACKLOG, lag=129599.8m, window=10min, records=2
[account]    Mode=BACKLOG, lag=129589.8m, window=10min, records=1
...
================================================================================
Total records  : 43,456
Elapsed        : 198.81 s   (3.31 min)
Throughput     : 218.58 rec/s
Per-entity:
  account     21,228   lastSyncEnd=2025-05-18T23:54:19Z
  contact     21,228   lastSyncEnd=2025-05-18T23:54:19Z
  systemuser   1,000   lastSyncEnd=2025-05-18T23:54:19Z
================================================================================
```

A re-run on the same dataset writes 0 records and exits — proof of idempotency.

---

## Things to be honest about

Because this is a "build it yourself" pattern, here are the trade-offs you take on. These are not flaws — they are the price of *not* using Synapse Link / Fabric Link, and you should price them in:

1. **You own the watermark.** If the destination is wiped without the state container, the next run will think it's fresh and re-fetch from your `OVERRIDE_START_TIME` (or default 90 days back). Treat the state container as production data — back it up.
2. **Schema is your problem.** When Dataverse adds a new field to the `audit` table, the patch applies in production. Your sink will keep working — the field just shows up in the JSON blob — but if you're projecting columns (e.g., the attribute allow-list), update the config.
3. **Permissions are your problem.** The application user needs read on `audit` (and `RetrieveAuditDetails` privileges per entity). The Power Platform admin centre handles this, but it's an out-of-band step.
4. **Cost shape changes.** You're trading "Dataverse storage entitlement" for "Cosmos RUs" or "ADLS bytes" or "Snowflake credits." Run the math on your record volume before committing.
5. **Auditing your auditor.** If this pipeline becomes evidence in a compliance investigation, the *pipeline itself* needs an audit trail. The reference implementation stamps every output document with a `runId` (UUID per window) and a `processedAt` timestamp. Keep the orchestrator logs.
6. **It is not a Microsoft product.** I'll repeat this because it matters: if you adopt it, you own the operations, the upgrades, and the on-call pager. The recommended path is still Synapse Link / Fabric Link.

---

## When to reach for it (and when not to)

**Reasonable fit:**

- You need to retain audit history beyond Dataverse's default 90 days and you've decided not to expand your storage entitlement.
- Your security/architecture review hasn't approved Synapse Link in your tenant yet.
- You want a portable, scriptable export that can target storage you already own.
- You want field-level control over what leaves the platform.

**Not a good fit:**

- You need *current state* of business tables (account, contact). Use Synapse Link / Fabric Link.
- You need sub-second freshness in the destination. This pattern's natural cadence is one window length (default 10 min); use Dataverse webhooks for true real-time.
- You don't have somewhere to operate a small Python container, function, or scheduled job.
- You don't have an internal owner for the pipeline.

---

## The reference implementation

The code that backs this post lives at <https://github.com/SweetsNSavories/DataverseAuditLogSyn> under MIT, with no warranty. The `unified-deployment` folder is the version this post describes — single Python codebase, swap sinks via `config.json`, runs locally / in a container / as an Azure Function.

Three files are worth a read if you adopt or fork it:

- `unified-deployment/main.py` — the orchestrator (window loop, watermark logic, exception handling)
- `unified-deployment/sinks/base.py` — the `AuditSink` interface and the `SinkPartialWriteError` contract
- `unified-deployment/sinks/cosmos_sink.py` — the most-tested sink; HPK + serverless-aware container creation + idempotent upsert

A 30-minute read end-to-end. If something feels wrong for your environment, change it — that's the whole point of publishing it.

---

## Closing

Synapse Link / Fabric Link remains the recommended path for getting Dataverse data into Azure. This pattern exists because the audit table has properties that don't fit cleanly into "snapshot the current state" tooling, and because not every customer's compliance posture or budget allows the recommended path on day one.

If you're a Dataverse admin staring at a 90-day cliff and wondering what to do, this is one option among several — adopt it, adapt it, or use it as the "here's the shape of the problem" sketch when you talk to your platform team about doing something more substantial.

Comments, forks, and "this is wrong because…" PRs welcome.
