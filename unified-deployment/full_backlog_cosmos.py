"""
Full-backlog Cosmos sync test.

Uses the configured 60-min backlog window slices, all 7 entities, and walks
back N days (default 90 = Dataverse default audit retention). Times the run
end-to-end and reports per-entity stats from the Cosmos sync_state container.

Usage:
    python full_backlog_cosmos.py [DAYS_BACK]

Prerequisites:
    python reset_cosmos_state.py    # if you want a fresh backlog
"""
import asyncio
import json
import logging
import os
import sys
import time
from copy import deepcopy
from datetime import datetime, timedelta

from dotenv import load_dotenv  # type: ignore
load_dotenv(override=True)

import main as m  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s]: %(message)s",
    datefmt="%H:%M:%S",
)


def _build_test_config(concurrency: int = 3, window_min: int = 10) -> dict:
    with open("config.json", "r", encoding="utf-8-sig") as f:
        cfg = json.load(f)
    cfg["sink"]["type"] = "cosmos"
    cfg["features"]["dry_run"] = False
    cfg["features"]["exit_when_caught_up"] = True
    cfg["performance"]["max_concurrent_entities"] = concurrency
    # Force fixed window size for the entire run (no backlog vs continuous switch).
    cfg["windowSizeMinutes"]["backlog"] = window_min
    cfg["windowSizeMinutes"]["continuous"] = window_min
    cfg["modeAutoDetect"]["enabled"] = False
    return cfg


async def main_async(days_back: int):
    m.CONFIG = _build_test_config()
    start = datetime.utcnow() - timedelta(days=days_back)
    m.OVERRIDE_START_TIME = start.isoformat()

    entities = [e["name"] for e in m.CONFIG["entities"]]
    print("=" * 78)
    print("FULL BACKLOG COSMOS SYNC")
    print(f"  Window size (backlog) : {m.CONFIG['windowSizeMinutes']['backlog']} min")
    print(f"  Window size (live)    : {m.CONFIG['windowSizeMinutes']['continuous']} min")
    print(f"  Concurrency           : {m.CONFIG['performance']['max_concurrent_entities']} entities in parallel")
    print(f"  Start (UTC)           : {start.isoformat()}")
    print(f"  Now   (UTC)           : {datetime.utcnow().isoformat()}")
    print(f"  Days back             : {days_back}")
    print(f"  Entities ({len(entities):>2})         : {', '.join(entities)}")
    print(f"  Sink                  : cosmos -> {os.getenv('COSMOS_ENDPOINT')}")
    print("=" * 78)

    t0 = time.perf_counter()
    total = await m.run_sync("full-backlog-cosmos")
    elapsed = time.perf_counter() - t0

    print()
    print("=" * 78)
    print(f"  Total records processed : {total}")
    print(f"  Elapsed wall time       : {elapsed:.1f} seconds  "
          f"({elapsed/60:.1f} min)")
    if total:
        print(f"  Avg seconds/record      : {elapsed/total:.3f}")
        print(f"  Records / second        : {total/elapsed:.1f}")
    print("=" * 78)

    # Per-entity breakdown straight from Cosmos sync_state
    try:
        from azure.cosmos import CosmosClient
        client = CosmosClient(os.environ["COSMOS_ENDPOINT"],
                              credential=os.environ["COSMOS_KEY"])
        db = client.get_database_client(os.getenv("COSMOS_DATABASE", "dataverse_audit"))
        state = db.get_container_client(os.getenv("COSMOS_CONTAINER_STATE", "sync_state"))
        rows = list(state.query_items(
            "SELECT c.id, c.lastSyncEnd, c.recordCount, c.updatedAt FROM c",
            enable_cross_partition_query=True,
        ))
        print()
        print("Per-entity state (from Cosmos sync_state):")
        print(f"  {'entity':<18} {'recordCount':>12}  lastSyncEnd")
        for r in sorted(rows, key=lambda x: -int(x.get('recordCount', 0))):
            print(f"  {r['id']:<18} {r.get('recordCount', 0):>12}  {r.get('lastSyncEnd', '')}")
    except Exception as e:
        print(f"  (could not query sync_state: {e})")

    return total


if __name__ == "__main__":
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 90
    try:
        asyncio.run(main_async(days))
        sys.exit(0)
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
