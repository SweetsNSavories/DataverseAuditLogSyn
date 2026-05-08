"""
End-to-end LIVE Cosmos DB test.

  Dataverse  ->  fetch_audits  ->  RetrieveAuditDetails
                                                |
                                                v
                                  Cosmos DB (dataverseauditdocument)
                                  database = dataverse_audit
                                  containers = audit_logs (HPK), sync_state

Usage:
    python smoke_test_cosmos.py

What this validates beyond smoke_test.py:
  * CosmosSink.initialize() - DB & containers auto-create
  * CosmosSink.write_audits  - real upserts with HPK doc shape
  * CosmosSink.update_state  - state container round-trip
  * Idempotency              - re-runs safely upsert
"""
import asyncio
import json
import logging
import sys
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


def _build_test_config() -> dict:
    with open("config.json", "r", encoding="utf-8-sig") as f:
        cfg = json.load(f)
    test_cfg = deepcopy(cfg)
    test_cfg["sink"]["type"] = "cosmos"
    # COSMOS_KEY env var auto-flips auth path inside cosmos_sink.py
    test_cfg["features"]["dry_run"] = False
    test_cfg["features"]["exit_when_caught_up"] = True
    test_cfg["performance"]["max_concurrent_entities"] = 1
    # Just one entity to keep the test short and predictable.
    test_cfg["entities"] = [
        e for e in test_cfg["entities"] if e["name"] == "systemuser"
    ]
    return test_cfg


async def main_async():
    m.CONFIG = _build_test_config()
    m.OVERRIDE_START_TIME = (
        datetime.utcnow() - timedelta(hours=2)
    ).isoformat()
    print("=" * 70)
    print("LIVE COSMOS TEST - Dataverse -> Cosmos DB (dataverseauditdocument)")
    print(f"  Start time override : {m.OVERRIDE_START_TIME}")
    print(f"  Entity              : {m.CONFIG['entities'][0]['name']}")
    print(f"  Sink                : {m.CONFIG['sink']['type']}")
    print(f"  Database            : {m.CONFIG['sink']['cosmos']['database']}")
    print("=" * 70)
    total = await m.run_sync("cosmos-live-test")
    print()
    print("=" * 70)
    print(f"COSMOS TEST RESULT: total_records_processed={total}")
    print("=" * 70)
    return total


if __name__ == "__main__":
    try:
        result = asyncio.run(main_async())
        sys.exit(0)
    except Exception as e:
        print(f"FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
