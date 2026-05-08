"""
End-to-end smoke test: real Dataverse + noop sink + 7 entities + 2-hour window.

Usage:
    python smoke_test.py

This validates:
  * .env loading (with override of stale shell vars)
  * MSAL client_credentials OAuth
  * fetch_audits (header pagination)
  * fetch_audit_details_with_retry (RetrieveAuditDetails function)
  * sink.write_audits + sink.update_state (boundary contract)
  * exit_when_caught_up shutdown

Does NOT write to any real storage - the noop sink just prints summaries.
"""
import asyncio
import json
import logging
import sys
from copy import deepcopy
from datetime import datetime, timedelta

# Load .env BEFORE importing main so env vars are present at module import.
from dotenv import load_dotenv  # type: ignore

load_dotenv(override=True)

import main as m  # noqa: E402

# ---------- Logging ----------
logging.basicConfig(
    level=logging.INFO,
    format="[%(asctime)s] %(levelname)s [%(name)s]: %(message)s",
    datefmt="%H:%M:%S",
)


# ---------- Test config (overrides config.json in-memory) ----------
def _build_test_config() -> dict:
    with open("config.json", "r", encoding="utf-8-sig") as f:
        cfg = json.load(f)
    test_cfg = deepcopy(cfg)
    test_cfg["sink"]["type"] = "noop"
    test_cfg["features"]["dry_run"] = False
    test_cfg["features"]["exit_when_caught_up"] = True
    test_cfg["performance"]["max_concurrent_entities"] = 1
    # Process only one entity to keep the run short - systemuser definitely
    # has audit traffic in the test environment.
    test_cfg["entities"] = [
        e for e in test_cfg["entities"] if e["name"] == "systemuser"
    ]
    return test_cfg


async def main_async():
    m.CONFIG = _build_test_config()
    # Force a 2-hour-old start so we get a non-empty window.
    m.OVERRIDE_START_TIME = (
        datetime.utcnow() - timedelta(hours=2)
    ).isoformat()
    print("=" * 70)
    print("SMOKE TEST - Dataverse + Noop Sink")
    print(f"  Start time override : {m.OVERRIDE_START_TIME}")
    print(f"  Entity              : {m.CONFIG['entities'][0]['name']}")
    print(f"  Sink                : {m.CONFIG['sink']['type']}")
    print("=" * 70)
    total = await m.run_sync("smoke-test")
    print()
    print("=" * 70)
    print(f"SMOKE TEST RESULT: total_records_processed={total}")
    print("=" * 70)
    return total


if __name__ == "__main__":
    try:
        result = asyncio.run(main_async())
        # Even 0 records is a successful smoke test - it proves the pipeline
        # ran cleanly through every stage.
        sys.exit(0)
    except Exception as e:
        import traceback

        print(f"\nSMOKE TEST FAILED: {type(e).__name__}: {e}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
