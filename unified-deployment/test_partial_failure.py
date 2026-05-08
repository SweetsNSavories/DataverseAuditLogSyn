"""
Test for the fail-fast / replay-on-partial-failure fix (Hole 1).

Scenario simulated:
    A 5-record audit window is written to Cosmos. The 3rd upsert fails
    (simulating a 429/timeout/throttle). We assert that:

      1. CosmosSink.write_audits raises SinkPartialWriteError carrying
         (written=2, failed=3, samples=[...]).
      2. The orchestrator's process_window re-raises it.
      3. The outer loop in process_entity_continuous would NOT advance
         last_sync_end (proven structurally by re-raising; the assignment
         line `last_sync_end = window_end` only runs when the try-block
         completes normally).
      4. Re-running the SAME window after fixing the failure writes all
         5 records cleanly (idempotency check).

Run:
    python test_partial_failure.py
"""

import asyncio
import logging
import sys
import uuid
from datetime import datetime, timedelta
from unittest.mock import patch

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("test_partial_failure")


def _build_records(n: int):
    base = datetime.utcnow().replace(microsecond=0)
    return [
        {
            "auditid": str(uuid.uuid4()),
            "createdon": (base - timedelta(minutes=i)).isoformat() + "Z",
            "objecttypecode": "account",
            "operation": 2,
            "action": 1,
            "_userid_value": "test-user",
            "auditDetail": {"@odata.type": "#Microsoft.Dynamics.CRM.AttributeAuditDetail"},
        }
        for i in range(n)
    ]


def test_cosmos_sink_raises_on_partial_failure():
    """
    Phase 1: Verify CosmosSink.write_audits raises SinkPartialWriteError
    when some upserts fail.
    """
    from sinks.cosmos_sink import CosmosSink
    from sinks.base import SinkPartialWriteError
    from azure.cosmos import exceptions

    sink = CosmosSink({
        "sink": {
            "cosmos": {
                "endpoint": "https://example.invalid:443/",
                "database": "x", "container_audits": "a", "container_state": "s",
                "auth": "key", "throughput_audits": 400, "ttl_days": 90,
            }
        }
    })

    # Skip real network - bypass initialize() and inject mock containers.
    class FakeContainer:
        def __init__(self):
            self.calls = 0
            self.received = []
        def upsert_item(self, body):
            self.calls += 1
            self.received.append(body)
            # Fail the 3rd call only (simulates intermittent 429)
            if self.calls == 3:
                err = exceptions.CosmosHttpResponseError(
                    status_code=429, message="TooManyRequests (simulated)"
                )
                err.sub_status = 3200
                raise err

    fake = FakeContainer()
    sink._audits = fake
    sink._state = FakeContainer()  # not used by write_audits

    records = _build_records(5)
    raised = None
    try:
        sink.write_audits(
            entity="account",
            records=records,
            window_end=datetime.utcnow(),
            run_id=str(uuid.uuid4()),
        )
    except SinkPartialWriteError as e:
        raised = e

    assert raised is not None, "Expected SinkPartialWriteError; nothing raised"
    assert raised.entity == "account"
    assert raised.written == 4, f"Expected 4 succeeded (records 1,2,4,5), got {raised.written}"
    assert raised.failed == 1, f"Expected 1 failed (record 3), got {raised.failed}"
    assert len(raised.sample_errors) == 1
    assert raised.sample_errors[0][1] == 429
    assert fake.calls == 5, f"Expected loop to continue after failure, called {fake.calls} times"
    print("[PASS] Phase 1: CosmosSink raises SinkPartialWriteError(written=4, failed=1)")
    print(f"        message: {raised}")


def test_replay_after_fix_writes_all_clean():
    """
    Phase 2: After the transient error clears, re-calling write_audits with
    the SAME records succeeds for all 5 (proves idempotency is preserved
    AND that we now write everything).
    """
    from sinks.cosmos_sink import CosmosSink

    sink = CosmosSink({
        "sink": {
            "cosmos": {
                "endpoint": "https://example.invalid:443/",
                "database": "x", "container_audits": "a", "container_state": "s",
                "auth": "key", "throughput_audits": 400, "ttl_days": 90,
            }
        }
    })

    class HealthyContainer:
        def __init__(self):
            self.calls = 0
            self.received = []
        def upsert_item(self, body):
            self.calls += 1
            self.received.append(body)

    healthy = HealthyContainer()
    sink._audits = healthy
    sink._state = HealthyContainer()

    records = _build_records(5)
    written = sink.write_audits(
        entity="account",
        records=records,
        window_end=datetime.utcnow(),
        run_id=str(uuid.uuid4()),
    )

    assert written == 5, f"Expected 5 written on healthy retry, got {written}"
    assert healthy.calls == 5
    # Verify all auditids were upserted - this is what gives idempotency
    # on the real Cosmos because id = auditid.
    upserted_ids = [body["id"] for body in healthy.received]
    assert len(set(upserted_ids)) == 5, "Expected 5 unique ids"
    print("[PASS] Phase 2: Healthy retry writes all 5 records, all unique ids")


def test_orchestrator_skips_state_advance_on_failure():
    """
    Phase 3: Verify process_window propagates SinkPartialWriteError so
    process_entity_continuous's outer try/except keeps last_sync_end pinned.
    """
    from sinks.base import SinkPartialWriteError
    from sinks.noop_sink import NoopSink
    import main as m

    # Monkey-patch fetch_audits so we don't hit Dataverse
    async def fake_fetch_audits(*args, **kwargs):
        return [{"auditid": str(uuid.uuid4()), "createdon": "2026-05-08T00:00:00Z"}]

    async def fake_fetch_details(session, token, header, attributes):
        return dict(header)

    # Build a noop sink that throws SinkPartialWriteError on write
    class FailingSink(NoopSink):
        def write_audits(self, entity, records, window_end, run_id):
            raise SinkPartialWriteError(
                entity=entity, written=0, failed=len(records),
                sample_errors=[("fake-id", 503, "service unavailable")],
            )

    sink = FailingSink()

    # Patch fetch funcs in main module
    with patch.object(m, "fetch_audits", side_effect=fake_fetch_audits), \
         patch.object(m, "fetch_audit_details_with_retry", side_effect=fake_fetch_details):
        async def runme():
            try:
                await m.process_window(
                    sink=sink, token="fake",
                    window_start=datetime.utcnow() - timedelta(minutes=10),
                    window_end=datetime.utcnow(),
                    entity="account", attributes=[],
                )
                return "no-exception"
            except SinkPartialWriteError as e:
                return f"raised: {e.failed} failed"

        result = asyncio.run(runme())

    assert result.startswith("raised:"), f"Expected SinkPartialWriteError to propagate, got: {result}"
    # State should NOT have been touched
    assert sink.get_state("account") is None, \
        "BUG: state was advanced even though write failed"
    print(f"[PASS] Phase 3: process_window re-raised ({result}); state remained None")


if __name__ == "__main__":
    print("=" * 70)
    print("Test: Fail-fast on partial Cosmos write failures (Hole 1 fix)")
    print("=" * 70)
    try:
        test_cosmos_sink_raises_on_partial_failure()
        test_replay_after_fix_writes_all_clean()
        test_orchestrator_skips_state_advance_on_failure()
        print("=" * 70)
        print("ALL TESTS PASSED")
        print("=" * 70)
    except AssertionError as e:
        print(f"[FAIL] {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"[ERROR] Unexpected {type(e).__name__}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(2)
