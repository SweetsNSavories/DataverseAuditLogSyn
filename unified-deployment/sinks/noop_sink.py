"""
NoopSink - dry-run / smoke-test sink that prints what it would do.

Use cases:
  * End-to-end pipeline validation without provisioning real storage.
  * Local debugging - see exactly what records would be written.
  * CI smoke tests where you only want to validate Dataverse connectivity.

Configure via:
    "sink": { "type": "noop" }

NOT for production use - records are not persisted anywhere.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, List, Optional

from .base import AuditSink

logger = logging.getLogger("sink.noop")


class NoopSink(AuditSink):
    """In-memory state, no-op writes. Logs everything for inspection."""

    name = "noop"

    def __init__(self) -> None:
        self._state: Dict[str, datetime] = {}
        self._total_writes: int = 0

    def initialize(self) -> None:
        logger.info("Noop sink initialised (records will be discarded)")

    def get_state(self, entity: str) -> Optional[datetime]:
        return self._state.get(entity)

    def update_state(
        self, entity: str, last_sync_end: datetime, record_count: int
    ) -> None:
        self._state[entity] = last_sync_end
        logger.info(
            "[%s] state -> last_sync_end=%s (+%d records)",
            entity, last_sync_end.isoformat(), record_count,
        )

    def write_audits(
        self,
        entity: str,
        records: List[Dict],
        window_end: datetime,
        run_id: str,
    ) -> int:
        self._total_writes += len(records)
        logger.info(
            "[%s] write_audits run_id=%s records=%d (cumulative=%d)",
            entity, run_id, len(records), self._total_writes,
        )
        if records:
            sample = records[0]
            audit_id = sample.get("auditid", "?")
            detail_type = sample.get("auditDetailType", "(no detail)")
            keys = list(sample.keys())[:8]
            logger.info(
                "[%s]   sample auditid=%s detail_type=%s top_keys=%s",
                entity, audit_id, detail_type, keys,
            )
        return len(records)

    def close(self) -> None:
        logger.info(
            "Noop sink closed. Total records that would have been written: %d",
            self._total_writes,
        )
