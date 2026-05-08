"""Abstract base class for pluggable audit storage sinks."""

from __future__ import annotations

import abc
from datetime import datetime
from typing import Dict, List, Optional


class SinkError(Exception):
    """Raised when a sink operation fails in a way the caller should handle."""


class SinkPartialWriteError(SinkError):
    """
    Raised when write_audits succeeded for SOME records and failed for others.

    Carries enough context for the orchestrator to (a) log meaningfully and
    (b) decide NOT to advance lastSyncEnd so the window will be replayed
    on the next loop iteration. Replay is safe because writes are idempotent
    on auditid (already-written records become no-op upserts).

    Attributes:
        entity:        which entity the partial failure was for
        written:       count of records that DID succeed
        failed:        count of records that FAILED (>= 1)
        sample_errors: up to 3 (audit_id, status, message) tuples for diagnostics
    """

    def __init__(
        self,
        entity: str,
        written: int,
        failed: int,
        sample_errors: Optional[List[tuple]] = None,
    ) -> None:
        self.entity = entity
        self.written = written
        self.failed = failed
        self.sample_errors = sample_errors or []
        sample_str = "; ".join(
            f"audit={a} status={s} msg={m[:80]}" for a, s, m in self.sample_errors[:3]
        )
        super().__init__(
            f"[{entity}] partial write: {written} succeeded, {failed} failed. "
            f"Window will be retried (writes are idempotent). Samples: {sample_str}"
        )


class AuditSink(abc.ABC):
    """
    Storage-agnostic interface for landing Dataverse audit records and
    tracking per-entity sync state.

    Lifecycle:
        sink = get_sink(config)        # factory
        sink.initialize()              # create container/table/filesystem if missing
        last_end = sink.get_state(entity)
        sink.write_audits(entity, [details, ...], window_end=..., run_id=...)
        sink.update_state(entity, window_end, count)
        sink.close()

    All implementations must be:
      - Idempotent      : re-writing the same auditid must not duplicate
      - Crash-safe      : state advance only commits AFTER data write succeeds
      - Bounded-memory  : write_audits may receive a few thousand rows at once
    """

    name: str = "base"

    def __init__(self, config: Dict) -> None:
        self.config = config

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def initialize(self) -> None:
        """Optional one-time setup (create container, schema, filesystem, etc.)"""

    def close(self) -> None:
        """Release pooled connections / handles."""

    # ------------------------------------------------------------------
    # State (per-entity watermark)
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def get_state(self, entity: str) -> Optional[datetime]:
        """Return the last successfully synced 'window_end' for the entity, or None."""

    @abc.abstractmethod
    def update_state(self, entity: str, last_sync_end: datetime, record_count: int) -> None:
        """Atomically persist new watermark + cumulative record count."""

    # ------------------------------------------------------------------
    # Data write
    # ------------------------------------------------------------------
    @abc.abstractmethod
    def write_audits(
        self,
        entity: str,
        records: List[Dict],
        window_end: datetime,
        run_id: str,
    ) -> int:
        """
        Persist a batch of audit detail dicts. MUST be idempotent on (auditid).

        Returns the number of records actually written (may equal len(records)
        for first-time writes, or be less when sink dedupes upserts internally).
        """
