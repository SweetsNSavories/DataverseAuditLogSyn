"""
Azure Cosmos DB (NoSQL API) implementation of AuditSink.

Why Cosmos DB for audit data:
  - Low-latency contextual lookups by auditid / userid / entity
  - Multi-region writes for global read replicas
  - Hierarchical Partition Keys: ["/entity", "/auditYearMonth"]
      -> overcomes 20 GB logical partition limit on big entities
      -> targets queries to a small slice instead of fan-out
  - Per-container TTL maps cleanly to Dataverse 90-day audit retention
  - Auth via Microsoft Entra ID (RBAC) - no keys in code

Schema (one document per audit):
  {
    "id":             "<auditid>",                    # required, dedup key
    "entity":         "account",                      # 1st HPK level
    "auditYearMonth": "2026-05",                      # 2nd HPK level
    "auditId":        "<auditid>",
    "createdOn":      "2026-05-08T06:01:38.59Z",
    "objectTypeCode": "systemuser",
    "operation":      2, "action": 33,
    "userId":         "...",
    "changes":        {...full audit detail...},
    "processedAt":    "...", "runId":  "..."
  }

State doc lives in container `sync_state` with id=entity, pk=/entity.
"""

from __future__ import annotations

import logging
import os
import re
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .base import AuditSink, SinkError, SinkPartialWriteError

logger = logging.getLogger("sink.cosmos")


def _safe_id(value: str) -> str:
    """Cosmos `id` cannot contain / \\ ? # and must be <= 1023 chars."""
    return re.sub(r"[\\/?#]", "_", str(value))[:1023]


class CosmosSink(AuditSink):
    """
    Synchronous Cosmos sink using azure-cosmos. The orchestrator already
    parallelises entity processing, so a sync client is enough and keeps
    dependencies small.
    """

    name = "cosmos"

    def __init__(self, config: Dict) -> None:
        super().__init__(config)
        self._client = None
        self._db = None
        self._audits = None
        self._state = None

    # ------------------------------------------------------------------
    def initialize(self) -> None:
        try:
            from azure.cosmos import CosmosClient, PartitionKey, exceptions  # noqa: F401
        except ImportError as e:
            raise SinkError(
                "azure-cosmos is not installed. Run: pip install azure-cosmos"
            ) from e

        cfg = (self.config.get("sink", {}).get("cosmos") or {})
        endpoint = os.getenv("COSMOS_ENDPOINT") or cfg.get("endpoint")
        if not endpoint:
            raise SinkError(
                "Cosmos endpoint missing. Set env COSMOS_ENDPOINT or sink.cosmos.endpoint"
            )

        db_name = os.getenv("COSMOS_DATABASE") or cfg.get("database", "audit_db")
        audits_container = (
            os.getenv("COSMOS_CONTAINER_AUDITS") or cfg.get("container_audits", "audit_logs")
        )
        state_container = (
            os.getenv("COSMOS_CONTAINER_STATE") or cfg.get("container_state", "sync_state")
        )
        # Connection mode + creds
        key = os.getenv("COSMOS_KEY")  # for emulator / quick start
        use_aad = (cfg.get("auth", "aad").lower() == "aad") and not key

        if use_aad:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
            self._client = CosmosClient(endpoint, credential=credential)
            logger.info(f"CosmosSink connected to {endpoint} via DefaultAzureCredential (AAD RBAC)")
        else:
            if not key:
                raise SinkError(
                    "COSMOS_KEY is empty and AAD auth is disabled. "
                    "Provide COSMOS_KEY or set sink.cosmos.auth=aad."
                )
            self._client = CosmosClient(endpoint, credential=key)
            logger.info(f"CosmosSink connected to {endpoint} via key (suitable for emulator/dev)")

        # Database
        try:
            self._db = self._client.create_database_if_not_exists(id=db_name)
        except exceptions.CosmosHttpResponseError as e:
            raise SinkError(f"Cannot create/access database '{db_name}': {e.message}") from e

        # Hierarchical Partition Key for audits container
        # NOTE: HPK requires API version >= 2022-11-15; falls back to single PK on older accounts
        ttl_days = cfg.get("ttl_days", 90)
        ttl_seconds = int(ttl_days * 24 * 3600) if ttl_days else None
        throughput_audits = cfg.get("throughput_audits", 400)  # autoscale-min equivalent

        # Helper that survives serverless accounts (which reject offer_throughput).
        def _create_container(container_id: str, partition_key, throughput: Optional[int], ttl: Optional[int]):
            kwargs = {"id": container_id, "partition_key": partition_key}
            if ttl is not None:
                kwargs["default_ttl"] = ttl
            try:
                if throughput is not None:
                    return self._db.create_container_if_not_exists(offer_throughput=throughput, **kwargs)
                return self._db.create_container_if_not_exists(**kwargs)
            except exceptions.CosmosHttpResponseError as err:
                msg = (err.message or "").lower()
                if throughput is not None and "serverless" in msg:
                    logger.info(
                        f"Account is serverless - retrying create_container '{container_id}' "
                        "without offer_throughput"
                    )
                    return self._db.create_container_if_not_exists(**kwargs)
                raise

        try:
            from azure.cosmos import PartitionKey
            try:
                pk = PartitionKey(path=["/entity", "/auditYearMonth"], kind="MultiHash")
            except TypeError:
                # Older SDKs without HPK
                pk = PartitionKey(path="/entity")
            self._audits = _create_container(
                container_id=audits_container,
                partition_key=pk,
                throughput=throughput_audits,
                ttl=ttl_seconds,
            )
            logger.info(
                f"Container '{audits_container}' ready "
                f"(HPK=/entity,/auditYearMonth ttl_days={ttl_days})"
            )
        except exceptions.CosmosHttpResponseError as e:
            raise SinkError(f"Cannot create audits container: {e.message}") from e

        # State container - small, single-PK by entity
        try:
            self._state = _create_container(
                container_id=state_container,
                partition_key=PartitionKey(path="/entity"),
                throughput=400,
                ttl=None,
            )
            logger.info(f"Container '{state_container}' ready (PK=/entity)")
        except exceptions.CosmosHttpResponseError as e:
            raise SinkError(f"Cannot create state container: {e.message}") from e

    def close(self) -> None:
        # azure-cosmos sync client doesn't need explicit close
        self._client = None

    # ------------------------------------------------------------------
    def get_state(self, entity: str) -> Optional[datetime]:
        from azure.cosmos import exceptions
        try:
            doc = self._state.read_item(item=entity, partition_key=entity)
            v = doc.get("lastSyncEnd")
            return datetime.fromisoformat(v.replace("Z", "")) if v else None
        except exceptions.CosmosResourceNotFoundError:
            return None
        except Exception as e:
            logger.warning(f"get_state({entity}) failed: {e}")
            return None

    def update_state(self, entity: str, last_sync_end: datetime, record_count: int) -> None:
        from azure.cosmos import exceptions
        try:
            doc = self._state.read_item(item=entity, partition_key=entity)
            doc["lastSyncEnd"] = last_sync_end.isoformat()
            doc["recordCount"] = int(doc.get("recordCount", 0)) + int(record_count)
            doc["updatedAt"] = datetime.utcnow().isoformat()
            self._state.replace_item(item=entity, body=doc)
        except exceptions.CosmosResourceNotFoundError:
            self._state.create_item(
                {
                    "id": entity,
                    "entity": entity,
                    "lastSyncEnd": last_sync_end.isoformat(),
                    "recordCount": int(record_count),
                    "updatedAt": datetime.utcnow().isoformat(),
                }
            )

    # ------------------------------------------------------------------
    def write_audits(
        self,
        entity: str,
        records: List[Dict],
        window_end: datetime,
        run_id: str,
    ) -> int:
        if not records:
            return 0

        from azure.cosmos import exceptions

        processed_at = datetime.utcnow().isoformat()
        written = 0
        failures: List[tuple] = []  # (audit_id, status_code, message)

        for r in records:
            audit_id = r.get("auditid") or r.get("AuditId") or str(uuid.uuid4())
            created_on = r.get("createdon") or window_end.isoformat()
            try:
                ym = created_on[:7]  # YYYY-MM
            except Exception:
                ym = window_end.strftime("%Y-%m")

            doc = {
                "id":             _safe_id(audit_id),
                "entity":         entity,
                "auditYearMonth": ym,
                "auditId":        audit_id,
                "createdOn":      created_on,
                "objectTypeCode": r.get("objecttypecode") or r.get("ObjectTypeCode"),
                "operation":      r.get("operation"),
                "action":         r.get("action"),
                "userId":         (r.get("_userid_value")
                                   or r.get("userid")
                                   or (r.get("userId") if isinstance(r.get("userId"), str) else None)),
                "changes":        r,
                "processedAt":    processed_at,
                "runId":          run_id,
            }
            try:
                # upsert = idempotent on id within partition
                self._audits.upsert_item(body=doc)
                written += 1
            except exceptions.CosmosHttpResponseError as e:
                # 429 retry handled by SDK by default; surface anything else
                logger.error(
                    f"Cosmos upsert failed for audit {audit_id} "
                    f"(status={e.status_code}, sub={getattr(e, 'sub_status', '?')}): {e.message}"
                )
                failures.append((audit_id, e.status_code, str(e.message)))
            except Exception as e:
                # Catch-all so a single unexpected error doesn't silently advance state.
                logger.error(
                    f"Cosmos upsert raised unexpected {type(e).__name__} "
                    f"for audit {audit_id}: {e}"
                )
                failures.append((audit_id, -1, f"{type(e).__name__}: {e}"))

        if failures:
            # Refuse to report success. Caller (process_window) MUST treat this
            # as "do not advance lastSyncEnd" so the entire window gets replayed.
            # Replay is safe: succeeded records become no-op upserts on the same id.
            raise SinkPartialWriteError(
                entity=entity,
                written=written,
                failed=len(failures),
                sample_errors=failures,
            )

        return written
