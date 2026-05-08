"""
ADLS Gen2 / OneLake (Parquet) implementation of AuditSink.

OneLake speaks the ADLS Gen2 DFS protocol, so a single implementation
serves both targets - the only difference is the account URL:

  ADLS Gen2 :  https://<account>.dfs.core.windows.net
  OneLake   :  https://onelake.dfs.fabric.microsoft.com

Layout (Hive-partitioned for Spark / Synapse / Trino / Fabric SQL):

  <root>/audits/entity=<entity>/year=<YYYY>/month=<MM>/
       run-<runId>-window-<endIso>.parquet

  <root>/_state/sync_state.json

Idempotency strategy: each (entity, window_end) writes to a new file - safe
to replay because the orchestrator's state container guarantees we never
re-process the same window unless it failed. Compaction is left to a downstream
job (e.g. Fabric notebook OPTIMIZE/Z-ORDER).
"""

from __future__ import annotations

import io
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from .base import AuditSink, SinkError

logger = logging.getLogger("sink.adls")


class ADLSParquetSink(AuditSink):
    """Lands audit batches as Hive-partitioned Parquet on ADLS Gen2 or OneLake."""

    name = "adls"

    def __init__(self, config: Dict) -> None:
        super().__init__(config)
        self._fs_client = None
        self._root_path = ""
        self._target_label = "ADLS Gen2"

    # ------------------------------------------------------------------
    def initialize(self) -> None:
        try:
            from azure.storage.filedatalake import DataLakeServiceClient  # noqa: F401
            import pyarrow  # noqa: F401
        except ImportError as e:
            raise SinkError(
                "ADLS sink requires azure-storage-file-datalake + pyarrow. "
                "Run: pip install azure-storage-file-datalake pyarrow"
            ) from e

        cfg = (self.config.get("sink", {}).get("adls") or {})
        target = (cfg.get("target") or "adls").lower()
        self._target_label = "OneLake" if target == "onelake" else "ADLS Gen2"

        # Endpoint resolution: explicit > env > derived from target
        account_url = (
            os.getenv("ADLS_ACCOUNT_URL")
            or cfg.get("account_url")
        )
        if not account_url:
            if target == "onelake":
                account_url = "https://onelake.dfs.fabric.microsoft.com"
            else:
                account_name = os.getenv("ADLS_ACCOUNT_NAME") or cfg.get("account_name")
                if not account_name:
                    raise SinkError(
                        "ADLS account missing. Set ADLS_ACCOUNT_URL or ADLS_ACCOUNT_NAME, "
                        "or sink.adls.account_name (omit for OneLake)."
                    )
                account_url = f"https://{account_name}.dfs.core.windows.net"

        # Filesystem (= container in ADLS / workspace in OneLake)
        filesystem = (
            os.getenv("ADLS_FILESYSTEM")
            or cfg.get("filesystem")
            or "audit-sync"
        )
        self._root_path = (
            os.getenv("ADLS_ROOT_PATH") or cfg.get("root_path") or "audits"
        ).strip("/")

        # Auth: AAD (default), or shared key for ADLS dev
        from azure.storage.filedatalake import DataLakeServiceClient
        key = os.getenv("ADLS_KEY") or cfg.get("account_key")
        if key and target != "onelake":
            service = DataLakeServiceClient(account_url=account_url, credential=key)
            logger.info(f"{self._target_label} connected via shared key")
        else:
            from azure.identity import DefaultAzureCredential
            credential = DefaultAzureCredential(exclude_interactive_browser_credential=False)
            service = DataLakeServiceClient(account_url=account_url, credential=credential)
            logger.info(f"{self._target_label} connected via DefaultAzureCredential (AAD)")

        self._fs_client = service.get_file_system_client(file_system=filesystem)
        # Create filesystem if missing (skipped on OneLake which auto-provisions on access)
        try:
            self._fs_client.create_file_system()
            logger.info(f"Created filesystem '{filesystem}'")
        except Exception:
            pass  # already exists or no permission to create (OneLake)

        logger.info(
            f"ADLSParquetSink ready: target={self._target_label} "
            f"url={account_url} filesystem={filesystem} root={self._root_path}"
        )

    def close(self) -> None:
        self._fs_client = None

    # ------------------------------------------------------------------
    # State stored as JSON at <root>/_state/sync_state.json
    # ------------------------------------------------------------------
    def _state_path(self) -> str:
        return f"{self._root_path}/_state/sync_state.json"

    def _read_state_blob(self) -> Dict:
        try:
            file_client = self._fs_client.get_file_client(self._state_path())
            data = file_client.download_file().readall()
            return json.loads(data.decode("utf-8")) if data else {}
        except Exception:
            return {}

    def _write_state_blob(self, state: Dict) -> None:
        body = json.dumps(state, indent=2, default=str).encode("utf-8")
        file_client = self._fs_client.get_file_client(self._state_path())
        # ADLS Gen2 atomic overwrite: create with overwrite=True
        file_client.upload_data(body, overwrite=True)

    def get_state(self, entity: str) -> Optional[datetime]:
        state = self._read_state_blob()
        v = (state.get(entity) or {}).get("lastSyncEnd")
        if not v:
            return None
        try:
            return datetime.fromisoformat(v.replace("Z", ""))
        except Exception:
            return None

    def update_state(self, entity: str, last_sync_end: datetime, record_count: int) -> None:
        state = self._read_state_blob()
        prev = state.get(entity) or {"recordCount": 0}
        state[entity] = {
            "entity":       entity,
            "lastSyncEnd":  last_sync_end.isoformat(),
            "recordCount":  int(prev.get("recordCount", 0)) + int(record_count),
            "updatedAt":    datetime.utcnow().isoformat(),
        }
        self._write_state_blob(state)

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
        import pyarrow as pa
        import pyarrow.parquet as pq

        processed_at = datetime.utcnow().isoformat()
        rows = []
        for r in records:
            rows.append(
                {
                    "audit_id":         r.get("auditid"),
                    "entity":           entity,
                    "created_on":       r.get("createdon"),
                    "object_type_code": r.get("objecttypecode"),
                    "operation":        r.get("operation"),
                    "action":           r.get("action"),
                    "user_id":          r.get("_userid_value") or r.get("userid"),
                    "changes":          json.dumps(r, default=str),
                    "processed_at":     processed_at,
                    "run_id":           run_id,
                }
            )
        table = pa.Table.from_pylist(rows)

        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)

        year = window_end.strftime("%Y")
        month = window_end.strftime("%m")
        end_iso = window_end.strftime("%Y%m%dT%H%M%S")
        path = (
            f"{self._root_path}/audits/entity={entity}/year={year}/month={month}/"
            f"run-{run_id}-window-{end_iso}.parquet"
        )
        file_client = self._fs_client.get_file_client(path)
        file_client.upload_data(buf.read(), overwrite=True)
        logger.info(
            f"[{self._target_label}] {entity}: wrote {len(rows)} rows -> {path}"
        )
        return len(rows)
