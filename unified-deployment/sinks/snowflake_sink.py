"""Snowflake implementation of AuditSink."""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional

from .base import AuditSink, SinkError

logger = logging.getLogger("sink.snowflake")


class SnowflakeSink(AuditSink):
    """
    Snowflake sink:
      - audit_logs(audit_id, entity, changes VARIANT, processed_at, run_id)
      - sync_state(entity, last_sync_end, record_count, updated_at)

    Idempotency via MERGE on audit_id when enable_idempotent_upserts=true.
    """

    name = "snowflake"

    def __init__(self, config: Dict) -> None:
        super().__init__(config)
        self._sf = None
        self._connection = None

    # ------------------------------------------------------------------
    def initialize(self) -> None:
        try:
            import snowflake.connector
        except ImportError as e:
            raise SinkError(
                "snowflake-connector-python is not installed. "
                "Run: pip install snowflake-connector-python"
            ) from e
        self._sf = snowflake.connector

        sf_cfg = (self.config.get("sink", {}).get("snowflake") or
                  self.config.get("snowflake", {}).get("connection", {}))
        timeout = sf_cfg.get("timeout_seconds", 30)

        self._connection = self._sf.connect(
            user=os.getenv("SNOWFLAKE_USER"),
            password=os.getenv("SNOWFLAKE_PASSWORD"),
            account=os.getenv("SNOWFLAKE_ACCOUNT"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            database=os.getenv("SNOWFLAKE_DATABASE", "AUDIT_DB"),
            schema=os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            login_timeout=timeout,
        )

        # Best-effort table creation (no-op if tables already exist)
        cur = self._connection.cursor()
        cur.execute(
            """CREATE TABLE IF NOT EXISTS audit_logs (
                 audit_id     STRING,
                 entity       STRING,
                 changes      VARIANT,
                 processed_at TIMESTAMP_NTZ,
                 run_id       STRING,
                 CONSTRAINT pk_audit_logs PRIMARY KEY (audit_id)
               )"""
        )
        cur.execute(
            """CREATE TABLE IF NOT EXISTS sync_state (
                 entity        STRING PRIMARY KEY,
                 last_sync_end TIMESTAMP_NTZ,
                 record_count  NUMBER,
                 updated_at    TIMESTAMP_NTZ
               )"""
        )
        self._connection.commit()
        logger.info("SnowflakeSink ready (audit_logs + sync_state)")

    def close(self) -> None:
        try:
            if self._connection is not None:
                self._connection.close()
        finally:
            self._connection = None

    # ------------------------------------------------------------------
    def get_state(self, entity: str) -> Optional[datetime]:
        from snowflake.connector import DictCursor
        cur = self._connection.cursor(DictCursor)
        try:
            cur.execute("SELECT last_sync_end FROM sync_state WHERE entity = %s", (entity,))
            row = cur.fetchone()
            if not row or not row.get("LAST_SYNC_END"):
                return None
            v = row["LAST_SYNC_END"]
            return datetime.fromisoformat(v) if isinstance(v, str) else v
        except Exception as e:
            logger.warning(f"get_state({entity}) failed: {e}")
            return None

    def update_state(self, entity: str, last_sync_end: datetime, record_count: int) -> None:
        cur = self._connection.cursor()
        cur.execute(
            """MERGE INTO sync_state target
               USING (SELECT %s AS entity, %s AS last_sync_end,
                             %s AS record_count, %s AS updated_at) source
               ON target.entity = source.entity
               WHEN MATCHED THEN UPDATE SET
                 last_sync_end = source.last_sync_end,
                 record_count  = target.record_count + source.record_count,
                 updated_at    = source.updated_at
               WHEN NOT MATCHED THEN INSERT
                 (entity, last_sync_end, record_count, updated_at)
                 VALUES (source.entity, source.last_sync_end,
                         source.record_count, source.updated_at)
            """,
            (entity, last_sync_end.isoformat(), record_count, datetime.utcnow().isoformat()),
        )
        self._connection.commit()

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

        features = self.config.get("features", {})
        idempotent = features.get("enable_idempotent_upserts", True)
        sf_query = self.config.get("snowflake", {}).get("query", {})
        batch_size = sf_query.get("batch_insert_size", 100)
        cur = self._connection.cursor()

        written = 0
        for i in range(0, len(records), batch_size):
            chunk = records[i : i + batch_size]
            if idempotent:
                for d in chunk:
                    cur.execute(
                        """MERGE INTO audit_logs target
                           USING (SELECT %s AS audit_id, %s AS entity,
                                         PARSE_JSON(%s) AS changes,
                                         %s AS processed_at, %s AS run_id) source
                           ON target.audit_id = source.audit_id
                           WHEN MATCHED THEN UPDATE SET
                             changes = source.changes,
                             processed_at = source.processed_at,
                             run_id = source.run_id
                           WHEN NOT MATCHED THEN INSERT
                             (audit_id, entity, changes, processed_at, run_id)
                             VALUES (source.audit_id, source.entity,
                                     source.changes, source.processed_at, source.run_id)
                        """,
                        (
                            d.get("auditid", ""),
                            entity,
                            json.dumps(d, default=str),
                            datetime.utcnow().isoformat(),
                            run_id,
                        ),
                    )
                    written += 1
            else:
                cur.executemany(
                    """INSERT INTO audit_logs
                       (audit_id, entity, changes, processed_at, run_id)
                       SELECT column1, column2, PARSE_JSON(column3), column4, column5
                       FROM VALUES (%s, %s, %s, %s, %s)""",
                    [
                        (
                            d.get("auditid", ""),
                            entity,
                            json.dumps(d, default=str),
                            datetime.utcnow().isoformat(),
                            run_id,
                        )
                        for d in chunk
                    ],
                )
                written += len(chunk)

        self._connection.commit()
        return written
