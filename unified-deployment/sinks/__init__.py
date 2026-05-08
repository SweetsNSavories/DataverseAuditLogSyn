"""
Pluggable storage sinks for Dataverse Audit Sync.

Customers can choose where audit data lands:

  - snowflake : Snowflake (existing)
  - cosmos    : Azure Cosmos DB (NoSQL API) - low-latency lookups, elastic scale,
                hierarchical partition keys for tenant/entity isolation
  - adls      : Azure Data Lake Storage Gen2 (Parquet) - cheap analytics, lakehouse-ready
  - onelake   : Microsoft Fabric OneLake (Parquet) - same protocol as ADLS Gen2,
                lands directly in a Lakehouse for Fabric notebooks/SQL/Power BI

All sinks implement the same interface (`AuditSink`) so the orchestrator in
main.py is sink-agnostic.
"""

from .base import AuditSink, SinkError, SinkPartialWriteError
from .factory import get_sink

__all__ = ["AuditSink", "SinkError", "SinkPartialWriteError", "get_sink"]
