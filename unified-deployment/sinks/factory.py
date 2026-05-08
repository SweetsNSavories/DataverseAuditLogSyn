"""Sink factory - returns an AuditSink concrete implementation based on config."""

from __future__ import annotations

from typing import Dict

from .base import AuditSink, SinkError


def get_sink(config: Dict) -> AuditSink:
    """
    Read `config["sink"]["type"]` and return the matching sink instance.

    Imports are deferred so users can install only the dependencies they need
    (e.g. snowflake-connector-python is not required if the customer picks
    cosmos or adls).
    """
    sink_cfg = config.get("sink") or {}
    sink_type = (sink_cfg.get("type") or "snowflake").lower().strip()

    if sink_type == "snowflake":
        from .snowflake_sink import SnowflakeSink
        return SnowflakeSink(config)

    if sink_type in ("cosmos", "cosmosdb", "azure-cosmos"):
        from .cosmos_sink import CosmosSink
        return CosmosSink(config)

    if sink_type in ("adls", "adls2", "adlsgen2", "datalake", "onelake", "fabric"):
        from .adls_parquet_sink import ADLSParquetSink
        return ADLSParquetSink(config)

    if sink_type in ("noop", "none", "dryrun", "dry-run", "stub"):
        # Smoke-test sink - prints what it would write. Not for production.
        from .noop_sink import NoopSink
        return NoopSink()

    raise SinkError(
        f"Unknown sink type '{sink_type}'. "
        f"Valid options: snowflake | cosmos | adls | onelake | noop"
    )
