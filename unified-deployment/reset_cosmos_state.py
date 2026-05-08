"""
Reset Cosmos containers for the audit-sync demo. Deletes all docs from
`audit_logs` and `sync_state` so the next run does a fresh backlog from
OVERRIDE_START_TIME.

Usage:
    python reset_cosmos_state.py
"""
import os
import sys
from dotenv import load_dotenv  # type: ignore
load_dotenv(override=True)

from azure.cosmos import CosmosClient

ENDPOINT = os.environ["COSMOS_ENDPOINT"]
KEY = os.environ["COSMOS_KEY"]
DB = os.environ.get("COSMOS_DATABASE", "dataverse_audit")
AUDITS = os.environ.get("COSMOS_CONTAINER_AUDITS", "audit_logs")
STATE = os.environ.get("COSMOS_CONTAINER_STATE", "sync_state")


def purge_container(db, container_id: str, pk_paths: list[str]):
    container = db.get_container_client(container_id)
    docs = list(container.query_items(
        "SELECT c.id, " + ", ".join(f"c.{p.lstrip('/')}" for p in pk_paths) + " FROM c",
        enable_cross_partition_query=True,
    ))
    print(f"  Purging {len(docs)} docs from '{container_id}' ...")
    for d in docs:
        if len(pk_paths) == 1:
            pk_value = d.get(pk_paths[0].lstrip("/"))
        else:
            pk_value = [d.get(p.lstrip("/")) for p in pk_paths]
        try:
            container.delete_item(item=d["id"], partition_key=pk_value)
        except Exception as e:
            print(f"    skip {d['id']}: {e}")
    print(f"  done.")


def main():
    client = CosmosClient(ENDPOINT, credential=KEY)
    db = client.get_database_client(DB)
    purge_container(db, AUDITS, ["/entity", "/auditYearMonth"])
    purge_container(db, STATE, ["/entity"])
    print("Reset complete.")


if __name__ == "__main__":
    main()
