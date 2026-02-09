# Dataverse Resilient Auditing Job

## Overview
This C# console application implements a resilient, windowed auditing job for Microsoft Dataverse. It retrieves entity record changes from the Audit table, running one thread per entity and processing changes in 5-minute sliding windows.

## How It Works
- **Thread Isolation:** Each entity is processed in its own thread for isolation and parallelism.
- **Windowed Processing:** For each 5-minute window:
  1. Persist a "Started" record (entity, window start/end).
  2. Retrieve all Audit entries for the entity and window (with paging and retry).
  3. For each entry, fetch field-level change details (with retry).
  4. Hold all change details in memory until the window is processed.
  5. Bulk persist results and mark the window "Completed".
  6. Only then advance to the next window.
- **Crash Recovery:** If the job crashes, it reprocesses the last incomplete window to ensure no data loss or duplicates.

## Key Classes
- `WindowSlice`: Tracks window start/end, entity, and status.
- `AuditEntry`: Represents a single audit record.
- `AuditDetail`: Field-level change details for an audit entry.
- `IRepository`: Interface for persistence (window tracking, audit details).

## Best Practices
- **Paging:** Use paging when retrieving large audit result sets.
- **Retries:** Implement exponential backoff for network/API errors.
- **Thread Isolation:** Each entity runs in its own thread to avoid cross-entity interference.
- **Bulk Persistence:** Persist audit details in bulk for efficiency.
- **Idempotency:** Reprocessing incomplete windows ensures no data loss or duplication.

## Usage
1. Configure the list of entities to audit in `Program.cs`.
2. Implement `IRepository` for your storage (DB, file, etc.).
3. Run the application. It will process all entities in parallel, window by window.

## Extending
- Replace `InMemoryRepository` with a real implementation.
- Integrate with Dataverse SDK for actual Audit/AuditDetail retrieval.
- Tune window size and retry logic as needed.

---

**This job ensures reliable, repeatable, and scalable audit extraction from Dataverse, even in the face of failures.**
