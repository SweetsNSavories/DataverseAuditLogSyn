using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace DataverseAuditJob
{
    class Program
    {
        static async Task Main(string[] args)
        {
            Console.WriteLine("=== Dataverse Audit Job Started ===");

            // Example: List of entities to audit
            var entities = new[] { "account", "contact" };
            var windowSize = TimeSpan.FromMinutes(5);
            var repository = new InMemoryRepository(); // Replace with real implementation

            var tasks = entities.Select(entity =>
                Task.Run(() => ProcessEntityAsync(entity, windowSize, repository))
            ).ToArray();

            await Task.WhenAll(tasks);
            Console.WriteLine("=== All Entities Processed ===");
        }

        static async Task ProcessEntityAsync(string entityName, TimeSpan windowSize, IRepository repository)
        {
            // Thread isolation: each entity runs in its own thread
            var now = DateTime.UtcNow;
            var lastWindow = await repository.GetLastWindowSliceAsync(entityName);
            var start = lastWindow?.End ?? now.AddHours(-1); // Start 1 hour ago if no record

            while (start < now)
            {
                var end = start.Add(windowSize);
                var slice = new WindowSlice
                {
                    EntityName = entityName,
                    Start = start,
                    End = end,
                    Status = WindowStatus.Started
                };
                await repository.PersistWindowSliceAsync(slice);

                // Step 1: Retrieve audit entries (with paging & retry)
                var auditEntries = await FetchAuditEntriesWithRetryAsync(entityName, start, end);

                // Step 2: For each audit entry, fetch details (with retry)
                var details = new List<AuditDetail>();
                foreach (var entry in auditEntries)
                {
                    var detail = await FetchAuditDetailWithRetryAsync(entry);
                    details.Add(detail);
                }

                // Step 3: Bulk persist results
                await repository.BulkPersistAuditDetailsAsync(details);

                // Step 4: Mark slice completed
                slice.Status = WindowStatus.Completed;
                await repository.PersistWindowSliceAsync(slice);

                start = end;
            }
        }

        // Simulated: Fetch audit entries with paging and retry
        static async Task<List<AuditEntry>> FetchAuditEntriesWithRetryAsync(string entity, DateTime start, DateTime end)
        {
            var retries = 3;
            for (int attempt = 1; attempt <= retries; attempt++)
            {
                try
                {
                    // TODO: Implement paging for large result sets
                    await Task.Delay(100); // Simulate network
                    return new List<AuditEntry> { new AuditEntry { AuditId = Guid.NewGuid(), EntityName = entity, CreatedOn = start.AddMinutes(1) } };
                }
                catch (Exception)
                {
                    if (attempt == retries) throw;
                    await Task.Delay(500 * attempt); // Exponential backoff
                }
            }
            return new List<AuditEntry>();
        }

        // Simulated: Fetch audit detail with retry
        static async Task<AuditDetail> FetchAuditDetailWithRetryAsync(AuditEntry entry)
        {
            var retries = 3;
            for (int attempt = 1; attempt <= retries; attempt++)
            {
                try
                {
                    await Task.Delay(50); // Simulate network
                    return new AuditDetail { AuditId = entry.AuditId, ChangedFields = new[] { "name" }, OldValues = new[] { "old" }, NewValues = new[] { "new" } };
                }
                catch (Exception)
                {
                    if (attempt == retries) throw;
                    await Task.Delay(500 * attempt);
                }
            }
            return null;
        }
    }

    // Window tracking
    public class WindowSlice
    {
        public string EntityName { get; set; }
        public DateTime Start { get; set; }
        public DateTime End { get; set; }
        public WindowStatus Status { get; set; }
    }
    public enum WindowStatus { Started, Completed }

    // Audit entry
    public class AuditEntry
    {
        public Guid AuditId { get; set; }
        public string EntityName { get; set; }
        public DateTime CreatedOn { get; set; }
    }

    // Audit detail
    public class AuditDetail
    {
        public Guid AuditId { get; set; }
        public string[] ChangedFields { get; set; }
        public string[] OldValues { get; set; }
        public string[] NewValues { get; set; }
    }

    // Repository interface
    public interface IRepository
    {
        Task<WindowSlice> GetLastWindowSliceAsync(string entityName);
        Task PersistWindowSliceAsync(WindowSlice slice);
        Task BulkPersistAuditDetailsAsync(IEnumerable<AuditDetail> details);
    }

    // Example in-memory repository (replace with real DB or storage)
    public class InMemoryRepository : IRepository
    {
        private readonly ConcurrentDictionary<string, List<WindowSlice>> _windows = new();
        public Task<WindowSlice> GetLastWindowSliceAsync(string entityName)
        {
            _windows.TryGetValue(entityName, out var list);
            return Task.FromResult(list?.OrderByDescending(w => w.End).FirstOrDefault());
        }
        public Task PersistWindowSliceAsync(WindowSlice slice)
        {
            var list = _windows.GetOrAdd(slice.EntityName, _ => new List<WindowSlice>());
            lock (list) { list.RemoveAll(w => w.Start == slice.Start); list.Add(slice); }
            return Task.CompletedTask;
        }
        public Task BulkPersistAuditDetailsAsync(IEnumerable<AuditDetail> details)
        {
            // Simulate persistence
            return Task.CompletedTask;
        }
    }
}
