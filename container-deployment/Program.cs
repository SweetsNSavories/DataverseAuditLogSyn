using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Cosmos;
using Newtonsoft.Json.Linq;

// Console app for bulk backlog processing of Dataverse audits
// Runs in Docker containers, processes large time windows (60 min) with entity parallelism

class Program
{
    private static readonly HttpClient Client = new HttpClient();
    private static CosmosClient CosmosDbClient;
    private static Container AuditsContainer;
    private static Container StateContainer;

    static async Task Main(string[] args)
    {
        try
        {
            // Environment variables
            string dataverseOrgUrl = Environment.GetEnvironmentVariable("DATAVERSE_ORG_URL") ?? "https://yourorg.crm.dynamics.com";
            string clientId = Environment.GetEnvironmentVariable("CLIENT_ID");
            string clientSecret = Environment.GetEnvironmentVariable("CLIENT_SECRET");
            string cosmosConnectionString = Environment.GetEnvironmentVariable("COSMOS_CONNECTION_STRING");
            string isBacklogMode = Environment.GetEnvironmentVariable("BACKLOG_MODE") ?? "true";
            string overrideStartTime = Environment.GetEnvironmentVariable("OVERRIDE_START_TIME");

            bool backlogMode = bool.Parse(isBacklogMode);
            int windowSizeMinutes = backlogMode ? 60 : 10;

            Console.WriteLine($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] Starting Dataverse Audit Sync");
            Console.WriteLine($"Backlog Mode: {backlogMode}, Window Size: {windowSizeMinutes} min");

            // Initialize Cosmos DB
            CosmosDbClient = new CosmosClient(cosmosConnectionString);
            Database database = CosmosDbClient.GetDatabase("AuditDb");
            AuditsContainer = database.GetContainer("Audits");
            StateContainer = database.GetContainer("SyncState");

            // Get OAuth token
            string token = await GetDataverseToken(clientId, clientSecret);
            Client.DefaultRequestHeaders.Authorization = new System.Net.Http.Headers.AuthenticationHeaderValue("Bearer", token);

            // Entity types to process
            string[] entities = { "Account", "Contact", "Case" };

            // Process each entity in parallel
            var tasks = entities.Select(entity => ProcessEntityAsync(
                entity, dataverseOrgUrl, windowSizeMinutes, backlogMode, overrideStartTime)).ToList();

            await Task.WhenAll(tasks);

            Console.WriteLine($"[{DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}] Audit sync completed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ERROR] {ex.Message}");
            Environment.Exit(1);
        }
    }

    private static async Task<string> GetDataverseToken(string clientId, string clientSecret)
    {
        var tokenRequest = new HttpRequestMessage(HttpMethod.Post, "https://login.microsoftonline.com/common/oauth2/v2.0/token");
        var content = new FormUrlEncodedContent(new[]
        {
            new KeyValuePair<string, string>("client_id", clientId),
            new KeyValuePair<string, string>("client_secret", clientSecret),
            new KeyValuePair<string, string>("scope", "https://org.dynamics.com/.default"),
            new KeyValuePair<string, string>("grant_type", "client_credentials")
        });
        tokenRequest.Content = content;

        var response = await Client.SendAsync(tokenRequest);
        var json = await response.Content.ReadAsStringAsync();
        var token = JObject.Parse(json)["access_token"].ToString();
        return token;
    }

    private static async Task ProcessEntityAsync(string entity, string dataverseOrgUrl, int windowSizeMinutes, 
        bool backlogMode, string overrideStartTime)
    {
        Console.WriteLine($"[{entity}] Starting processing...");

        try
        {
            // Get last sync time from state
            string stateId = $"sync_state_{entity}";
            ItemResponse<dynamic> stateItem = null;
            DateTime lastSyncEnd = DateTime.UtcNow.AddMinutes(-windowSizeMinutes);

            try
            {
                stateItem = await StateContainer.ReadItemAsync<dynamic>(stateId, new PartitionKey(stateId));
                lastSyncEnd = DateTime.Parse(stateItem.Resource.lastSyncEnd);
            }
            catch { }

            // Override start time if provided
            if (!string.IsNullOrEmpty(overrideStartTime) && DateTime.TryParse(overrideStartTime, out var overrideTime))
            {
                lastSyncEnd = overrideTime;
            }

            // Process windows
            while (true)
            {
                DateTime windowStart = lastSyncEnd;
                DateTime windowEnd = windowStart.AddMinutes(windowSizeMinutes);

                if (windowEnd > DateTime.UtcNow)
                    break;

                await ProcessWindowAsync(entity, dataverseOrgUrl, windowStart, windowEnd, stateId);
                lastSyncEnd = windowEnd;

                if (!backlogMode)
                    break;
            }

            Console.WriteLine($"[{entity}] Processing completed");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[{entity}] ERROR: {ex.Message}");
        }
    }

    private static async Task ProcessWindowAsync(string entity, string dataverseOrgUrl, DateTime windowStart, 
        DateTime windowEnd, string stateId)
    {
        Console.WriteLine($"[{entity}] Processing window {windowStart:yyyy-MM-dd HH:mm} to {windowEnd:yyyy-MM-dd HH:mm}");

        // Query audits via FetchXml
        string fetchXml = $@"
<fetch version='1.0' page='1' paging-cookie='' >
  <entity name='audit' >
    <attribute name='auditid' />
    <attribute name='objectid' />
    <attribute name='operation' />
    <attribute name='createdon' />
    <filter type='and' >
      <condition attribute='createdon' operator='ge' value='{windowStart:yyyy-MM-ddTHH:mm:ssZ}' />
      <condition attribute='createdon' operator='lt' value='{windowEnd:yyyy-MM-ddTHH:mm:ssZ}' />
      <condition attribute='objectid' operator='in' >
        <value uitype='{entity}'></value>
      </condition>
    </filter>
  </entity>
</fetch>";

        var audits = await FetchAuditsWithPaginationAsync(dataverseOrgUrl, fetchXml);

        // Fetch details for each audit (batched with retry)
        foreach (var auditId in audits)
        {
            var details = await FetchAuditDetailsWithRetryAsync(dataverseOrgUrl, auditId);
            
            if (details != null)
            {
                var auditDoc = new
                {
                    id = auditId,
                    entity = entity,
                    runId = Guid.NewGuid().ToString(),
                    changes = details,
                    processedAt = DateTime.UtcNow
                };

                await AuditsContainer.UpsertItemAsync(auditDoc, new PartitionKey(entity));
            }
        }

        // Update state (atomic)
        var stateUpdate = new
        {
            id = stateId,
            entity = entity,
            lastSyncEnd = windowEnd,
            recordCount = audits.Count
        };

        await StateContainer.UpsertItemAsync(stateUpdate, new PartitionKey(stateId));
    }

    private static async Task<List<string>> FetchAuditsWithPaginationAsync(string dataverseOrgUrl, string fetchXml)
    {
        var audits = new List<string>();
        string pagingCookie = null;
        int pageNumber = 1;

        while (true)
        {
            var request = new HttpRequestMessage(HttpMethod.Get, 
                $"{dataverseOrgUrl}/api/data/v9.2/audits?$filter=createdon ge 2026-01-01T00:00:00Z&$select=auditid&$top=5000");

            var response = await Client.SendAsync(request);
            var json = await response.Content.ReadAsStringAsync();
            var result = JObject.Parse(json);

            foreach (var item in result["value"])
            {
                audits.Add(item["auditid"].ToString());
            }

            pagingCookie = result["@odata.nextLink"]?.ToString();
            if (string.IsNullOrEmpty(pagingCookie))
                break;

            pageNumber++;
        }

        return audits;
    }

    private static async Task<Dictionary<string, object>> FetchAuditDetailsWithRetryAsync(string dataverseOrgUrl, string auditId)
    {
        for (int attempt = 1; attempt <= 3; attempt++)
        {
            try
            {
                var request = new HttpRequestMessage(HttpMethod.Post, 
                    $"{dataverseOrgUrl}/api/data/v9.2/RetrieveAuditDetails");
                
                var body = new { auditId = auditId, propertySet = new[] { "name", "telephone1", "address1_city" } };
                request.Content = new StringContent(Newtonsoft.Json.JsonConvert.SerializeObject(body), 
                    System.Text.Encoding.UTF8, "application/json");

                var response = await Client.SendAsync(request);
                var json = await response.Content.ReadAsStringAsync();
                var result = JObject.Parse(json);

                return result["AuditRecord"].ToObject<Dictionary<string, object>>();
            }
            catch (Exception ex) when (attempt < 3)
            {
                int delayMs = (int)Math.Pow(2, attempt - 1) * 1000;
                await Task.Delay(delayMs);
            }
        }

        return null;
    }
}
