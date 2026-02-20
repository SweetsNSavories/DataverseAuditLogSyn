using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Azure.Cosmos;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json.Linq;

// Azure Functions timer-triggered function for continuous real-time audit sync
// Runs every 10 minutes, processes 10-minute time windows, captures new audits

public static class AuditSyncFunction
{
    private static readonly HttpClient Client = new HttpClient();
    private static CosmosClient CosmosDbClient;
    private static Container AuditsContainer;
    private static Container StateContainer;

    [Function("AuditSyncTimer")]
    public static async Task Run(
        [TimerTrigger("0 */10 * * * *")] TimerInfo myTimer,
        ILogger log)
    {
        try
        {
            log.LogInformation($"AuditSync function started at {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}");

            // Environment variables
            string dataverseOrgUrl = Environment.GetEnvironmentVariable("DATAVERSE_ORG_URL") ?? "https://yourorg.crm.dynamics.com";
            string clientId = Environment.GetEnvironmentVariable("CLIENT_ID");
            string clientSecret = Environment.GetEnvironmentVariable("CLIENT_SECRET");
            string cosmosConnectionString = Environment.GetEnvironmentVariable("COSMOS_CONNECTION_STRING");

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

            // Process each entity (sequential for continuous phase)
            foreach (string entity in entities)
            {
                await ProcessEntityAsync(entity, dataverseOrgUrl, log);
            }

            log.LogInformation($"AuditSync function completed at {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}");
        }
        catch (Exception ex)
        {
            log.LogError($"AuditSync function error: {ex.Message}");
            throw;
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

    private static async Task ProcessEntityAsync(string entity, string dataverseOrgUrl, ILogger log)
    {
        try
        {
            // Get last sync time from state (10-minute window)
            string stateId = $"sync_state_{entity}";
            DateTime lastSyncEnd = DateTime.UtcNow.AddMinutes(-10);

            try
            {
                ItemResponse<dynamic> stateItem = await StateContainer.ReadItemAsync<dynamic>(stateId, new PartitionKey(stateId));
                lastSyncEnd = DateTime.Parse(stateItem.Resource.lastSyncEnd);
            }
            catch { }

            DateTime windowStart = lastSyncEnd;
            DateTime windowEnd = DateTime.UtcNow;

            log.LogInformation($"[{entity}] Processing window {windowStart:yyyy-MM-dd HH:mm} to {windowEnd:yyyy-MM-dd HH:mm}");

            // Query audits
            var audits = await FetchAuditsWithPaginationAsync(dataverseOrgUrl, windowStart, windowEnd, entity);

            // Fetch details and store
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
            log.LogInformation($"[{entity}] Processed {audits.Count} audits");
        }
        catch (Exception ex)
        {
            log.LogError($"[{entity}] Error: {ex.Message}");
        }
    }

    private static async Task<List<string>> FetchAuditsWithPaginationAsync(string dataverseOrgUrl, DateTime windowStart, 
        DateTime windowEnd, string entity)
    {
        var audits = new List<string>();

        var filterQuery = $"createdon ge {windowStart:yyyy-MM-ddTHH:mm:ssZ} and createdon lt {windowEnd:yyyy-MM-ddTHH:mm:ssZ}";
        var request = new HttpRequestMessage(HttpMethod.Get, 
            $"{dataverseOrgUrl}/api/data/v9.2/audits?$filter={Uri.EscapeDataString(filterQuery)}&$select=auditid&$top=5000");

        var response = await Client.SendAsync(request);
        var json = await response.Content.ReadAsStringAsync();
        var result = JObject.Parse(json);

        foreach (var item in result["value"])
        {
            audits.Add(item["auditid"].ToString());
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
