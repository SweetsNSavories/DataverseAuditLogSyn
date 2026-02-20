# Function App Deployment - Continuous Real-Time Sync

This folder contains an Azure Functions solution for continuous real-time capture of new Dataverse audits every 10 minutes.

## Overview

Deploy a serverless Azure Function App with timer trigger (every 10 minutes) to capture new audits as they occur, indefinitely.

## Prerequisites

- Azure Subscription with Function App and Cosmos DB
- Dataverse environment with App Registration
- Azure CLI or Visual Studio Code with Azure Functions extension
- .NET 8.0 SDK

## Configuration

Update `local.settings.json` with your credentials:

```json
{
  "Values": {
    "DATAVERSE_ORG_URL": "https://yourorg.crm.dynamics.com",
    "CLIENT_ID": "your-client-id",
    "CLIENT_SECRET": "your-secret",
    "COSMOS_CONNECTION_STRING": "your-cosmos-connection-string"
  }
}
```

## Local Development

```bash
# Install Azure Functions Core Tools
# https://learn.microsoft.com/azure/azure-functions/functions-run-local

# Start function locally
func start

# Test timer trigger (creates in-memory timer event)
# Default schedule: every 10 minutes
```

## Azure Deployment

### Option 1: Visual Studio Code

1. Open Command Palette (Ctrl+Shift+P)
2. "Azure Functions: Deploy to Function App"
3. Select subscription and function app

### Option 2: Azure CLI

```bash
# Create resource group
az group create --name audit-sync-rg --location eastus

# Create storage account (required for Function App)
az storage account create \
  --resource-group audit-sync-rg \
  --name auditsyncstg \
  --sku Standard_LRS

# Create Function App (Consumption plan)
az functionapp create \
  --resource-group audit-sync-rg \
  --consumption-plan-location eastus \
  --runtime dotnet-isolated \
  --runtime-version 8.0 \
  --functions-version 4 \
  --name dataverse-audit-sync \
  --storage-account auditsyncstg

# Set application settings
az functionapp config appsettings set \
  --resource-group audit-sync-rg \
  --name dataverse-audit-sync \
  --settings \
    DATAVERSE_ORG_URL=https://yourorg.crm.dynamics.com \
    CLIENT_ID=$CLIENT_ID \
    CLIENT_SECRET=$CLIENT_SECRET \
    COSMOS_CONNECTION_STRING=$COSMOS_CONNECTION_STRING

# Deploy code
func azure functionapp publish dataverse-audit-sync --build remote
```

### Option 3: GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy

on: [push]

jobs:
  build-and-deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-dotnet@v3
        with:
          dotnet-version: '8.0'
      - run: cd function-app-deployment && dotnet build
      - uses: Azure/functions-action@v1
        with:
          app-name: dataverse-audit-sync
          package: function-app-deployment
          publish-profile: ${{ secrets.AZURE_FUNCTIONAPP_PUBLISH_PROFILE }}
```

## Timer Schedule

Default cron expression: `0 */10 * * * *` (every 10 minutes)

Modify in `AuditSyncFunction.cs`:
```csharp
[TimerTrigger("0 */10 * * * *")]  // Every 10 minutes
[TimerTrigger("0 0 * * * *")]     // Daily at midnight
```

## Monitoring

### Application Insights

1. Open Azure Portal → Function App → Application Insights
2. View logs, metrics, traces in real-time
3. Create alerts for failures

### Azure CLI

```bash
# View recent invocations
az functionapp log tail --name dataverse-audit-sync \
  --resource-group audit-sync-rg

# View function metrics
az monitor metrics list \
  --resource /subscriptions/{sub}/resourceGroups/audit-sync-rg/providers/Microsoft.Web/sites/dataverse-audit-sync
```

## Cost

**Execution**: ~4,320 runs/month × $0.20 per million = $0.86  
**Compute**: ~36,000 GB-s/month (30s × 4,320) × $0.000016 = $0.58  
**Cosmos DB writes**: ~20k audits/month = $10  
**Cosmos DB reads**: ~$3  

**Total: ~$16/month**

## Troubleshooting

- **Function not triggering**: Check timer trigger syntax in Azure Portal → Function App → Functions → Timer trigger
- **OAuth errors**: Verify Client ID/Secret and App Registration permissions
- **Cosmos DB throttling**: Check request units (RU/s), increase if needed
- **Timeout**: Default is 10 minutes, adjust `host.json` if needed

## Cleanup

```bash
az group delete --name audit-sync-rg --yes
```
