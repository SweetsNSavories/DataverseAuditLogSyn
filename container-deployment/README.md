# Container Deployment - Bulk Backlog Processing

This folder contains a Docker-based solution for capturing 5-10 years of historical Dataverse audit data in 1-2 weeks.

## Overview

Deploy 3-5 containers (Account, Contact, Case, etc.) in parallel, each processing its own entity type with 60-minute time windows for fast historical catch-up.

## Prerequisites

- Docker and Docker Compose installed
- Azure Subscription with Cosmos DB account
- Dataverse environment with App Registration created
- Environment variables configured

## Environment Variables

Create a `.env` file:

```
DATAVERSE_ORG_URL=https://yourorg.crm.dynamics.com
CLIENT_ID=your-app-registration-client-id
CLIENT_SECRET=your-app-registration-client-secret
COSMOS_CONNECTION_STRING=your-cosmos-db-connection-string
BACKLOG_MODE=true
```

## Local Testing

```bash
# Copy .env file
cp .env.example .env
# Edit .env with your credentials

# Start 3 parallel containers
docker-compose up -d

# View logs
docker-compose logs -f

# Stop containers
docker-compose down
```

## Azure Container Instances Deployment

```bash
# Create resource group
az group create --name audit-sync-rg --location eastus

# Create container registry
az acr create --resource-group audit-sync-rg \
  --name auditsyncregistry --sku Basic

# Build and push image
az acr build --registry auditsyncregistry \
  --image dataverse-audit:latest .

# Deploy Account processor
az container create \
  --resource-group audit-sync-rg \
  --name dataverse-audit-account \
  --image auditsyncregistry.azurecr.io/dataverse-audit:latest \
  --environment-variables \
    DATAVERSE_ORG_URL=https://yourorg.crm.dynamics.com \
    BACKLOG_MODE=true \
  --secure-environment-variables \
    CLIENT_ID=$CLIENT_ID \
    CLIENT_SECRET=$CLIENT_SECRET \
    COSMOS_CONNECTION_STRING=$COSMOS_CONNECTION_STRING

# Deploy Contact processor
# ... repeat with ENTITY=Contact

# Deploy Case processor
# ... repeat with ENTITY=Case

# Monitor
az container logs --resource-group audit-sync-rg \
  --name dataverse-audit-account --follow
```

## How It Works

1. **Window Processing**: Each container processes 60-minute time windows
2. **FetchXml Query**: Retrieves all audits in the window via Dataverse Web API
3. **Pagination**: Handles 100k+ audits via paging cookies
4. **Detail Fetch**: Calls RetrieveAuditDetails for field-level changes (batched, with retry)
5. **Cosmos DB Storage**: Upserts audit records and updates sync state (atomic)
6. **Crash Recovery**: On restart, detects incomplete windows and reprocesses from last checkpoint

## Cost

**3-5 containers × $50/week × 1-2 weeks = $325-425 total (one-time)**

## Troubleshooting

- **Container exits**: Check logs for missing environment variables
- **Dataverse API errors**: Verify App Registration permissions and OAuth token
- **Cosmos DB throttling**: Check RU/s settings, exponential backoff retries after 3 attempts
- **Duplicate records**: Idempotent upserts prevent duplicates

## Cleanup

Delete containers in Azure Portal or via Azure CLI:
```bash
az group delete --name audit-sync-rg --yes
```
