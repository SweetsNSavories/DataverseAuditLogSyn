# Container Deployment - Backlog Phase (Python + Snowflake)

Bulk historical audit sync to Snowflake. Processes 60-minute time windows in parallel Docker containers, one per entity (Account, Contact, Case).

## Configuration

### config.json - Fully Customizable

Edit `config.json` to control:
- **Window sizes** (backlog vs continuous)
- **Entities** to process
- **Attributes** to track per entity

Example `config.json`:

```json
{
  "windowSizeMinutes": {
    "backlog": 60,
    "continuous": 10
  },
  "entities": [
    {
      "name": "Account",
      "attributes": ["name", "telephone1", "address1_city", "address1_country"]
    },
    {
      "name": "Contact",
      "attributes": ["fullname", "emailaddress1", "mobilephone"]
    },
    {
      "name": "Case",
      "attributes": ["title", "description", "prioritycode"]
    }
  ]
}
```

### Environment Variables

Override or supplement config.json:

```bash
# Process only one entity (even if config.json has multiple)
ENTITY=Account

# Use backlog 60-min windows
BACKLOG_MODE=true

# Restart from specific time
OVERRIDE_START_TIME=2026-02-01T00:00:00Z
```

## Prerequisites

- Docker & Docker Compose
- Snowflake account with warehouse
- Dataverse environment + App Registration
- Azure CLI (for cloud deployment)

## Snowflake Setup

### Create Database & Tables

```sql
-- Connect to Snowflake
-- Create database
CREATE DATABASE IF NOT EXISTS AUDIT_DB;

-- Create audits table
CREATE TABLE IF NOT EXISTS AUDIT_DB.PUBLIC.AUDITS (
    AUDIT_ID STRING NOT NULL,
    ENTITY STRING NOT NULL,
    CHANGES VARIANT,
    PROCESSED_AT TIMESTAMP,
    RUN_ID STRING,
    CREATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    PRIMARY KEY (AUDIT_ID)
);

-- Create sync state table
CREATE TABLE IF NOT EXISTS AUDIT_DB.PUBLIC.SYNC_STATE (
    ENTITY STRING PRIMARY KEY,
    LAST_SYNC_END TIMESTAMP,
    RECORD_COUNT INTEGER,
    UPDATED_AT TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Create indexes
CREATE INDEX IF NOT EXISTS idx_entity_time ON AUDITS(ENTITY, CREATED_AT);
CREATE INDEX IF NOT EXISTS idx_run_id ON AUDITS(RUN_ID);
```

## Local Development

### Run Locally (Docker Compose)

```bash
# Build images
docker-compose build

# Start all 3 processors
docker-compose up -d

# View logs
docker-compose logs -f

# Stop
docker-compose down
```

### Customize for Your Entities

1. Edit `config.json`:
   ```json
   {
     "windowSizeMinutes": {"backlog": 60, "continuous": 10},
     "entities": [
       {
         "name": "Lead",
         "attributes": ["firstname", "lastname", "emailaddress1"]
       },
       {
         "name": "Opportunity",
         "attributes": ["name", "value", "stagecode"]
       }
     ]
   }
   ```

2. Restart containers:
   ```bash
   docker-compose down
   docker-compose up -d
   ```

### Run Specific Entity

```bash
docker run -e ENTITY=Account -e DATAVERSE_ORG_URL=... dataverse-audit-sync
```

## Azure Deployment

### Option 1: Azure Container Instances (ACI)

```bash
# Create resource group
az group create --name audit-sync-rg --location eastus

# Create container registry
az acr create --resource-group audit-sync-rg \
  --name auditsyncregistry --sku Basic

# Build and push image
az acr build --registry auditsyncregistry \
  --image dataverse-audit-sync:latest .

# Login to registry
az acr login --name auditsyncregistry

# Deploy Account processor
az container create \
  --resource-group audit-sync-rg \
  --name account-audit-processor \
  --image auditsyncregistry.azurecr.io/dataverse-audit-sync:latest \
  --environment-variables \
    ENTITY=Account \
    DATAVERSE_ORG_URL=https://yourorg.crm.dynamics.com \
    CLIENT_ID=$CLIENT_ID \
    CLIENT_SECRET=$CLIENT_SECRET \
    SNOWFLAKE_USER=$SNOWFLAKE_USER \
    SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD \
    SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT \
    SNOWFLAKE_WAREHOUSE=$SNOWFLAKE_WAREHOUSE \
    BACKLOG_MODE=true \
  --registry-login-server auditsyncregistry.azurecr.io \
  --registry-username $(az acr credential show --name auditsyncregistry --query username -o tsv) \
  --registry-password $(az acr credential show --name auditsyncregistry --query passwords[0].value -o tsv)

# Deploy Contact processor
az container create --resource-group audit-sync-rg \
  --name contact-audit-processor \
  --image auditsyncregistry.azurecr.io/dataverse-audit-sync:latest \
  --environment-variables ENTITY=Contact ...

# Deploy Case processor
az container create --resource-group audit-sync-rg \
  --name case-audit-processor \
  --image auditsyncregistry.azurecr.io/dataverse-audit-sync:latest \
  --environment-variables ENTITY=Case ...
```

### Monitor Containers

```bash
# View container status
az container list --resource-group audit-sync-rg --output table

# View logs
az container logs --resource-group audit-sync-rg --name account-audit-processor

# Stop container
az container stop --resource-group audit-sync-rg --name account-audit-processor

# Delete all
az group delete --resource-group audit-sync-rg --yes
```

## Cost Breakdown

| Item | Cost |
|------|------|
| 3 containers × $50/week × 1.5 weeks | $225 |
| Snowflake compute (4 credits × $2/credit) | $8 |
| Snowflake storage (1M audits ≈ 100MB) | $2 |
| **Total** | **~$235** |

(More cost-effective than Cosmos DB)

## Troubleshooting

### Container exits immediately
```bash
docker logs account-audit-processor
```

### OAuth token errors
- Verify `CLIENT_ID` and `CLIENT_SECRET`
- Check App Registration has Dataverse API permissions

### Snowflake connection fails
- Test credentials: `snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER`
- Verify database `AUDIT_DB` exists
- Check network access (may need firewall rules)

### No audits found
- Verify `DATAVERSE_ORG_URL` is correct
- Check audit data exists in Dataverse for time range
- Review logs for filter errors

### Custom attributes not working
- Verify attributes exist in Dataverse entity
- Check attribute names in `config.json` are exact (case-sensitive)
- View logs: `docker logs <container>`

## Recovery

If container crashes mid-window:
1. It will restart automatically (restart policy: unless-stopped)
2. On startup, reads `last_sync_end` from Snowflake
3. Reprocesses the same 60-minute window (idempotent via upsert)
4. No data loss, no duplicates

## Configuration Examples

### Add Custom Entity

Edit `config.json`:

```json
{
  "windowSizeMinutes": {"backlog": 60, "continuous": 10},
  "entities": [
    {
      "name": "Lead",
      "attributes": ["firstname", "lastname", "emailaddress1", "phonenumber"]
    }
  ]
}
```

### Faster Backlog Processing

Increase window size (fewer but larger windows):

```json
{
  "windowSizeMinutes": {"backlog": 180, "continuous": 10}
}
```

### Track More Attributes

```json
{
  "entities": [
    {
      "name": "Account",
      "attributes": [
        "name",
        "telephone1",
        "address1_city",
        "address1_country",
        "revenue",
        "industrycode",
        "employees"
      ]
    }
  ]
}
```

## Next Phase

Once backlog is complete (1-2 weeks), deploy **function-app-deployment-python** for continuous 10-minute sync.
