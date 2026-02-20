# Function App Deployment - Continuous Real-Time Sync (Python + Snowflake)

Azure Functions timer-triggered function. Runs every 10 minutes to capture new Dataverse audits and sync to Snowflake indefinitely.

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
      "attributes": ["name", "telephone1", "address1_city"]
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

## Prerequisites

- Azure Subscription with Function App
- Snowflake account with warehouse
- Dataverse environment + App Registration
- Python 3.11+
- Azure Functions Core Tools

## Local Development

### Setup

```bash
# Install Azure Functions Core Tools
# https://learn.microsoft.com/en-us/azure/azure-functions/functions-run-local

# Install Python dependencies
pip install -r requirements.txt

# Copy config template and update credentials
cp local.settings.json .env
# Edit .env with your credentials
```

### Run Locally

```bash
# Start function locally
func start

# Should see output:
# Listening on http://localhost:7071/
# Azure Functions Core Tools started...
```

### Test Timer Trigger

Function will simulate 10-minute intervals. Watch logs for execution.

## Configuration

Update `local.settings.json` with Snowflake credentials:

```json
{
  "Values": {
    "DATAVERSE_ORG_URL": "https://yourorg.crm.dynamics.com",
    "CLIENT_ID": "your-app-id",
    "CLIENT_SECRET": "your-secret",
    "SNOWFLAKE_USER": "your-user",
    "SNOWFLAKE_PASSWORD": "your-password",
    "SNOWFLAKE_ACCOUNT": "xy12345.us-east-1",
    "SNOWFLAKE_WAREHOUSE": "COMPUTE_WH"
  }
}
```

### Customize config.json

Change which entities and attributes are tracked:

```json
{
  "windowSizeMinutes": {"backlog": 60, "continuous": 10},
  "entities": [
    {
      "name": "Lead",
      "attributes": ["firstname", "lastname", "emailaddress1"]
    },
    {
      "name": "Quote",
      "attributes": ["name", "totalamount", "quoteid"]
    }
  ]
}
```

## Azure Deployment

### Option 1: Visual Studio Code

1. Install Azure Functions extension
2. Open Command Palette (Ctrl+Shift+P)
3. "Azure Functions: Deploy to Function App"
4. Select subscription and function app

### Option 2: Azure CLI

```bash
# Create resource group
az group create --name audit-sync-rg --location eastus

# Create storage account (required)
az storage account create \
  --resource-group audit-sync-rg \
  --name auditsyncstg \
  --sku Standard_LRS

# Create Function App (Consumption plan, Python runtime)
az functionapp create \
  --resource-group audit-sync-rg \
  --consumption-plan-location eastus \
  --runtime python \
  --runtime-version 3.11 \
  --functions-version 4 \
  --name dataverse-audit-sync-func \
  --storage-account auditsyncstg

# Set application settings
az functionapp config appsettings set \
  --resource-group audit-sync-rg \
  --name dataverse-audit-sync-func \
  --settings \
    DATAVERSE_ORG_URL=https://yourorg.crm.dynamics.com \
    CLIENT_ID=$CLIENT_ID \
    CLIENT_SECRET=$CLIENT_SECRET \
    SNOWFLAKE_USER=$SNOWFLAKE_USER \
    SNOWFLAKE_PASSWORD=$SNOWFLAKE_PASSWORD \
    SNOWFLAKE_ACCOUNT=$SNOWFLAKE_ACCOUNT \
    SNOWFLAKE_WAREHOUSE=$SNOWFLAKE_WAREHOUSE

# Deploy code
func azure functionapp publish dataverse-audit-sync-func --build remote
```

### Option 3: GitHub Actions

Create `.github/workflows/deploy.yml`:

```yaml
name: Deploy Function App

on: [push]

jobs:
  deploy:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - run: |
          cd function-app-deployment-python
          pip install -r requirements.txt
      - uses: Azure/functions-action@v1
        with:
          app-name: dataverse-audit-sync-func
          package: function-app-deployment-python
          publish-profile: ${{ secrets.AZURE_FUNCTIONAPP_PUBLISH_PROFILE }}
```

## Timer Schedule

Default: Every 10 minutes (`0 */10 * * * *`)

Modify in `audit_sync_timer.py`:

```python
@timer_blueprint.timer_trigger(arg_name="myTimer", schedule="0 */10 * * * *")  # Every 10 min
@timer_blueprint.timer_trigger(arg_name="myTimer", schedule="0 0 * * * *")     # Daily
```

## Monitoring

### Application Insights

```bash
# View logs in real-time
az monitor log-analytics query \
  --workspace $(az monitor log-analytics workspace list --resource-group audit-sync-rg --query [0].id -o tsv) \
  --analytics-query "FunctionAppLogs | where FunctionName == 'AuditSyncTimer' | tail 20"
```

### Azure Portal

1. Go to Function App → Monitor
2. View invocation count, success rate, execution time
3. Set up alerts for failures

### Local Logging

```bash
func start --verbose
```

## Cost

**Executions**: ~4,320 runs/month × $0.20 per million = $0.86  
**Compute**: ~36,000 GB-s/month × $0.000016 = $0.58  
**Snowflake compute**: ~8 credits/month = $16  
**Snowflake storage**: ~$2  

**Total: ~$19/month** (vs ~$850/month for always-on container)

## Troubleshooting

### Function not triggered
- Check timer trigger syntax
- Verify function status in Portal (should show "Enabled")
- Check Application Insights logs

### OAuth errors
- Verify credentials in Application Settings
- Check App Registration has Dataverse API permissions
- Ensure secret hasn't expired

### Snowflake errors
- Test connection: `snowsql -c <connection-name>`
- Verify database `AUDIT_DB` exists
- Check network access (may need firewall rules)

### Timeout errors
- Default timeout: 10 minutes (set in `host.json`)
- Check if 10 minutes enough for typical sync
- Increase if needed: `"functionTimeout": "00:15:00"`

### Custom config not loading
- Verify `config.json` is in deployment package
- Check logs for JSON parsing errors
- Ensure entities exist in Dataverse

## Cleanup

```bash
az group delete --resource-group audit-sync-rg --yes
```

## Architecture

```
Dataverse (audits created) 
    ↓
Function App (timer trigger every 10 min)
    ↓
Snowflake (AUDITS table - incremental sync)
```

Same resilience as container version:
- On failure, restarts and reprocesses same 10-minute window
- Idempotent upserts prevent duplicates
- Atomic state updates prevent partial syncs
- Configuration driven by config.json
