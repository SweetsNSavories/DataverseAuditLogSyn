import azure.functions as func
import logging
from datetime import datetime
from audit_sync_function import main

# Timer-triggered function: runs every 10 minutes
# Cron: 0 */10 * * * *

timer_blueprint = func.Blueprint()

@timer_blueprint.timer_trigger(arg_name="myTimer", schedule="0 */10 * * * *")
def AuditSyncTimer(myTimer: func.TimerRequest) -> None:
    """
    Timer trigger: runs every 10 minutes (0 */10 * * * *)
    Captures new Dataverse audits and syncs to Snowflake
    """
    try:
        logging.info(f"AuditSync timer trigger at {datetime.utcnow().isoformat()}")
        main(myTimer)
    except Exception as e:
        logging.error(f"Error in AuditSyncTimer: {e}")
        raise
