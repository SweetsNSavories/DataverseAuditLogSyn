"""
Azure Function wrapper - reuses the unified main.py
====================================================

This is a thin wrapper that lets you deploy the SAME unified main.py code
as an Azure Function with a timer trigger.

Behavior:
- Runs every 10 minutes (configurable in function.json or below)
- Sets exit_when_caught_up=true (function should exit, not loop)
- Same adaptive backlog → live behavior, just chunked into function invocations
"""

import logging
import asyncio
import os
from datetime import datetime
import azure.functions as func

# Force exit_when_caught_up for function deployments BEFORE importing main
# (Azure Functions should exit after each invocation, not loop forever)
os.environ["FUNCTION_MODE"] = "true"

from main import run_sync, CONFIG, logger

# Override config for function context
CONFIG["features"]["exit_when_caught_up"] = True


app = func.FunctionApp()


@app.schedule(schedule="0 */10 * * * *", arg_name="mytimer", run_on_startup=False, use_monitor=True)
async def audit_sync_timer(mytimer: func.TimerRequest) -> None:
    """
    Timer-triggered audit sync.
    
    Default: runs every 10 minutes.
    Adaptive: catches up backlog if behind, processes only new data if current.
    """
    if mytimer.past_due:
        logging.warning("Timer is past due!")
    
    logger.info(f"Azure Function triggered at {datetime.utcnow().isoformat()}")
    
    try:
        await run_sync(invocation_source="azure-function")
        logger.info("Azure Function completed successfully")
    except Exception as e:
        logger.error(f"Azure Function failed: {e}", exc_info=True)
        raise
