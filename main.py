import time
import logging
from datetime import datetime, time as dt_time
import schedule

# Import modular strategy files
import db_utils
import get_signals
import scanner
import enter
import exit
import config

# Setup logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("sidbot.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def is_market_open():
    """Checks if current time is within regular NYSE hours (9:30 AM - 4:00 PM EST)."""
    now = datetime.now().time()
    market_start = dt_time(9, 30)
    market_end = dt_time(16, 0)
    # weekday() < 5 ensures Monday (0) through Friday (4)
    return market_start <= now <= market_end and datetime.now().weekday() < 5


def run_exit_logic():
    """
    Performs nearly continuous monitoring for RSI 50 targets.
    Called by the main loop during market hours.
    """
    if is_market_open():
        logger.info("Executing periodic exit scan...")
        exit.monitor_and_execute_exits()


def run_prep_sequence():
    """
    Step 1: The 'Heavy Lifting' (3:30 PM).
    Syncs data, discovers signals, validates gates, and scores setups.
    """
    if not is_market_open():
        return

    logger.info("--- STARTING PREPARATION SEQUENCE (15:30) ---")

    # 1. Sync latest snapshots from Alpaca to Supabase
    db_utils.sync_latest_market_data()

    # 2. Discovery: Find current RSI extremes (<=30 or >=70)
    get_signals.populate_sid_extremes()

    # 3. Validation: Check momentum gates (Daily/Weekly RSI & MACD Slopes)
    scanner.validate_staged_signals()

    # 4. Scoring: Apply weights (Preferred list, SPY alignment, etc.)
    clients = db_utils.get_clients()
    scanner.score_and_validate_staged(clients['supabase_client'])

    logger.info("--- PREPARATION COMPLETE. SETUPS STAGED FOR 15:45 ---")


def run_execution_sequence():
    """
    Step 2: The 'Trigger' (3:45 PM).
    Executes Alpaca bracket orders for symbols marked 'is_ready'.
    """
    if not is_market_open():
        return

    logger.info("--- STARTING EXECUTION SEQUENCE (15:45) ---")
    enter.execute_sid_entries()
    logger.info("--- EXECUTION SEQUENCE COMPLETE ---")

def run_daily_maintenance():
    """Performs end-of-day database cleanup."""
    logger.info("--- STARTING DAILY MAINTENANCE ---")
    db_utils.cleanup_expired_signals()
    logger.info("--- MAINTENANCE COMPLETE ---")

# --- SCHEDULING ---

# 15:30: Data Sync and Signal Scoring
schedule.every().monday.to_friday().at("15:30").do(run_prep_sequence)

# 15:45: Final Entry Execution
schedule.every().monday.to_friday().at("15:45").do(run_execution_sequence)

# Add a 16:30 schedule for the cleanup script
schedule.every().monday.to_friday().at("16:30").do(run_daily_maintenance)


def main():
    logger.info("SidBot Orchestrator active on Ubuntu Droplet (EST).")
    logger.info(f"Limits: Max Positions={config.MAX_OPEN_POSITIONS}, Shorting={config.ALLOW_SHORT}")

    while True:
        try:
            # Run scheduled tasks (Prep and Entry)
            schedule.run_pending()

            # Continuous Exit Monitoring during market hours
            run_exit_logic()

            # Sleep 60 seconds to manage CPU and API rate limits
            time.sleep(60)

        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            # Pause briefly before attempting to resume
            time.sleep(30)


if __name__ == "__main__":
    main()