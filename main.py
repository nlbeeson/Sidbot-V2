import time
from datetime import datetime, time as dt_time
from zoneinfo import ZoneInfo  # Fix #9: explicit timezone for EST market hours check
import schedule

# Import modular strategy files
import db_utils
import get_signals
import scanner
import enter
import exit
import config
import reporter
from unified_logger import get_logger

logger = get_logger(__name__)

# Eastern timezone for all market hours calculations
EST = ZoneInfo("America/New_York")

# Day names used to create fresh Job objects per time slot.
# IMPORTANT: Do NOT store schedule.every().monday etc. in a shared list and
# reuse it across multiple .at().do() loops. The schedule library's .at() and
# .do() methods modify the Job object IN PLACE, so reusing the same Job objects
# in a second loop overwrites the time and function set by the first loop.
# Each call to schedule.every() must create a brand-new Job object.
_weekday_names = ['monday', 'tuesday', 'wednesday', 'thursday', 'friday']

def is_market_open():
    """
    Checks if current time is within regular NYSE hours (9:30 AM - 4:00 PM EST).
    Fix #9: uses explicit EST/EDT timezone via ZoneInfo so this works correctly
    on UTC servers (e.g. DigitalOcean droplets) without relying on system locale.
    """
    now_est = datetime.now(EST)
    market_start = dt_time(9, 30)
    market_end = dt_time(16, 0)
    # weekday() < 5 ensures Monday (0) through Friday (4)
    return market_start <= now_est.time() <= market_end and now_est.weekday() < 5


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

    # Fetch clients once and reuse for all scanner calls below
    clients = db_utils.get_clients()
    supabase = clients['supabase_client']

    # 2b. Update extreme prices on staged signals — tracks the absolute lowest low
    # (LONG) or highest high (SHORT) since the RSI touch, which sets the FIXED_WHOLE stop.
    scanner.update_staged_extreme_prices(supabase)

    # 3. Validation: Check momentum gates (Daily/Weekly RSI & MACD Slopes)
    # Fix #1: validate_staged_signals requires supabase as its first argument
    scanner.validate_staged_signals(supabase)

    # 4. Scoring: Apply weights (Preferred list, SPY alignment, etc.)
    scanner.score_and_validate_staged(supabase)

    # 5. Trigger Daily Intelligence Report
    # Running this immediately after scoring ensures the email contains the freshest data.
    logger.info("Generating and sending daily intelligence report...")
    reporter.send_report()

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
for day in _weekday_names:
    getattr(schedule.every(), day).at("15:30").do(run_prep_sequence)

# 15:45: Final Entry Execution
for day in _weekday_names:
    getattr(schedule.every(), day).at("15:45").do(run_execution_sequence)

# 16:30: End-of-day database cleanup
for day in _weekday_names:
    getattr(schedule.every(), day).at("16:30").do(run_daily_maintenance)


def main():
    logger.info("SidBot Orchestrator active on Ubuntu Droplet (EST).")

    last_exit_check = 0  # Track the last time we ran the exit scan

    while True:
        try:
            # 1. High-frequency heartbeat for the scheduler (every 1 second)
            schedule.run_pending()

            # 2. Lower-frequency check for exits
            current_time = time.time()
            if current_time - last_exit_check >= 300:  # 300 seconds = 5 minutes
                run_exit_logic()
                last_exit_check = current_time

            # 3. Short sleep to keep CPU usage low and scheduler accurate
            time.sleep(1)

        except Exception as e:
            logger.error(f"Critical error in main loop: {e}")
            time.sleep(30)


if __name__ == "__main__":
    main()