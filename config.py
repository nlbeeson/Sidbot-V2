import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# --- DATABASE & API CONFIG ---
# These pull from your .env for security, with local defaults for development
APCA_API_KEY_ID = os.getenv("APCA_API_KEY_ID")
APCA_API_SECRET_KEY = os.getenv("APCA_API_SECRET_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
RESEND_API_KEY = os.getenv("RESEND_API_KEY")
EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_RECEIVER = os.getenv("EMAIL_RECEIVER")

# --- PORTFOLIO & RISK MANAGEMENT ---
MAX_OPEN_POSITIONS = 3       # Default limit for total open trades
RISK_PER_TRADE = 0.01        # Risking 1% of total equity per position
PAPER_TRADING = True         # Toggle for Alpaca Paper vs Live environment
ALLOW_SHORT = True           # Toggle to enable/disable short selling in the strategy

# --- SID METHOD PARAMETERS ---
# --- STRATEGY SELECTION ---
STOP_LOSS_STRATEGY = "ATR_TRAIL"  # Options: "FIXED_WHOLE" or "ATR_TRAIL"
EXIT_STRATEGY = "FIXED" # Options: "FIXED" (RSI 50 crossover) or "MOMENTUM" (Remains in trade past RSI  until RSI momentum reverses)

# --- ATR SPECIFIC ---
ATR_PERIOD = 14
ATR_MULTIPLIER = 3.0

# Discovery Levels (get_signals.py)
RSI_EXTREME_OVERSOLD = 30
RSI_EXTREME_OVERBOUGHT = 70
RSI_SIGNAL_PERIOD = 28  # Lookback period to confirm RSI extreme is still valid (e.g. 28 days)

# Validation Levels (scanner.py)
RSI_MOMENTUM_ROOM_LONG = 45  # Must be <= this to stay in Long setup
RSI_MOMENTUM_ROOM_SHORT = 55 # Must be >= this to stay in Short setup
EARNINGS_RESTRICTION_DAYS = 14 # Do not enter if earnings within this window

# --- INDICATOR SETTINGS ---
RSI_PERIOD = 14
MACD_FAST = 12
MACD_SLOW = 26
MACD_SIGNAL = 9

# --- CONVICTION SCORING WEIGHTS ---
WEIGHT_PREFERRED_LIST = 2
WEIGHT_MACD_CROSSOVER = 1
WEIGHT_REVERSAL_PATTERN = 1
WEIGHT_SPY_ALIGNMENT = 1
WEIGHT_SECTOR_ALIGNMENT = 1

# --- EXIT STRATEGY ---
RSI_EXIT_TARGET = 50         # Close position when Daily RSI crosses this