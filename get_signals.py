import logging
import pandas as pd
from datetime import datetime
from ta.momentum import RSIIndicator
from db_utils import get_clients
import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


def populate_sid_extremes():
    """
    Step 1: Scans market_data for symbols CURRENTLY hitting RSI extremes (>=70 or <=30).
    Populates the sid_method_signal_watchlist table for further monitoring.
    """
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. Fetch all symbols from ticker_reference
    tickers_resp = supabase.table("ticker_reference").select("symbol").execute()
    symbols = [item['symbol'] for item in tickers_resp.data]

    logger.info(f"Scanning {len(symbols)} symbols for current RSI extremes...")

    for symbol in symbols:
        try:
            # 2. Fetch recent daily data (30 bars is sufficient for RSI-14 calculation)
            data_resp = supabase.table("market_data") \
                .select("timestamp, close, high, low") \
                .eq("symbol", symbol) \
                .eq("timeframe", "1d") \
                .order("timestamp", desc=True) \
                .limit(30) \
                .execute()

            if len(data_resp.data) < 15:
                continue

            # Convert to DataFrame and ensure chronological order
            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Calculate Current RSI
            rsi_series = RSIIndicator(close=df['close'], window=14).rsi()
            curr_rsi = rsi_series.iloc[-1]
            curr_price = df['close'].iloc[-1]

            # 4. Filter for CURRENT extremes only
            direction = None
            if curr_rsi <= config.RSI_EXTREME_OVERSOLD:
                direction = 'LONG'
            elif curr_rsi >= config.RSI_EXTREME_OVERBOUGHT:
                direction = 'SHORT'

            # 5. Upsert to sid_method_signal_watchlist
            if direction:
                # extreme_price is the high/low at the time of the RSI touch
                extreme_val = df['low'].iloc[-1] if direction == 'LONG' else df['high'].iloc[-1]

                payload = {
                    "symbol": symbol,
                    "direction": direction,
                    "rsi_touch_value": round(float(curr_rsi), 4),
                    "rsi_touch_date": datetime.now().isoformat(),
                    "last_updated": datetime.now().isoformat(),
                    "extreme_price": float(extreme_val),
                    "entry_price": float(curr_price),
                    "is_ready": False,  # Gates/Turn checks are Step 2
                    "stop_loss_strategy": config.STOP_LOSS_STRATEGY,
                    "exit_strategy": config.EXIT_STRATEGY,
                    "logic_trail": {
                        "event": "Initial RSI Extreme Hit",
                        "rsi_at_touch": round(float(curr_rsi), 2),
                        "price_at_touch": float(curr_price)
                    }
                }

                supabase.table("sid_method_signal_watchlist").upsert(payload, on_conflict="symbol").execute()
                logger.info(f"🚩 Found {direction} setup for {symbol} (RSI: {curr_rsi:.2f})")

        except Exception as e:
            logger.error(f"❌ Error processing {symbol}: {e}")


if __name__ == "__main__":
    populate_sid_extremes()