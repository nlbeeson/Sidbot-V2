import os
import logging
import pandas as pd
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, ReplaceOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from ta.momentum import RSIIndicator
from db_utils import get_clients
import risk  # Math engine for ATR Trailing logic
import config

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[logging.FileHandler("sidbot.log"), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

def check_momentum_exit(side, curr_rsi, prev_rsi):
    """
    Returns True if momentum has reversed against the trade at or past the RSI 50 target.
    """
    if side == 'LONG':
        # Exit if RSI reaches 50 AND starts falling
        return curr_rsi >= config.RSI_EXIT_TARGET and curr_rsi < prev_rsi
    else:
        # Exit if RSI reaches 50 AND starts rising
        return curr_rsi <= config.RSI_EXIT_TARGET and curr_rsi > prev_rsi


def monitor_and_execute_exits():
    """
    Monitors open Alpaca positions. Handles RSI-based profit taking and
    updates ATR-based trailing stops.
    """
    clients = get_clients()
    supabase = clients['supabase_client']
    trading_client = TradingClient(
        config.APCA_API_KEY_ID,
        config.APCA_API_SECRET_KEY,
        paper=config.PAPER_TRADING
    )

    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")
        return

    if not positions:
        logger.debug("No open positions to monitor.")
        return

    for pos in positions:
        symbol = pos.symbol
        side = pos.side.upper()  # 'LONG' or 'SHORT'
        qty = abs(float(pos.qty))

        try:
            # 1. Fetch assigned strategies and current stop from DB
            signal_data = supabase.table("sid_method_signal_watchlist") \
                .select("exit_strategy, stop_loss_strategy, stop_loss") \
                .eq("symbol", symbol).maybe_single().execute()

            # Default to FIXED if no record is found in the watchlist
            exit_strat = signal_data.data['exit_strategy'] if signal_data.data else "FIXED"
            sl_strat = signal_data.data['stop_loss_strategy'] if signal_data.data else "FIXED_WHOLE"
            stored_stop = float(signal_data.data['stop_loss']) if signal_data.data and signal_data.data['stop_loss'] else None

            # 2. Fetch Market Data (Need enough for RSI and ATR)
            data_resp = supabase.table("market_data") \
                .select("*") \
                .eq("symbol", symbol) \
                .eq("timeframe", "1d") \
                .order("timestamp", desc=True) \
                .limit(30) \
                .execute()

            if len(data_resp.data) < 20:
                continue

            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Calculate technicals
            rsi_series = RSIIndicator(close=df['close'], window=14).rsi()
            curr_rsi = rsi_series.iloc[-1]
            prev_rsi = rsi_series.iloc[-2]

            # --- RSI PROFIT EXIT LOGIC ---
            should_exit = False
            if exit_strat == "MOMENTUM":
                # Only exit if the target is reached AND momentum reverses
                should_exit = check_momentum_exit(side, curr_rsi, prev_rsi)
            else:
                # FIXED Strategy: Hard exit the moment the target is reached
                if side == 'LONG' and curr_rsi >= config.RSI_EXIT_TARGET:
                    should_exit = True
                elif side == 'SHORT' and curr_rsi <= config.RSI_EXIT_TARGET:
                    should_exit = True

            if should_exit:
                exit_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                trading_client.submit_order(
                    MarketOrderRequest(
                        symbol=symbol,
                        qty=qty,
                        side=exit_side,
                        time_in_force=TimeInForce.GTC
                    )
                )
                logger.info(f"🛑 EXIT ({exit_strat}): Closed {symbol} at RSI {curr_rsi:.2f}")
                continue  # Move to the next position

            # --- ATR RATCHET LOGIC (Trailing Stop) ---
            if sl_strat == "ATR_TRAIL" and stored_stop:
                # Use the math engine in risk.py to see if the stop should move
                new_stop = risk.calculate_ratchet_stop(stored_stop, df, side)

                if new_stop != stored_stop:
                    # Update DB with the new tightened stop
                    supabase.table("sid_method_signal_watchlist").update({
                        "stop_loss": new_stop,
                        "last_updated": datetime.now().isoformat()
                    }).eq("symbol", symbol).execute()

                    # Find and update the open Alpaca stop-loss order
                    orders = trading_client.get_orders()
                    for order in orders:
                        if order.symbol == symbol and order.type.value == 'stop':
                            trading_client.replace_order_by_id(
                                order.id,
                                ReplaceOrderRequest(stop_price=new_stop)
                            )
                            logger.info(f"🔄 RATCHET: Trailed {symbol} stop to {new_stop}")
                            break

        except Exception as e:
            logger.error(f"❌ Error monitoring {symbol}: {e}")

if __name__ == "__main__":
    monitor_and_execute_exits()