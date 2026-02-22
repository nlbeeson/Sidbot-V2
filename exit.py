import os
import logging
import pandas as pd
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from ta.momentum import RSIIndicator
from db_utils import get_clients

import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

def check_momentum_exit(side, curr_rsi, prev_rsi):
    """
    Returns True if momentum has reversed against the trade.
    """
    if side == 'long':
        # Exit if RSI reaches 50 AND starts falling
        return curr_rsi >= config.RSI_EXIT_TARGET and curr_rsi < prev_rsi
    else:
        # Exit if RSI reaches 50 AND starts rising
        return curr_rsi <= config.RSI_EXIT_TARGET and curr_rsi > prev_rsi


def monitor_and_execute_exits():
    clients = get_clients()
    supabase = clients['supabase_client']
    trading_client = TradingClient(config.APCA_API_KEY_ID, config.APCA_API_SECRET_KEY, paper=config.PAPER_TRADING)

    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")
        return

    for pos in positions:
        symbol = pos.symbol
        side = pos.side.upper()  # 'LONG' or 'SHORT'
        qty = abs(float(pos.qty))

        try:
            # 1. Fetch current strategy and stored stop from DB
            signal_data = supabase.table("sid_method_signal_watchlist") \
                .select("exit_strategy, stop_loss_strategy, stop_loss") \
                .eq("symbol", symbol).maybe_single().execute()

            # Default to FIXED if not found (fallback)
            exit_strat = signal_data.data['exit_strategy'] if signal_data.data else "FIXED"
            sl_strat = signal_data.data['stop_loss_strategy'] if signal_data.data else "FIXED_WHOLE"
            stored_stop = float(signal_data.data['stop_loss']) if signal_data.data and signal_data.data[
                'stop_loss'] else None

            # 2. Fetch Market Data
            data_resp = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1d").order(
                "timestamp", desc=True).limit(30).execute()
            if len(data_resp.data) < 20: continue
            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Calculate RSI
            rsi_series = RSIIndicator(close=df['close'], window=14).rsi()
            curr_rsi = rsi_series.iloc[-1]
            prev_rsi = rsi_series.iloc[-2]

            # --- EXIT LOGIC (RSI) ---
            should_exit = False
            if exit_strat == "MOMENTUM":
                if side == 'LONG' and curr_rsi >= config.RSI_EXIT_TARGET and curr_rsi < prev_rsi:
                    should_exit = True
                elif side == 'SHORT' and curr_rsi <= config.RSI_EXIT_TARGET and curr_rsi > prev_rsi:
                    should_exit = True
            else:  # FIXED Strategy
                if (side == 'LONG' and curr_rsi >= config.RSI_EXIT_TARGET) or (
                        side == 'SHORT' and curr_rsi <= config.RSI_EXIT_TARGET):
                    should_exit = True

            if should_exit:
                exit_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
                trading_client.submit_order(
                    MarketOrderRequest(symbol=symbol, qty=qty, side=exit_side, time_in_force=TimeInForce.GTC))
                logger.info(f"🛑 EXIT ({exit_strat}): {symbol} at RSI {curr_rsi:.2f}")
                continue  # Move to next position

            # --- RATCHET LOGIC (ATR Trailing Stop) ---
            if sl_strat == "ATR_TRAIL" and stored_stop:
                new_stop = risk.calculate_ratchet_stop(stored_stop, df, side)

                if new_stop != stored_stop:
                    # Update DB
                    supabase.table("sid_method_signal_watchlist").update({"stop_loss": new_stop}).eq("symbol",
                                                                                                     symbol).execute()

                    # Update Alpaca Order (Find the open stop-loss order for this position)
                    orders = trading_client.get_orders()
                    for order in orders:
                        if order.symbol == symbol and order.type.value == 'stop':
                            trading_client.replace_order_by_id(order.id, ReplaceOrderRequest(stop_price=new_stop))
                            logger.info(f"🔄 RATCHET: Updated {symbol} stop to {new_stop}")
                            break

        except Exception as e:
            logger.error(f"❌ Error monitoring {symbol}: {e}")