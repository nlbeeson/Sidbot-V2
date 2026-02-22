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


def monitor_and_execute_exits():
    """
    Monitors open Alpaca positions and closes them when Daily RSI crosses 50.
    """
    clients = get_clients()
    supabase = clients['supabase_client']

    # Initialize Alpaca Trading Client
    trading_client = TradingClient(
        config.APCA_API_KEY_ID,
        config.APCA_API_SECRET_KEY,
        paper=config.PAPER_TRADING
    )

    # 1. Fetch all open positions
    try:
        positions = trading_client.get_all_positions()
    except Exception as e:
        logger.error(f"Failed to fetch positions: {e}")
        return

    if not positions:
        logger.info("No open positions found.")
        return

    for pos in positions:
        symbol = pos.symbol
        side = pos.side  # 'long' or 'short'
        qty = abs(float(pos.qty))

        try:
            # 2. Fetch the latest daily data from market_data
            # We need ~30 bars to calculate a reliable RSI-14
            data_resp = supabase.table("market_data") \
                .select("close") \
                .eq("symbol", symbol) \
                .eq("timeframe", "1d") \
                .order("timestamp", desc=True) \
                .limit(30) \
                .execute()

            if len(data_resp.data) < 15:
                continue

            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Calculate current RSI
            rsi_series = RSIIndicator(close=df['close'], window=14).rsi()
            curr_rsi = rsi_series.iloc[-1]

            # 4. Check RSI 50 Crossover Logic
            # Exit LONG if RSI >= 50 | Exit SHORT if RSI <= 50
            should_exit = False
            if side == 'long' and curr_rsi >= config.RSI_EXIT_TARGET:
                should_exit = True
            elif side == 'short' and curr_rsi <= config.RSI_EXIT_TARGET:
                should_exit = True

            # 5. Execute Exit Order
            if should_exit:
                exit_side = OrderSide.SELL if side == 'long' else OrderSide.BUY

                exit_request = MarketOrderRequest(
                    symbol=symbol,
                    qty=qty,
                    side=exit_side,
                    time_in_force=TimeInForce.GTC
                )

                trading_client.submit_order(order_data=exit_request)
                logger.info(f"🛑 EXIT: Closed {side} {symbol} at RSI {curr_rsi:.2f}")
            else:
                logger.info(f"⏳ Holding {symbol} ({side}): Current RSI is {curr_rsi:.2f}")

        except Exception as e:
            logger.error(f"❌ Error monitoring exit for {symbol}: {e}")


if __name__ == "__main__":
    monitor_and_execute_exits()