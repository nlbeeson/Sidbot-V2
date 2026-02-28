import os
import math
import pandas as pd
from datetime import datetime
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, ReplaceOrderRequest, GetOrdersRequest
from alpaca.trading.enums import OrderSide, TimeInForce, QueryOrderStatus
from ta.momentum import RSIIndicator
from db_utils import get_clients
import risk
import config
from unified_logger import get_logger

logger = get_logger(__name__)


def check_momentum_exit(side, curr_rsi, prev_rsi):
    """
    Returns True if momentum has reversed against the trade.
    LONG:  hold while RSI rises above 50; exit when it starts to fall (peak confirmed).
    SHORT: hold while RSI falls below 50; exit when it starts to rise (bottom confirmed).
    Both directions mirror each other — wait past RSI_EXIT_TARGET, exit on reversal.
    """
    if side == 'LONG':
        return curr_rsi >= config.RSI_EXIT_TARGET and curr_rsi < prev_rsi
    else:  # SHORT
        return curr_rsi <= config.RSI_EXIT_TARGET and curr_rsi > prev_rsi


def monitor_and_execute_exits():
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

    for pos in positions:
        symbol = pos.symbol
        # Fix #2: pos.side is a PositionSide enum; .value extracts the string ("long"/"short")
        # before calling .upper() — calling .upper() directly on the enum raises AttributeError
        side = pos.side.value.upper()
        qty = abs(float(pos.qty))

        try:
            # 1. Fetch signal data safely
            response = supabase.table("sid_method_signal_watchlist") \
                .select("exit_strategy, stop_loss_strategy, stop_loss, fill_price, partial_exit_done") \
                .eq("symbol", symbol).maybe_single().execute()

            # Fix: Ensure the response object exists before checking .data
            if not response or not hasattr(response, 'data') or response.data is None:
                logger.warning(f"⚠️ No watchlist record for {symbol}. Skipping automated exit logic.")
                continue

            signal_data = response.data
            exit_strat = signal_data.get('exit_strategy', 'FIXED')
            sl_strat = signal_data.get('stop_loss_strategy', 'FIXED_WHOLE')
            stored_stop = float(signal_data['stop_loss']) if signal_data.get('stop_loss') else None

            # 2. Fetch Market Data
            data_resp = supabase.table("market_data").select("*").eq("symbol", symbol).eq("timeframe", "1d").order(
                "timestamp", desc=True).limit(30).execute()

            if not data_resp.data or len(data_resp.data) < 20:
                continue

            df = pd.DataFrame(data_resp.data).iloc[::-1]

            # 3. Calculate Indicators
            rsi_series = RSIIndicator(close=df['close'], window=14).rsi()
            curr_rsi, prev_rsi, prev_prev_rsi = rsi_series.iloc[-1], rsi_series.iloc[-2], rsi_series.iloc[-3]

            # 4. Exit Logic
            exit_side = OrderSide.SELL if side == 'LONG' else OrderSide.BUY
            at_rsi_target = (side == 'LONG' and curr_rsi >= config.RSI_EXIT_TARGET) or \
                            (side == 'SHORT' and curr_rsi <= config.RSI_EXIT_TARGET)

            # Hoisted here so the early exit gate can reference it without entering the MOMENTUM block
            partial_done = signal_data.get('partial_exit_done', False)

            # --- EARLY EXIT: 2-day RSI reversal (configurable) ---
            # Mirrors the mentor's manual "exit if RSI moves against trade 2 days in a row".
            # Only fires BEFORE RSI 50 is reached, and NOT during MOMENTUM Phase 2
            # (Phase 2 manages its own momentum-reversal exit after the partial close).
            if config.EARLY_EXIT_ON_RSI_REVERSAL and not at_rsi_target and not partial_done:
                two_day_reversal = (
                    (side == 'LONG' and curr_rsi < prev_rsi and prev_rsi < prev_prev_rsi) or
                    (side == 'SHORT' and curr_rsi > prev_rsi and prev_rsi > prev_prev_rsi)
                )
                if two_day_reversal:
                    trading_client.submit_order(
                        MarketOrderRequest(symbol=symbol, qty=qty, side=exit_side,
                                           time_in_force=TimeInForce.GTC))
                    supabase.table("sid_method_signal_watchlist").update({"is_active": False}).eq("symbol", symbol).execute()
                    logger.info(f"🚪 EARLY EXIT (2-day RSI reversal): Closed {symbol} at RSI {curr_rsi:.2f}")
                    continue

            if exit_strat == "MOMENTUM":
                fill_price = float(signal_data['fill_price']) if signal_data.get('fill_price') else None

                if not partial_done and at_rsi_target:
                    # Phase 1: RSI crossed 50 — close 50%, move stop to break even.
                    partial_qty = math.floor(qty / 2)
                    if partial_qty > 0:
                        trading_client.submit_order(
                            MarketOrderRequest(symbol=symbol, qty=partial_qty, side=exit_side,
                                               time_in_force=TimeInForce.GTC))
                        logger.info(f"📊 PARTIAL EXIT: Closed {partial_qty} {symbol} at RSI {curr_rsi:.2f}")

                    # Move stop to break even whether or not a partial order was submitted
                    # (position may be 1 share — still want the stop moved).
                    if fill_price:
                        supabase.table("sid_method_signal_watchlist").update({
                            "partial_exit_done": True,
                            "stop_loss": fill_price
                        }).eq("symbol", symbol).execute()
                        orders = trading_client.get_orders(GetOrdersRequest(status=QueryOrderStatus.OPEN))
                        for order in orders:
                            if order.symbol == symbol and order.type.value == 'stop':
                                trading_client.replace_order_by_id(
                                    order.id, ReplaceOrderRequest(stop_price=fill_price))
                                logger.info(f"🔄 BREAK EVEN: {symbol} stop moved to {fill_price}")
                                break
                    else:
                        # fill_price missing — mark done so we don't loop on this forever
                        supabase.table("sid_method_signal_watchlist").update({
                            "partial_exit_done": True
                        }).eq("symbol", symbol).execute()
                        logger.warning(f"⚠️ {symbol}: fill_price missing, cannot set break-even stop.")

                elif partial_done and check_momentum_exit(side, curr_rsi, prev_rsi):
                    # Phase 2: Momentum reversed — close remaining position.
                    trading_client.submit_order(
                        MarketOrderRequest(symbol=symbol, qty=qty, side=exit_side,
                                           time_in_force=TimeInForce.GTC))
                    supabase.table("sid_method_signal_watchlist").update({"is_active": False}).eq("symbol", symbol).execute()
                    logger.info(f"🛑 MOMENTUM EXIT (remaining 50%): Closed {symbol} at RSI {curr_rsi:.2f}")
                    continue

            else:  # FIXED
                if at_rsi_target:
                    trading_client.submit_order(
                        MarketOrderRequest(symbol=symbol, qty=qty, side=exit_side,
                                           time_in_force=TimeInForce.GTC))
                    supabase.table("sid_method_signal_watchlist").update({"is_active": False}).eq("symbol", symbol).execute()
                    logger.info(f"🛑 FIXED EXIT: Closed {symbol} at RSI {curr_rsi:.2f}")
                    continue

            # 5. Ratchet Logic
            if sl_strat == "ATR_TRAIL" and stored_stop:
                new_stop = risk.calculate_ratchet_stop(stored_stop, df, side)
                if new_stop != stored_stop:
                    supabase.table("sid_method_signal_watchlist").update({"stop_loss": new_stop}).eq("symbol",
                                                                                                     symbol).execute()
                    # Fix #5: filter to OPEN orders only so we don't try to replace
                    # a stop that has already been filled or cancelled
                    orders = trading_client.get_orders(
                        GetOrdersRequest(status=QueryOrderStatus.OPEN)
                    )
                    for order in orders:
                        if order.symbol == symbol and order.type.value == 'stop':
                            trading_client.replace_order_by_id(order.id, ReplaceOrderRequest(stop_price=new_stop))
                            logger.info(f"🔄 RATCHET: Updated {symbol} stop to {new_stop}")
                            break

        except Exception as e:
            logger.error(f"❌ Error monitoring {symbol}: {e}")


if __name__ == "__main__":
    monitor_and_execute_exits()