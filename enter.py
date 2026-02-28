import os
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.data.requests import StockLatestTradeRequest
import pandas as pd
from ta.momentum import RSIIndicator
from db_utils import get_clients
from risk import calculate_sid_stop_loss, calculate_position_size, calculate_atr_stop
import config
from unified_logger import get_logger

logger = get_logger(__name__)


def execute_sid_entries():
    """
    Final step in the entry pipeline.
    Checks portfolio limits, account permissions, prioritizes signals, and executes orders.
    """
    clients = get_clients()
    supabase = clients['supabase_client']

    trading_client = TradingClient(
        config.APCA_API_KEY_ID,
        config.APCA_API_SECRET_KEY,
        paper=config.PAPER_TRADING
    )

    # 1. Check current portfolio status and account capabilities
    try:
        account = trading_client.get_account()
        equity = float(account.equity)

        # Shorting Gate: Alpaca requires $2,000 for margin/shorting
        can_short_account = account.shorting_enabled and equity >= 2000
        is_shorting_allowed = config.ALLOW_SHORT and can_short_account

        open_positions = trading_client.get_all_positions()
        current_pos_count = len(open_positions)
        # Fix #4: use a set for O(1) lookups; update it locally after each entry
        # so subsequent iterations in the same run see the new position
        open_position_symbols = {p.symbol for p in open_positions}
    except Exception as e:
        logger.error(f"Could not fetch account/position data: {e}")
        return

    if current_pos_count >= config.MAX_OPEN_POSITIONS:
        logger.info(f"🛑 Max positions reached ({current_pos_count}/{config.MAX_OPEN_POSITIONS}).")
        return

    # 2. Get ready signals, ordered by score (Highest first)
    signals_resp = supabase.table("sid_method_signal_watchlist") \
        .select("*") \
        .eq("is_ready", True) \
        .order("market_score", desc=True) \
        .execute()

    if not signals_resp.data:
        logger.info("No ready signals found.")
        return

    for signal in signals_resp.data:
        if current_pos_count >= config.MAX_OPEN_POSITIONS:
            break

        symbol = signal['symbol']
        direction = signal['direction']

        # 3. Shorting Permission Check
        if direction == 'SHORT' and not is_shorting_allowed:
            logger.warning(f"🚫 Skipping SHORT for {symbol}: Shorting disabled or equity below $2,000.")
            continue

        # 4. Check if already in a position for this symbol
        # Fix #4: check against the locally-maintained set (updated after each entry)
        # instead of the stale list fetched once at the start of the run
        if symbol in open_position_symbols:
            continue

        # 5. Risk Calculation
        sl_strat = config.STOP_LOSS_STRATEGY
        exit_strat = config.EXIT_STRATEGY

        data_resp = supabase.table("market_data") \
            .select("*") \
            .eq("symbol", symbol) \
            .eq("timeframe", "1d") \
            .order("timestamp", desc=True) \
            .limit(30) \
            .execute()

        # Convert to DataFrame and reverse to chronological order
        df = pd.DataFrame(data_resp.data).iloc[::-1]

        # RSI room check — re-evaluated at entry time, not just at validation.
        # A signal marked is_ready=True days ago may have RSI drift too close to 50.
        curr_rsi = RSIIndicator(close=df['close'], window=config.RSI_PERIOD).rsi().iloc[-1]
        if direction == 'LONG' and curr_rsi > config.RSI_MOMENTUM_ROOM_LONG:
            logger.info(f"🚫 Skipping {symbol}: RSI {curr_rsi:.1f} too close to exit target for LONG entry.")
            continue
        if direction == 'SHORT' and curr_rsi < config.RSI_MOMENTUM_ROOM_SHORT:
            logger.info(f"🚫 Skipping {symbol}: RSI {curr_rsi:.1f} too close to exit target for SHORT entry.")
            continue

        # 2. Risk Calculation based on Strategy
        if sl_strat == "ATR_TRAIL":
            # Now 'df' is defined and can be passed to the function
            stop_loss = calculate_atr_stop(df, direction)
        else:
            stop_loss = calculate_sid_stop_loss(signal['extreme_price'], direction)

        # 3. Update DB with chosen strategies before entry
        supabase.table("sid_method_signal_watchlist").update({
            "stop_loss_strategy": sl_strat,
            "exit_strategy": exit_strat,
            "stop_loss": stop_loss
        }).eq("symbol", symbol).execute()

        # Fetch live price from Alpaca at entry time — one API call per trade entered,
        # not per symbol scanned, so rate limits are not a concern.
        latest = clients['alpaca_client'].get_stock_latest_trade(
            StockLatestTradeRequest(symbol_or_symbols=symbol)
        )
        entry_price = float(latest[symbol].price)
        qty = calculate_position_size(equity, config.RISK_PER_TRADE, entry_price, stop_loss)

        if qty <= 0:
            continue

        # 6. Submit Order with Bracket Stop Loss
        try:
            side = OrderSide.BUY if direction == 'LONG' else OrderSide.SELL
            order_data = MarketOrderRequest(
                symbol=symbol,
                qty=qty,
                side=side,
                time_in_force=TimeInForce.GTC,
                # This ensures the stop is placed immediately with the entry
                stop_loss=StopLossRequest(stop_price=stop_loss)
            )

            trading_client.submit_order(order_data=order_data)

            # Update local count and symbol set, then clean up DB
            current_pos_count += 1
            open_position_symbols.add(symbol)  # Fix #4: keep set in sync for this run
            supabase.table("sid_method_signal_watchlist").update({
                "is_active": True,
                "fill_price": entry_price  # Stored for MOMENTUM break-even stop calculation
            }).eq("symbol", symbol).execute()

            logger.info(f"🚀 {side} {qty} {symbol} (Score: {signal['market_score']}). Stop: {stop_loss}")

        except Exception as e:
            logger.error(f"❌ Failed entry for {symbol}: {e}")


if __name__ == "__main__":
    execute_sid_entries()