import os
import logging
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest, StopLossRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from db_utils import get_clients
from risk import calculate_sid_stop_loss, calculate_position_size, calculate_atr_stop
import config

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)


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
        if any(p.symbol == symbol for p in open_positions):
            continue

        # 5. Risk Calculation
        # 1. Determine Strategy (Defaults to config, but could be dynamic)
        sl_strat = config.STOP_LOSS_STRATEGY
        exit_strat = config.EXIT_STRATEGY

        # 2. Risk Calculation based on Strategy
        if sl_strat == "ATR_TRAIL":
            # Ensure you have enough data for ATR (usually 14+ periods)
            stop_loss = calculate_atr_stop(df, direction)
        else:
            stop_loss = calculate_sid_stop_loss(signal['extreme_price'], direction)

        # 3. Update DB with chosen strategies before entry
        supabase.table("sid_method_signal_watchlist").update({
            "stop_loss_strategy": sl_strat,
            "exit_strategy": exit_strat,
            "stop_loss": stop_loss
        }).eq("symbol", symbol).execute()

        stop_loss = calculate_sid_stop_loss(signal['extreme_price'], direction)
        entry_price = float(signal['entry_price'])
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

            # Update local count and clean up DB
            current_pos_count += 1
            supabase.table("sid_method_signal_watchlist").delete().eq("symbol", symbol).execute()

            logger.info(f"🚀 {side} {qty} {symbol} (Score: {signal['market_score']}). Stop: {stop_loss}")

        except Exception as e:
            logger.error(f"❌ Failed entry for {symbol}: {e}")


if __name__ == "__main__":
    execute_sid_entries()