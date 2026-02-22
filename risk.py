import math
import logging
from ta.volatility import AverageTrueRange
import config

logger = logging.getLogger(__name__)


def calculate_atr_stop(df, direction):
    """Initial ATR stop calculation used at entry."""
    atr_indicator = AverageTrueRange(
        high=df['high'], low=df['low'], close=df['close'], window=config.ATR_PERIOD
    )
    atr = atr_indicator.average_true_range().iloc[-1]
    curr_price = df['close'].iloc[-1]

    if direction == 'LONG':
        return curr_price - (atr * config.ATR_MULTIPLIER)
    else:
        return curr_price + (atr * config.ATR_MULTIPLIER)


def calculate_ratchet_stop(current_stop, df, direction):
    """
    Determines if the stop loss should be moved closer to price.
    Returns the NEW stop price if it moved, otherwise returns the OLD stop price.
    """
    atr_indicator = AverageTrueRange(
        high=df['high'], low=df['low'], close=df['close'], window=config.ATR_PERIOD
    )
    atr = atr_indicator.average_true_range().iloc[-1]
    curr_price = df['close'].iloc[-1]

    if direction == 'LONG':
        new_calculated_stop = curr_price - (atr * config.ATR_MULTIPLIER)
        # Only move stop UP (Ratchet)
        return max(current_stop, new_calculated_stop)
    else:
        new_calculated_stop = curr_price + (atr * config.ATR_MULTIPLIER)
        # Only move stop DOWN (Ratchet)
        return min(current_stop, new_calculated_stop)

def calculate_sid_stop_loss(extreme_price, direction):
    """
    Calculates the stop loss based on the absolute extreme price recorded.

    Rules:
    - SHORT: Round 'extreme_price' (highest high) UP to the next whole number.
      If already a whole number, move to the next whole number up (e.g., 65.00 -> 66.00).
    - LONG: Round 'extreme_price' (lowest low) DOWN to the next whole number.
      If already a whole number, move to the next whole number down (e.g., 35.00 -> 34.00).
    """
    if extreme_price is None:
        return None

    extreme_price = float(extreme_price)

    if direction == 'SHORT':
        # If it's exactly a whole number, math.ceil stays the same, so we add 1.
        # Otherwise, math.ceil moves it to the next whole number.
        if extreme_price.is_integer():
            stop_loss = extreme_price + 1.0
        else:
            stop_loss = math.ceil(extreme_price)

    elif direction == 'LONG':
        # If it's exactly a whole number, math.floor stays the same, so we subtract 1.
        # Otherwise, math.floor moves it down to the next whole number.
        if extreme_price.is_integer():
            stop_loss = extreme_price - 1.0
        else:
            stop_loss = math.floor(extreme_price)
    else:
        logger.error(f"Invalid direction '{direction}' provided for stop loss calculation.")
        return None

    return float(stop_loss)


def calculate_position_size(equity, risk_percent, entry_price, stop_loss):
    """
    Calculates the number of shares to buy/short based on a risk-per-trade percentage.
    """
    risk_amount = equity * risk_percent
    risk_per_share = abs(entry_price - stop_loss)

    if risk_per_share == 0:
        return 0

    qty = math.floor(risk_amount / risk_per_share)
    return qty