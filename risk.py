import math
import logging
from ta.volatility import AverageTrueRange
import config

logger = logging.getLogger(__name__)


def calculate_atr_stop(df, direction):
    """Calculates initial ATR-based stop loss."""
    atr_indicator = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=config.ATR_PERIOD)
    atr = atr_indicator.average_true_range().iloc[-1]
    curr_price = df['close'].iloc[-1]

    return curr_price - (atr * config.ATR_MULTIPLIER) if direction == 'LONG' else curr_price + (
                atr * config.ATR_MULTIPLIER)


def calculate_ratchet_stop(current_stop, df, direction):
    """
    Moves the stop only in the direction of the trade (Ratchet).
    Returns the new stop if it moved, otherwise returns the old stop.
    """
    atr_indicator = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=config.ATR_PERIOD)
    atr = atr_indicator.average_true_range().iloc[-1]
    curr_price = df['close'].iloc[-1]

    if direction == 'LONG':
        new_stop = curr_price - (atr * config.ATR_MULTIPLIER)
        return max(current_stop, new_stop)  # Only move UP
    else:
        new_stop = curr_price + (atr * config.ATR_MULTIPLIER)
        return min(current_stop, new_stop)  # Only move DOWN

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