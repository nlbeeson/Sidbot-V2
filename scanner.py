import logging
from datetime import datetime

import numpy as np
import pandas as pd
import pandas as pd
from db_utils import get_clients
from pref_watchlist import PREF_WATCHLIST
from ta.momentum import RSIIndicator
from ta.trend import MACD
import config

logger = logging.getLogger(__name__)


def calculate_daily_rsi(df, window=config.RSI_PERIOD):
    """
    Calculates the 14-period Daily RSI to monitor the move out of extreme territory.
    """
    rsi_series = RSIIndicator(close=df['close'], window=window).rsi()
    return rsi_series


def calculate_weekly_rsi(df_daily, window=config.RSI_PERIOD):
    """
    Resamples daily data to Weekly to ensure higher-timeframe alignment.
    """
    temp_df = df_daily.copy()
    temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'])
    temp_df.set_index('timestamp', inplace=True)

    # Resample to weekly (W-MON) to match standard trading weeks
    df_weekly = temp_df.resample('W-MON').agg({
        'open': 'first',
        'high': 'max',
        'low': 'min',
        'close': 'last'
    }).dropna()

    if len(df_weekly) < window:
        return None

    rsi_weekly = RSIIndicator(close=df_weekly['close'], window=window).rsi()
    return rsi_weekly


def calculate_daily_macd(df, fast=config.MACD_FAST, slow=config.MACD_SLOW, signal=config.MACD_SIGNAL):
    """
    Calculates MACD components for trend confirmation and conviction scoring.
    """
    macd_obj = MACD(close=df['close'], window_fast=fast, window_slow=slow, window_sign=signal)
    return {
        "line": macd_obj.macd(),
        "signal": macd_obj.macd_signal(),
        "diff": macd_obj.macd_diff()
    }


def update_staged_extreme_prices(supabase):
    """
    Step 2 (Continued): Monitors symbols in the signal_watchlist.
    If the current high (Short) or low (Long) exceeds the recorded extreme_price,
    the table is updated with the new extreme.
    """
    # 1. Fetch all symbols currently being tracked in the watchlist
    staged_resp = supabase.table("sid_method_signal_watchlist").select("symbol, direction, extreme_price").execute()

    if not staged_resp.data:
        return

    for entry in staged_resp.data:
        symbol = entry['symbol']
        direction = entry['direction']
        stored_extreme = float(entry['extreme_price']) if entry['extreme_price'] else None

        try:
            # 2. Fetch the most recent daily bar from market_data
            market_resp = supabase.table("market_data") \
                .select("high, low") \
                .eq("symbol", symbol) \
                .eq("timeframe", "1d") \
                .order("timestamp", desc=True) \
                .limit(1) \
                .execute()

            if not market_resp.data:
                continue

            current_high = float(market_resp.data[0]['high'])
            current_low = float(market_resp.data[0]['low'])
            update_needed = False
            new_extreme = stored_extreme

            # 3. Check for new extremes based on direction
            if direction == 'LONG':
                # For Longs, we want the lowest low
                if stored_extreme is None or current_low < stored_extreme:
                    new_extreme = current_low
                    update_needed = True

            elif direction == 'SHORT':
                # For Shorts, we want the highest high
                if stored_extreme is None or current_high > stored_extreme:
                    new_extreme = current_high
                    update_needed = True

            # 4. Perform the update if a more extreme price was found
            if update_needed:
                supabase.table("sid_method_signal_watchlist") \
                    .update({
                    "extreme_price": new_extreme,
                    "last_updated": datetime.now().isoformat()
                }) \
                    .eq("symbol", symbol) \
                    .execute()

                logger.info(f"🔄 Updated extreme_price for {symbol} ({direction}) to {new_extreme}")

        except Exception as e:
            logger.error(f"❌ Error updating extreme price for {symbol}: {e}")


def check_preferred_watchlist(symbol):
    """Adds +2 points if the stock is on the manually vetted list."""
    return config.WEIGHT_PREFERRED_LIST if symbol in PREF_WATCHLIST else 0


def check_macd_crossover(df, direction):
    """
    Checks if a MACD crossover just occurred (Line vs Signal).
    Adds +1 point.
    """
    exp1 = df['close'].ewm(span=config.MACD_FAST, adjust=False).mean()
    exp2 = df['close'].ewm(span=config.MACD_SLOW, adjust=False).mean()
    macd_line = exp1 - exp2
    signal_line = macd_line.ewm(span=config.MACD_SIGNAL, adjust=False).mean()

    # Check for crossover in the last 3 bars
    if direction == 'LONG':
        return config.WEIGHT_MACD_CROSSOVER if (macd_line.iloc[-3:] > signal_line.iloc[-3:]).any() and (
                macd_line.iloc[-5] < signal_line.iloc[-5]) else 0
    else:
        return config.WEIGHT_MACD_CROSSOVER if (macd_line.iloc[-3:] < signal_line.iloc[-3:]).any() and (
                macd_line.iloc[-5] > signal_line.iloc[-5]) else 0


def detect_double_top_bottom(df, direction, threshold=0.015):
    """
    Simplified check for Double Top/Bottom reversal patterns.
    threshold: % difference allowed between the two peaks/troughs.
    """
    if direction == 'LONG':
        # Double Bottom: Look for two similar lows with a peak between them
        recent_lows = df['low'].rolling(window=5, center=True).min().dropna().unique()
        if len(recent_lows) >= 2:
            last_low = df['low'].iloc[-10:].min()
            prev_low = df['low'].iloc[-40:-10].min()
            diff = abs(last_low - prev_low) / prev_low
            if diff <= threshold: return config.WEIGHT_REVERSAL_PATTERN
    else:
        # Double Top: Look for two similar highs with a trough between them
        last_high = df['high'].iloc[-10:].max()
        prev_high = df['high'].iloc[-40:-10].max()
        diff = abs(last_high - prev_high) / prev_high
        if diff <= threshold: return config.WEIGHT_REVERSAL_PATTERN
    return 0


def check_market_alignment(supabase, direction, symbol=None):
    """
    Checks SPY alignment and Sector ETF alignment.
    Returns (spy_aligned, sector_aligned)
    """
    # 1. SPY Alignment
    spy_data = supabase.table("market_data").select("close").eq("symbol", "SPY").eq("timeframe", "1d").order(
        "timestamp", desc=True).limit(2).execute()
    spy_up = spy_data.data[0]['close'] > spy_data.data[1]['close'] if len(spy_data.data) > 1 else False
    spy_aligned = (direction == 'LONG' and spy_up) or (direction == 'SHORT' and not spy_up)

    # 2. Sector Alignment (if sector_etf is in database)
    sector_aligned = False
    if symbol:
        ref = supabase.table("ticker_reference").select("sector_etf").eq("symbol", symbol).single().execute()
        sector_etf = ref.data.get('sector_etf') if ref.data else None

        if sector_etf:
            sector_data = supabase.table("market_data").select("close").eq("symbol", sector_etf).eq("timeframe",
                                                                                                    "1d").order(
                "timestamp", desc=True).limit(2).execute()
            if len(sector_data.data) > 1:
                sec_up = sector_data.data[0]['close'] > sector_data.data[1]['close']
                sector_aligned = (direction == 'LONG' and sec_up) or (direction == 'SHORT' and not sec_up)

    return spy_aligned, sector_aligned


def score_and_validate_staged(supabase):
    staged_signals = supabase.table("sid_method_signal_watchlist").select("*").execute()

    for signal in staged_signals.data:
        symbol = signal['symbol']
        direction = signal['direction']

        # 1. Fetch data
        data = supabase.table("market_data").select("*").eq("symbol", symbol).order("timestamp", desc=True).limit(
            60).execute()
        df = pd.DataFrame(data.data).iloc[::-1]

        # 2. Calculate Weights
        pref_pts = check_preferred_watchlist(symbol)
        macd_pts = check_macd_crossover(df, direction)
        pattern_pts = detect_double_top_bottom(df, direction)
        spy_aligned, sector_aligned = check_market_alignment(supabase, direction, symbol)

        spy_pts = config.WEIGHT_SPY_ALIGNMENT if spy_aligned else 0
        sector_pts = config.WEIGHT_SECTOR_ALIGNMENT if sector_aligned else 0

        total_score = pref_pts + macd_pts + pattern_pts + spy_pts + sector_pts

        # 3. Update the database
        supabase.table("sid_method_signal_watchlist").update({
            "market_score": total_score,
            "preferred_watchlist": pref_pts > 0,
            "macd_cross": macd_pts > 0,
            "pattern_confirmed": pattern_pts > 0,
            "spy_alignment": spy_aligned,
            "sector_alignment": sector_aligned,
            "last_updated": datetime.now().isoformat()
        }).eq("symbol", symbol).execute()

        logger.info(
            f"📊 {symbol} Scored: {total_score} pts (Pref: {pref_pts}, MACD: {macd_pts}, Pattern: {pattern_pts}, SPY: {spy_pts}, Sector: {sector_pts})")


def validate_staged_signals(supabase):
    """
    Validates staged signals: checks earnings window, RSI/MACD alignment,
    and marks is_ready=True only when ALL THREE indicators are aligned.
    """
    staged_signals = supabase.table("sid_method_signal_watchlist") \
        .select("*").eq("is_ready", False).execute()

    if not staged_signals.data:
        logger.info("No staged signals found to validate.")
        return

    for signal in staged_signals.data:
        symbol = signal['symbol']
        direction = signal['direction']

        try:
            # 1. EARNINGS CHECK (14-Day Rule)
            earnings_resp = supabase.table("earnings_calendar") \
                .select("report_date").eq("symbol", symbol) \
                .gte("report_date", datetime.now().date().isoformat()) \
                .order("report_date").limit(1).execute()

            next_earnings = None
            if earnings_resp.data:
                next_earnings = datetime.strptime(earnings_resp.data[0]['report_date'], '%Y-%m-%d').date()
                days_to_earnings = (next_earnings - datetime.now().date()).days
                if days_to_earnings <= config.EARNINGS_RESTRICTION_DAYS:
                    logger.info(f"🚫 {symbol} blocked: Earnings in {days_to_earnings} days.")
                    continue  # <-- continue is now OUTSIDE momentum block

            # 2. FETCH DATA
            data = supabase.table("market_data").select("*") \
                .eq("symbol", symbol).eq("timeframe", "1d") \
                .order("timestamp", desc=True).limit(100).execute()

            if len(data.data) < 50:
                continue

            df = pd.DataFrame(data.data).iloc[::-1]

            # 3. CALCULATE INDICATORS
            rsi_daily = calculate_daily_rsi(df)
            rsi_weekly = calculate_weekly_rsi(df)
            macd_data = calculate_daily_macd(df)

            # 4. CHECK SLOPES — direction-aware for all three
            d_rsi_turning = rsi_daily.iloc[-1] > rsi_daily.iloc[-2] if direction == 'LONG' \
                else rsi_daily.iloc[-1] < rsi_daily.iloc[-2]

            w_rsi_turning = False
            if rsi_weekly is not None and len(rsi_weekly) >= 2:
                w_rsi_turning = rsi_weekly.iloc[-1] > rsi_weekly.iloc[-2] if direction == 'LONG' \
                    else rsi_weekly.iloc[-1] < rsi_weekly.iloc[-2]  # ← THE FIX

            macd_line = macd_data['line']
            macd_turning = macd_line.iloc[-1] > macd_line.iloc[-2] if direction == 'LONG' \
                else macd_line.iloc[-1] < macd_line.iloc[-2]

            # 5. ALL THREE MUST ALIGN
            if all([d_rsi_turning, w_rsi_turning, macd_turning]):
                supabase.table("sid_method_signal_watchlist").update({
                    "is_ready": True,
                    "next_earnings": str(next_earnings) if next_earnings else None,
                    "last_updated": datetime.now().isoformat(),
                    "logic_trail": {
                        "event": "Alignments Confirmed",
                        "d_rsi_slope": "UP" if d_rsi_turning else "DOWN",
                        "w_rsi_slope": "UP" if w_rsi_turning else "DOWN",
                        "macd_slope": "UP" if macd_turning else "DOWN"
                    }
                }).eq("symbol", symbol).execute()
                logger.info(f"✅ {symbol} passed all alignments. Marked is_ready=True.")
            else:
                logger.debug(f"🟡 {symbol} still waiting: D_RSI={d_rsi_turning}, W_RSI={w_rsi_turning}, MACD={macd_turning}")

        except Exception as e:
            logger.error(f"❌ Error validating {symbol}: {e}")
