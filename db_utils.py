import supabase
import alpaca
import config

import os
import pandas as pd
from datetime import datetime
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
import config


def get_clients():
    return {
        "supabase_client": create_client(config.SUPABASE_URL, config.SUPABASE_KEY),
        "alpaca_client": StockHistoricalDataClient(config.APCA_API_KEY_ID, config.APCA_API_SECRET_KEY)
    }


def sync_latest_market_data():
    """
    Fetches latest snapshots from Alpaca and updates market_data table.
    Ensures scanner runs on the most recent prices.
    """
    clients = get_clients()
    supabase = clients['supabase_client']
    alpaca_data_client = StockHistoricalDataClient(config.APCA_API_KEY_ID, config.APCA_API_SECRET_KEY)

    # 1. Get all symbols to update
    tickers_resp = supabase.table("ticker_reference").select("symbol").execute()
    symbols = [item['symbol'] for item in tickers_resp.data]

    logger.info(f"Syncing latest data for {len(symbols)} symbols...")

    # Alpaca snapshots can handle multiple symbols in batches
    for i in range(0, len(symbols), 200):
        batch = symbols[i:i + 200]
        try:
            snapshots = alpaca_data_client.get_stock_snapshot(StockSnapshotRequest(symbol_or_symbols=batch))

            rows_to_upsert = []
            for symbol, snapshot in snapshots.items():
                bar = snapshot.daily_bar
                if bar:
                    rows_to_upsert.append({
                        "symbol": symbol,
                        "timestamp": bar.timestamp.isoformat(),
                        "open": float(bar.open),
                        "high": float(bar.high),
                        "low": float(bar.low),
                        "close": float(bar.close),
                        "volume": int(bar.volume),
                        "timeframe": "1d"
                    })

            if rows_to_upsert:
                supabase.table("market_data").upsert(rows_to_upsert,
                                                     on_conflict="symbol, timestamp, timeframe").execute()

        except Exception as e:
            logger.error(f"Error syncing batch starting with {batch[0]}: {e}")


def cleanup_stale_signals():
    """
    Removes symbols from sid_method_signal_watchlist if more than
    28 bars have passed since the initial RSI extreme touch.
    """
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. Fetch all staged signals
    staged = supabase.table("sid_method_signal_watchlist").select("symbol, rsi_touch_date").execute()

    for record in staged.data:
        symbol = record['symbol']
        touch_date = record['rsi_touch_date']

        try:
            # 2. Count daily bars in market_data since the rsi_touch_date
            count_resp = supabase.table("market_data") \
                .select("timestamp", count='exact') \
                .eq("symbol", symbol) \
                .eq("timeframe", "1d") \
                .gte("timestamp", touch_date) \
                .execute()

            bars_passed = count_resp.count if count_resp.count else 0

            # 3. Remove if past 28 bars
            if bars_passed > 28:
                supabase.table("sid_method_signal_watchlist").delete().eq("symbol", symbol).execute()
                logger.info(f"🧹 Removed stale signal {symbol}: {bars_passed} bars passed since RSI touch.")

        except Exception as e:
            logger.error(f"Error checking expiry for {symbol}: {e}")

