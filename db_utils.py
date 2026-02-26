from supabase import create_client, Client
from alpaca.data.historical import StockHistoricalDataClient
from alpaca.data.requests import StockSnapshotRequest
from datetime import datetime, timedelta, timezone

import config
from unified_logger import get_logger

logger = get_logger(__name__)


def get_clients():
    """
    Initializes and returns the Supabase and Alpaca clients.
    """
    url: str = config.SUPABASE_URL
    # Ensure config.SUPABASE_KEY is the Service Role Key
    key: str = config.SUPABASE_KEY
    supabase: Client = create_client(url, key)

    return {
        "supabase_client": supabase,
        "alpaca_client": StockHistoricalDataClient(config.APCA_API_KEY_ID, config.APCA_API_SECRET_KEY)
    }


def sync_latest_market_data():
    """
    Fetches latest snapshots from Alpaca and updates market_data table.
    Ensures scanner runs on the most recent prices.
    """
    clients = get_clients()
    supabase = clients['supabase_client']
    # Fix #11: reuse the client from get_clients() instead of creating a duplicate
    alpaca_data_client = clients['alpaca_client']

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


def cleanup_expired_signals():
    """
    Safety-net cleanup: removes non-active signals older than SIGNAL_EXPIRY_DAYS.

    Primary signal expiry is now RSI-based: validate_staged_signals() in scanner.py
    deletes a signal the moment RSI crosses the momentum room threshold (>45 for LONG,
    <55 for SHORT), meaning no room for the trade to reach RSI 50.

    This function is a fallback for signals that slipped through the RSI check
    (e.g., market data gaps, bot downtime). SIGNAL_EXPIRY_DAYS is intentionally
    generous (60 days) since most signals will already be gone by RSI-based expiry.
    """
    clients = get_clients()
    supabase = clients['supabase_client']

    # 1. Fetch non-active records only — skip is_active=True (open positions)
    staged = supabase.table("sid_method_signal_watchlist").select("symbol, rsi_touch_date").neq("is_active", True).execute()

    if not staged or not staged.data:
        return

    # 2. Use timezone-aware UTC now
    expiry_threshold = datetime.now(timezone.utc) - timedelta(days=config.SIGNAL_EXPIRY_DAYS)
    removed_count = 0

    for record in staged.data:
        symbol = record['symbol']

        # 3. Parse the date and force it to be UTC aware (.replace)
        # to match the expiry_threshold awareness
        raw_date = record['rsi_touch_date']
        touch_date = datetime.fromisoformat(raw_date).replace(tzinfo=timezone.utc)

        # 4. Perform the comparison
        if touch_date < expiry_threshold:
            supabase.table("sid_method_signal_watchlist").delete().eq("symbol", symbol).execute()
            logger.info(f"🧹 Removed expired signal: {symbol}")
            removed_count += 1

    if removed_count > 0:
        logger.info(f"Daily cleanup complete. Total symbols removed: {removed_count}")