import os
import logging
from datetime import datetime
import resend
from dotenv import load_dotenv
from db_utils import get_clients  # Aligned with modular structure
import config

# Setup logging with UTF-8 encoding for Windows emoji compatibility
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("sidbot.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)

# Load environment variables (API keys are pulled from config.py)
load_dotenv()
resend.api_key = config.RESEND_API_KEY


def get_tv_url(symbol, exchange):
    """
    Generates a TradingView chart URL using the format EXCHANGE:SYMBOL.
    Defaults to NYSE if exchange data is missing.
    """
    exch = exchange.upper() if exchange else "NYSE"
    return f"https://www.tradingview.com/chart/?symbol={exch}:{symbol}"


def generate_html_report():
    clients = get_clients()
    supabase = clients['supabase_client']

    # Fetch signals joined with ticker_reference to get the Exchange column
    data_resp = supabase.table("sid_method_signal_watchlist") \
        .select("*, ticker_reference(exchange)") \
        .execute()

    data = data_resp.data
    conf_rows, pot_rows, ready_count = "", "", 0

    for row in data:
        symbol, direction = row['symbol'], row['direction']

        # Extract exchange from the joined ticker_reference table
        exchange = row.get('ticker_reference', {}).get('exchange', 'NYSE')

        # Accessing the logic_trail structure established in scanner.py
        trail = row.get('logic_trail') or {}
        d_rsi_slope = trail.get('d_rsi_slope', 'N/A')
        w_rsi_slope = trail.get('w_rsi_slope', 'N/A')
        macd_slope = trail.get('macd_slope', 'N/A')

        # Preferred watchlist status from scanner scoring
        is_pref = row.get('preferred_watchlist', False)

        # Earnings display logic using config restriction days
        earn_date_str = row.get('next_earnings')
        if earn_date_str:
            try:
                earn_dt = datetime.strptime(earn_date_str, '%Y-%m-%d').date()
                days_left = (earn_dt - datetime.now().date()).days
                earn_disp = f"{days_left}d ({earn_date_str})"
                # Highlight if within the configured restriction window
                if 0 <= days_left <= config.EARNINGS_RESTRICTION_DAYS:
                    earn_disp = f'<span style="color:#e74c3c;font-weight:bold;">⚠️ {earn_disp}</span>'
            except:
                earn_disp = "N/A"
        else:
            earn_disp = "N/A"

        if row['is_ready']: ready_count += 1
        score = row.get('market_score', 0)
        color = "#27ae60" if direction == "LONG" else "#e74c3c"

        # Strategy metadata locked in by get_signals.py
        sl_strat = row.get('stop_loss_strategy', 'FIXED')
        exit_strat = row.get('exit_strategy', 'FIXED')

        row_html = f"""
            <tr>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;"><a href="{get_tv_url(symbol, exchange)}" style="color:#2962ff;font-weight:bold;text-decoration:none;">{symbol}</a></td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;"><span style="background:{color};color:white;padding:2px 6px;border-radius:4px;font-size:11px;">{direction}</span></td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;font-weight:bold;">{score}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">D:{d_rsi_slope} / W:{w_rsi_slope}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">{macd_slope}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;">{'✅' if is_pref else '❌'}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;font-size:11px;">{sl_strat}/{exit_strat}</td>
                <td style="padding:10px;border:1px solid #ddd;text-align:center;font-size:12px;">{earn_disp}</td>
            </tr>"""

        if row['is_ready']:
            conf_rows += row_html
        else:
            pot_rows += row_html

    headers = "<tr><th>Symbol</th><th>Dir</th><th>Score</th><th>RSI Slope</th><th>MACD</th><th>Pref</th><th>Strategy</th><th>Earnings</th></tr>"

    return f"""<html><body style="font-family:sans-serif;color:#333;line-height:1.6;"><div style="max-width:950px;margin:auto;padding:20px;">
        <h2 style="text-align:center;color:#2c3e50;">SidBot Intelligence Report</h2>
        <h3 style="color:#27ae60;border-bottom:2px solid #27ae60;">🚀 READY FOR ENTRY ({ready_count})</h3>
        <table style="width:100%;border-collapse:collapse;margin-bottom:30px;">
            <thead style="background:#f8f9fa;">{headers}</thead>
            <tbody>{conf_rows if conf_rows else '<tr><td colspan="8" style="text-align:center;">No ready signals.</td></tr>'}</tbody>
        </table>
        <h3 style="color:#3498db;border-bottom:2px solid #3498db;">⏳ WATCHLIST (Staged)</h3>
        <table style="width:100%;border-collapse:collapse;">
            <thead style="background:#f8f9fa;">{headers}</thead>
            <tbody>{pot_rows if pot_rows else '<tr><td colspan="8" style="text-align:center;">No signals found.</td></tr>'}</tbody>
        </table></div></body></html>"""


def send_report():
    try:
        html_body = generate_html_report()
        # Fix: Use single curly braces for the dictionary
        resend.Emails.send({
            "from": f"SidBot Advisor <{config.EMAIL_SENDER}>",
            "to": [config.EMAIL_RECEIVER],
            "subject": f"SidBot Daily Intelligence - {datetime.now().strftime('%b %d')}",
            "html": html_body
        })
        logger.info("✅ Daily intelligence report sent successfully.")
    except Exception as e:
        logger.error(f"Failed to send report: {e}") # Removed emoji for console safety


if __name__ == "__main__":
    send_report()