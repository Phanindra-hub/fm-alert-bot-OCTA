# -*- coding: utf-8 -*-
"""Telegram Profit Alert.ipynb

FM_UpdateBot — Combined Two-Threaded Alert Bot
===============================================
Thread 1 (Listener)   : Polls Telegram every 60s, parses new trade messages,
                         updates shared WATCHLIST
Thread 2 (Monitor)    : Checks prices every 90s, fires profit milestone alerts
                         with high-water mark logic

RULES:
  - Same ticker posted again → fresh start (milestones reset)
  - Profit drops below high-water mark → freeze alerts until recovered
  - Alert ladder: $800 → +$300 steps to $2000 → +$500 steps beyond

REQUIRES:
  pip install alpaca-py requests pytz 
"""

 
import time
import threading
import requests
import json
import re
import pytz
import os
from datetime import datetime
from alpaca.data import StockHistoricalDataClient
from alpaca.data.requests import StockLatestQuoteRequest
from alpaca.data.timeframe import TimeFrame,TimeFrameUnit
from alpaca.data.requests import StockBarsRequest
from alpaca.data.historical import StockHistoricalDataClient# Create stock historical data client
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest,LimitOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from alpaca.trading.requests import GetOrdersRequest
from alpaca.trading.enums import QueryOrderStatus
 


BOT_TOKEN      = os.getenv("BOT_TOKEN")
SOURCE_CHANNEL = os.getenv("SOURCE_CHANNEL") #"-1003719728720" # bot READS trade picks from here
ALERT_CHANNEL  = os.getenv("ALERT_CHANNEL")   # bot SENDS profit alerts here

ALPACA_API_KEY    = os.getenv("ALPACA_API_KEY")
ALPACA_API_SECRET = os.getenv("ALPACA_API_SECRET")

LISTENER_INTERVAL  = 60    # Thread 1: check Telegram every 60s
MONITOR_INTERVAL   = 90    # Thread 2: check prices every 90s
TIMEZONE           = "US/Central"
MAX_PICK_AGE_DAYS  = 20    # Auto-remove picks older than this many days

# Alpaca
client = StockHistoricalDataClient(ALPACA_API_KEY,  ALPACA_API_SECRET)
trading_client=TradingClient(ALPACA_API_KEY,  ALPACA_API_SECRET)

# Profit alert thresholds
FIRST_ALERT            = 800
TIER2_STEP             = 200
TIER2_MAX              = 2000
TIER3_STEP             = 500
TIER3_MAX              = 10000
TIER4_STEP             = 1000
 
watchlist_lock = threading.Lock()

# WATCHLIST structure per ticker:
# {
#   "META": {
#       "entry":           536.00,
#       "qty":             50,
#       "buy_date":        "Mar 30, 2026",
#       "update_id":       123456789,      ← Telegram update_id (dedup key)
#       "next_alert_idx":  0,              ← next milestone index
#       "high_water":      0.0,            ← highest profit ever reached
#       "frozen":          False,          ← True if profit dropped below HWM
#   }
# }

WATCHLIST = {}

# Tracks last Telegram update_id processed by listener
last_update_id = 0
 
def is_market_hours():
    cst = pytz.timezone(TIMEZONE)
    now = datetime.now(cst)

    start = now.replace(hour=3, minute=1, second=0, microsecond=0)
    end   = now.replace(hour=18, minute=59, second=0, microsecond=0)

    return start <= now <= end
 
STATE_FILE = "/data/bot_state_OCTA.json"
"""
if os.path.exists(STATE_FILE):
    with open(STATE_FILE, "r") as f:
        data = json.load(f)

    print("\n===== STATE JSON BACKUP START =====")
    print(json.dumps(data, indent=2))
    print("===== STATE JSON BACKUP END =====\n")
else:
    print("STATE FILE NOT FOUND")
"""
def save_state():
    try:
        with watchlist_lock:
            data = {
                "watchlist": WATCHLIST,
                "last_update_id": last_update_id
            }
        with open(STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"[ERROR] Saving state: {e}")
 
def load_state():
    global WATCHLIST, last_update_id
    try:
        with open(STATE_FILE, "r") as f:
            data = json.load(f)
            WATCHLIST = data.get("watchlist", {})
            last_update_id = data.get("last_update_id", 0)
        print("✅ State loaded successfully")
    except FileNotFoundError:
        print("ℹ️ No previous state found (starting fresh)")
    except Exception as e:
        print(f"[ERROR] Loading state: {e}")
 
# ─────────────────────────────────────────────
#  Build profit alert ladder
# ─────────────────────────────────────────────

def build_alert_levels(max_profit=50000):
    levels = [FIRST_ALERT]
    level  = FIRST_ALERT + TIER2_STEP
    while level <= TIER2_MAX:
        levels.append(level)
        level += TIER2_STEP
    level = TIER2_MAX + TIER3_STEP
    while level <= TIER3_MAX:
        levels.append(level)
        level += TIER3_STEP
    level = TIER3_MAX + TIER4_STEP
    while level <= max_profit:
        levels.append(level)
        level += TIER4_STEP
    return sorted(set(levels))

ALERT_LEVELS = build_alert_levels()

def extend_ladder(idx):
    """Extend ALERT_LEVELS dynamically if needed."""
    """
    while idx >= len(ALERT_LEVELS):
        ALERT_LEVELS.append(ALERT_LEVELS[-1] + TIER4_STEP)
    """
    return
# ─────────────────────────────────────────────
#  Telegram helpers
# ─────────────────────────────────────────────

def send_telegram_message(message: str):
    url     = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {"chat_id": ALERT_CHANNEL, "text": message, "parse_mode": "HTML"}
    try:
        resp = requests.post(url, json=payload, timeout=10)
        if resp.status_code != 200:
            print(f"[ERROR] Telegram send: {resp.text}")
    except Exception as e:
        print(f"[ERROR] Telegram send: {e}")


def fetch_updates(offset=None, limit=20):
    url    = f"https://api.telegram.org/bot{BOT_TOKEN}/getUpdates"
    params = {"allowed_updates": ["channel_post"], "limit": limit, "timeout": 10}
    if offset:
        params["offset"] = offset
    try:
        resp = requests.get(url, params=params, timeout=15)
        return resp.json()
    except Exception as e:
        print(f"[ERROR] getUpdates: {e}")
        return {"ok": False, "result": []}


def acknowledge_update(update_id: int):
    """Tell Telegram we've processed up to this update_id."""
    fetch_updates(offset=update_id + 1, limit=1)
 
# ─────────────────────────────────────────────
#  Price fetch via Alpaca
# ─────────────────────────────────────────────

def get_current_price(ticker: str):
    try:
        pos   = trading_client.get_open_position(ticker)
        price = round(float(pos.market_value) / float(pos.qty), 2)
        return price
    except Exception as e:
        print(f"[ERROR] Price fetch {ticker}: {e}")
        return None
 
# ─────────────────────────────────────────────
#  Parsing (from your read_telegram_messages.py)
# ─────────────────────────────────────────────

def clean_text(text):
    text = text.upper()
    text = re.sub(r'\n+', ' ', text)
    text = re.sub(r'[^\w\s$.,+-]', '', text)
    return text

def extract_ticker(text):
    candidates = re.findall(r'\b[A-Z]{2,5}\b', text)
    blacklist = {
        "HIGH","RISK","MARKET","BUYING","SELLING","SHORT","STOP","GREAT","WITH","OCTA",
        "LOSS","PORTFOLIO","ADDED","COVER","TERM","EARNINGS","AM","AT",'AFTER','AGAIN',"SDP","STP"
    }

    tickers = [c for c in candidates if c not in blacklist and len(c)]

    for i in tickers:
      print(i)
      try:
          asset = trading_client.get_asset(i)
          if asset.tradable:
            return i
          #print("Exists:", asset is not None)
          #print("Tradable:", asset.tradable)
          #print("Status:", asset.status)

      except Exception as e:
          print("Invalid ticker:", i)
"""
def extract_ticker(text):
    candidates = re.findall(r'\b[A-Z]{2,5}\b', text)
    blacklist  = {
        "HIGH", "RISK", "MARKET", "BUYING", "SELLING", "SHORT", "STOP",
        "GREAT", "WITH", "LOSS", "PORTFOLIO", "ADDED", "COVER", "TERM",
        "EARNINGS", "AM", "AFTER", "AGAIN", "SDP", "THE", "AND", "FOR",
        "ARE", "THIS", "FROM", "THAT","STP"
    }
    for c in candidates:
        if c in blacklist:
            continue
        try:
            asset = trading_client.get_asset(c)
            if asset.tradable:
                return c
        except Exception:
            continue
    return None

"""
def extract_side(text):
    if "SHORT SELLING" in text or "SHORT SOLD" in text:
        return "SHORT"
    return "LONG"


def extract_quantity(text):
    match = re.search(r'I\s+AM(.*?)AT', text, re.IGNORECASE)
    if match:
        segment = match.group(1)
        qty     = re.search(r'\b(\d{1,5})\b', segment)
        return int(qty.group(1)) if qty else None
    return None

"""
def extract_entry_price(text):
    added_match = re.search(r'ADDED\s+AT\s*\$?\s*(\d+)', text)
    if added_match:
        return float(added_match.group(1))
    prices = re.findall(r'\$\s*(\d+(?:\.\d+)?)', text)
    return float(prices[0]) if prices else None
"""


def extract_entry_price(text):
    # Priority 1: ADDED AT $price
    added_match = re.search(r'ADDED\s+AT\s*\$?\s*(\d+(?:\.\d+)?)\+?', text)
    if added_match:
        return float(added_match.group(1))
    # Priority 2: I AM BUYING qty TICKER $price  (price right after ticker)
    ticker_price = re.search(r'I\s+AM\s+BUYING\s+\d+\s+[A-Z]+\s*\$?\s*(\d+(?:\.\d+)?)\+?', text)
    if ticker_price:
        return float(ticker_price.group(1))
    # Priority 3: I AM BUYING qty TICKER AT $price
    buying_at = re.search(r'I\s+AM\s+BUYING\s+\d+\s+[A-Z]+\s+AT\s*\$?\s*(\d+(?:\.\d+)?)\+?', text)
    if buying_at:
        return float(buying_at.group(1))
    # Last resort: first $ in message
    prices = re.findall(r'\$\s*(\d+(?:\.\d+)?)', text)
    return float(prices[0]) if prices else None
    

def extract_buy_date(text):
    """Try to extract a date from the message, fallback to today."""
    cst   = pytz.timezone(TIMEZONE)
    today = datetime.now(cst).strftime("%b %d, %Y")
    """
    # Look for patterns like Mar 30, Mar30, 30 Mar, 03/30 etc.
    patterns = [
        r'(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2}',
        r'\d{1,2}\s+(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)',
        r'\d{2}/\d{2}(?:/\d{2,4})?',
    ]
    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            return m.group(0)
    """
    return today


def parse_trade(msg: str, update_id: int):
    """
    Parse a channel message into a trade dict.
    Returns None if we can't extract the essential fields.
    """
    text = clean_text(msg)
    print(text)
    ticker      = extract_ticker(text)
    side        = extract_side(text)
    qty         = extract_quantity(text)
    entry_price = extract_entry_price(text)
    buy_date    = extract_buy_date(msg)   # use original case for date

    print('ticker',ticker)
    print('side',side)
    print('qty',qty)
    print('entry_price',entry_price)
    if not ticker or not qty or not entry_price:
        return None   # couldn't parse — skip

    return {
        "ticker":    ticker,
        "side":      side,
        "qty":       qty,
        "entry":     entry_price,
        "buy_date":  buy_date,
        "update_id": update_id,
    }
 
# ─────────────────────────────────────────────
#  Add / overwrite stock in WATCHLIST
# ─────────────────────────────────────────────

def add_to_watchlist(trade: dict):
    ticker    = trade["ticker"]
    update_id = trade["update_id"]

    with watchlist_lock:
        existing = WATCHLIST.get(ticker)

        # Skip if same update_id already processed
        if existing and existing.get("update_id") == update_id:
            return

        # Fresh start for this ticker
        WATCHLIST[ticker] = {
            "entry":          trade["entry"],
            "qty":            trade["qty"],
            "buy_date":       trade["buy_date"],
            "buy_date_raw":   datetime.now(pytz.timezone(TIMEZONE)).strftime("%Y-%m-%d"),
            "update_id":      update_id,
            "next_alert_idx": 0,
            "high_water":     0.0,
            "frozen":         False,
        }

    action = "Updated" if existing else "Added"
    print(f"  [{action}] {ticker} | Entry: ${trade['entry']:.2f} | "
          f"Qty: {trade['qty']} | update_id: {update_id}")
    """
    send_telegram_message(
        f"📥 <b>New Position Tracked — {ticker}</b>\n\n"
        f"📌 Entry Price: <b>${trade['entry']:.2f}</b>\n"
        f"📦 Quantity:    <b>{trade['qty']} shares</b>\n"
        f"📅 Bought on:   <b>{trade['buy_date']}</b>\n"
        f"🔜 First alert at <b>$800 profit</b>\n\n"
        f"#FortuneMarkers #{ticker}"
    )
    """
    # ✅ SAVE STATE (CRITICAL)
    save_state()
 
# ─────────────────────────────────────────────
#  Auto-purge picks older than MAX_PICK_AGE_DAYS
# ─────────────────────────────────────────────

def purge_old_picks():
    cst     = pytz.timezone(TIMEZONE)
    today   = datetime.now(cst).date()
    removed = []

    with watchlist_lock:
        for ticker, cfg in list(WATCHLIST.items()):
            raw = cfg.get("buy_date_raw")
            if not raw:
                continue
            try:
                pick_date = datetime.strptime(raw, "%Y-%m-%d").date()
                age_days  = (today - pick_date).days
                if age_days >= MAX_PICK_AGE_DAYS:
                    del WATCHLIST[ticker]
                    removed.append((ticker, age_days))
            except Exception as e:
                print(f"[WARN] Could not parse date for {ticker}: {e}")

    for ticker, age in removed:
        print(f"  🗑️  Removed {ticker} — {age} days old (limit: {MAX_PICK_AGE_DAYS})")
        """
        send_telegram_message(
            f"🗑️ <b>Pick Expired — {ticker}</b>\n\n"
            f"Automatically removed after <b>{age} days</b> "
            f"(limit: {MAX_PICK_AGE_DAYS} days).\n\n"
            f"#FortuneMarkers #{ticker}"
        )
        """
    if removed:
        save_state()


# ─────────────────────────────────────────────
#  THREAD 1 — Telegram Listener
# ─────────────────────────────────────────────

def telegram_listener():
    global last_update_id
    cst = pytz.timezone(TIMEZONE)

    print("[Listener] Thread started ✅")

    while True:
        try:
            offset = last_update_id + 1 if last_update_id else None
            data   = fetch_updates(offset=offset, limit=20)

            if data.get("ok") and data["result"]:
                for update in data["result"]:
                    uid  = update["update_id"]
                    post = update.get("channel_post") or update.get("message")

                    if post and post.get("text"):
                        # ── Only process posts from SOURCE_CHANNEL ──────
                        chat_id = str(post.get("chat", {}).get("id"))

                        if chat_id != str(SOURCE_CHANNEL):
                            print(f"  [Listener] Skipped — {chat_id} is not source channel")
                            last_update_id = uid
                            continue

                        raw_text = post["text"]
                        ts       = datetime.fromtimestamp(
                                       post["date"], pytz.utc
                                   ).astimezone(cst).strftime("%b %d, %Y %I:%M %p")

                        print(f"\n[Listener] New message from SOURCE at {ts}:")
                        print(f"  → {raw_text[:]}")

                        trade = parse_trade(raw_text, uid)
                        if trade:
                            add_to_watchlist(trade)
                        else:
                            print(f"  ⚠️  Could not parse trade from message (skipping)")

                    last_update_id = uid

        except Exception as e:
            print(f"[Listener ERROR] {e}")

        # Auto-purge picks older than MAX_PICK_AGE_DAYS
        purge_old_picks()

        time.sleep(LISTENER_INTERVAL)
 
#Corrected Version
def alert_monitor():
    cst = pytz.timezone(TIMEZONE)
    print("[Monitor] Thread started ✅")

    #BUFFER = 50  # prevents micro-fluctuation freezes

    # Brief delay so listener can do first pass first
    time.sleep(10)

    while True:

        if not is_market_hours():
            print("[Monitor] 💤 Outside trading hours — sleeping...")
            time.sleep(60)
            continue
        now   = datetime.now(cst).strftime("%b %d, %Y %I:%M %p")
        today = datetime.now(cst).strftime("%b %d, %Y")

        with watchlist_lock:
            tickers = list(WATCHLIST.keys())

        for ticker in tickers:
            price = get_current_price(ticker)
            if price is None:
                print(f"[Monitor] ⚠️ Skipping {ticker} — no price")
                continue

            with watchlist_lock:
                cfg = WATCHLIST.get(ticker)
                if cfg is None:
                    continue

                entry    = cfg["entry"]
                qty      = cfg["qty"]
                buy_date = cfg["buy_date"]
                hwm      = cfg["high_water"]
                frozen   = cfg["frozen"]
                idx      = cfg["next_alert_idx"]
            BUFFER = max(50, hwm * 0.035)
            profit = (price - entry) * qty
            pct    = ((price - entry) / entry) * 100

            print(f"[Monitor] {ticker}: ${price:.2f} | "
                  f"P&L: ${profit:+,.2f} ({pct:+.2f}%) | "
                  f"HWM: ${hwm:,.2f} | Frozen: {frozen}")

            # ── High-water mark logic ──────────────────────────────
            if profit > hwm:
                with watchlist_lock:
                    WATCHLIST[ticker]["high_water"] = profit
                    WATCHLIST[ticker]["frozen"] = False
                save_state()
                hwm = profit
                frozen = False

            elif frozen:
                print(f"  ❄️  {ticker} frozen — waiting to recover to ${hwm:,.2f}")
                continue

            elif hwm > 0 and profit < (hwm - BUFFER):
                with watchlist_lock:
                    WATCHLIST[ticker]["frozen"] = True
                save_state()
                print(f"  ❄️  {ticker} frozen — profit dropped from ${hwm:,.2f} to ${profit:+,.2f}")
                continue

            # ── Alert ladder — fires only highest milestone crossed ─
            # Find highest level crossed this cycle
            highest_level = None
            while idx < len(ALERT_LEVELS) and profit >= ALERT_LEVELS[idx]:
                highest_level = ALERT_LEVELS[idx]
                idx += 1
            
            if idx > WATCHLIST[ticker]["next_alert_idx"] and highest_level is not None:
                if highest_level == FIRST_ALERT:
                    tier_label = "🟢 First Profit Milestone"
                elif highest_level <= TIER2_MAX:
                    tier_label = "🔵 Profit Milestone"
                elif highest_level <= TIER3_MAX:
                    tier_label = "🟡 Major Profit Milestone"
                else:
                    tier_label = "🏆 Exceptional Profit Milestone"
            
                msg = (
                    f"{tier_label} — <b>{ticker}</b>\n\n"
                    f"💵 Profit has reached <b>${highest_level:,.0f}</b>!\n\n"
                    f"📌 Entry Price:   <b>${entry:.2f}</b>\n"
                    f"💰 Current Price: <b>${price:.2f}</b>\n"
                    f"📦 Quantity:      <b>{qty} shares</b>\n"
                    f"📈 Total Profit:  <b>${profit:+,.2f} ({pct:+.2f}%)</b>\n\n"
                    f"📅 Bought on:     <b>{buy_date}</b>\n"
                    f"📅 Today's date:  <b>{today}</b>\n\n"
                    f"🕐 {now}\n"
                    f"#FortuneMarkers #{ticker}"
                )
            
                send_telegram_message(msg)
                print(f"  → 🚨 Alert sent: {ticker} profit crossed ${highest_level:,}")
            
                with watchlist_lock:
                    WATCHLIST[ticker]["next_alert_idx"] = idx
            
                save_state()
            
            elif idx >= len(ALERT_LEVELS):
                print(f"  🏁 {ticker} reached max configured alert ladder")

        time.sleep(MONITOR_INTERVAL)
 
# ─────────────────────────────────────────────
#  MAIN — launch both threads
# ─────────────────────────────────────────────

def main():

    load_state()
    print("\n" + "=" * 52)
    print("   FM_UpdateBot — Two-Threaded Alert Bot")
    print("=" * 52)

    preview = "  →  ".join([f"${x:,}" for x in ALERT_LEVELS[:8]])
    print(f"\n  Alert ladder: {preview} → ...")
    print(f"\n  Listener interval : {LISTENER_INTERVAL}s")
    print(f"  Monitor interval  : {MONITOR_INTERVAL}s")
    print(f"  Source channel    : {SOURCE_CHANNEL}  ← reads trade picks")
    print(f"  Alert channel     : {ALERT_CHANNEL}   ← sends profit alerts")
    print(f"  Max pick age      : {MAX_PICK_AGE_DAYS} days (auto-purge)")
    """
    send_telegram_message(
        f"🤖 <b>FM Two-Threaded Alert Bot is LIVE!</b>\n\n"
        f"📡 <b>Listener:</b> Watching channel every {LISTENER_INTERVAL}s\n"
        f"🔔 <b>Monitor:</b> Checking prices every {MONITOR_INTERVAL}s\n\n"
        f"📬 Post a trade message to add stocks automatically!\n"
        f"❄️ High-water mark protection active\n\n"
        f"🔔 <b>Alert ladder:</b>\n"
        f"  🟢 First alert at <b>$800 profit</b>\n"
        f"  🔵 Then every <b>$300</b> → up to $2,000\n"
        f"  🏆 Then every <b>$500</b> beyond $2,000"
    )
    """
    # Launch Thread 1 — Listener
    t1 = threading.Thread(target=telegram_listener, name="Listener", daemon=True)
    t1.start()

    # Launch Thread 2 — Monitor
    t2 = threading.Thread(target=alert_monitor, name="Monitor", daemon=True)
    t2.start()

    print("\n  ✅ Both threads running. Press Ctrl+C to stop.\n")

    # Keep main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\n  🛑 Shutting down...")
 
if __name__ == "__main__":
    main()

 
