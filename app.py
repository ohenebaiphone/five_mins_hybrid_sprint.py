#!/usr/bin/env python3
"""
app.py — Off-Peak Polymarket Trading Bot + Live Web Dashboard for Railway
"""

import json
import os
import ssl
import sys
import time
import datetime
import threading
import logging
from collections import deque
from flask import Flask, jsonify, render_template
import requests
import websocket
from dotenv import load_dotenv

load_dotenv()

# ── Config ──────────────────────────────────────────────────────────────────────
LIVE_WS_URL   = "wss://ws-live-data.polymarket.com/"
GAMMA_HOST    = "https://gamma-api.polymarket.com"
CLOB_HOST     = "https://clob.polymarket.com"
WINDOW_SECS   = 300          # 5-minute window
WAKE_UP_BEFORE      = 55    # seconds before close to start streaming/probing
STREAK_WIN_CAP      = 4     # Take Profit: Reset streak after 4 consecutive wins

# OFF-PEAK GUARDRAILS
BET_WINDOW_START    = 40    # earliest check for Streak 1 (T-40s)
BET_WINDOW_END      = 5     # latest check for all (T-5s)
MAX_EARLY_PRICE      = 0.75  # Max price allowed ($0.75 guarantees >= 33% profit payout)
MIN_MOVE_REQUIRED   = 10.0  # BTC must be at least $10.00 away from PTB

CONFIDENCE_THRESHOLD = 0.65  # dominant side must be priced at $0.65+ (65% confidence)
PROBE_MARKS = [40, 35, 30, 25, 20, 18, 15, 12, 10, 8, 5, 3, 2, 1, 0]

SETTLE_POLL_INTERVAL = 5
SETTLE_MAX_ATTEMPTS  = 60

STATE_FILE = "sprint_state.json"

# ── Global State ────────────────────────────────────────────────────────────────
sprint_stake = 1.00
sprint_wins = 0

streak_1_stake = 1.00
streak_1_wins = 0
streak_2_stake = 1.00
streak_2_wins = 0

wallet_balance = 0.00
current_phase = 1
btc_price = 0.00
btc_ptb = 0.00

trade_history = deque(maxlen=20)
console_logs = deque(maxlen=50)

log_lock = threading.Lock()
state_lock = threading.Lock()

def log_info(msg):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    formatted = f"[{timestamp}] {msg}"
    print(formatted)
    sys.stdout.flush()
    with log_lock:
        console_logs.append(formatted)

def record_trade(name, side, stake, outcome):
    timestamp = datetime.datetime.now().strftime("%H:%M:%S")
    with log_lock:
        trade_history.appendleft({
            "time": timestamp,
            "name": name,
            "side": side,
            "stake": stake,
            "outcome": outcome
        })

def save_state():
    with state_lock:
        try:
            state = {
                "sprint_stake": sprint_stake,
                "sprint_wins": sprint_wins,
                "streak_1_stake": streak_1_stake,
                "streak_1_wins": streak_1_wins,
                "streak_2_stake": streak_2_stake,
                "streak_2_wins": streak_2_wins
            }
            with open(STATE_FILE, "w") as f:
                json.dump(state, f, indent=4)
        except Exception as e:
            log_info(f"⚠️ Error saving state file: {e}")

def load_state():
    global sprint_stake, sprint_wins, streak_1_stake, streak_1_wins, streak_2_stake, streak_2_wins
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                state = json.load(f)
            sprint_stake = state.get("sprint_stake", 1.00)
            sprint_wins = state.get("sprint_wins", 0)
            streak_1_stake = state.get("streak_1_stake", 1.00)
            streak_1_wins = state.get("streak_1_wins", 0)
            streak_2_stake = state.get("streak_2_stake", 1.00)
            streak_2_wins = state.get("streak_2_wins", 0)
            log_info(f"📂 Loaded state from {STATE_FILE}.")
            return True
        except Exception as e:
            log_info(f"⚠️ Error loading state file: {e}")
    return False

# ── Web Server Setup ────────────────────────────────────────────────────────────
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)
logging.getLogger('werkzeug').setLevel(logging.ERROR)

@app.route('/')
def home():
    return render_template('dashboard.html')

@app.route('/api/status')
def api_status():
    with state_lock:
        s1_w = streak_1_wins
        s1_s = streak_1_stake
        s2_w = streak_2_wins
        s2_s = streak_2_stake
        spr_w = sprint_wins
        spr_s = sprint_stake

    with log_lock:
        logs_list = list(console_logs)
        trades_list = list(trade_history)

    return jsonify({
        "balance": wallet_balance,
        "phase": current_phase,
        "market": {
            "price": btc_price,
            "ptb": btc_ptb
        },
        "streak_1": {"wins": s1_w, "stake": s1_s},
        "streak_2": {"wins": s2_w, "stake": s2_s},
        "sprint": {"wins": spr_w, "stake": spr_s},
        "trades": trades_list,
        "logs": logs_list
    })

def make_ssl_ctx():
    ctx = ssl.create_default_context()
    for p in ("/etc/ssl/cert.pem", "/etc/ssl/certs/ca-certificates.crt"):
        if os.path.exists(p):
            ctx.load_verify_locations(p)
            break
    return ctx

WS_HEADERS = {
    "Origin": "https://polymarket.com",
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36"
    ),
}

def win_start(ts=None):
    t = ts if ts is not None else time.time()
    return int(t // WINDOW_SECS) * WINDOW_SECS

def win_end(ts=None):
    return win_start(ts) + WINDOW_SECS

def slug_for(ts=None):
    return f"btc-updown-5m-{win_start(ts)}"

def fmt(ts):
    return datetime.datetime.fromtimestamp(ts).strftime("%H:%M:%S")

def fmt_win(start_ts):
    return f"{fmt(start_ts)} → {fmt(start_ts + WINDOW_SECS)}"

class WSFeed:
    def __init__(self):
        self._price = None
        self._ts_ms = None
        self._lock  = threading.Lock()
        self._ready = threading.Event()
        self._stopped = False
        self._ws_app = None

    def start(self):
        ssl_ctx = make_ssl_ctx()

        def on_open(ws):
            ws.send(json.dumps({
                "action": "subscribe",
                "subscriptions": [{"topic": "crypto_prices_chainlink", "type": "update"}],
            }))

        def on_message(ws, raw):
            if not raw:
                return
            try:
                msg = json.loads(raw)
            except Exception:
                return
            if msg.get("topic") != "crypto_prices_chainlink":
                return
            p = msg.get("payload", {})
            if p.get("symbol") != "btc/usd":
                return
            with self._lock:
                self._price = p.get("value")
                self._ts_ms = p.get("timestamp")
                self._ready.set()

        def on_close(ws, close_status_code, close_msg):
            if not self._stopped:
                log_info("⚠️ WS feed disconnected — reconnecting in 2 seconds...")
                time.sleep(2)
                self.start()

        def on_error(ws, err):
            pass

        app = websocket.WebSocketApp(
            LIVE_WS_URL, header=WS_HEADERS,
            on_open=on_open, on_message=on_message,
            on_close=on_close, on_error=on_error
        )
        self._ws_app = app
        threading.Thread(
            target=lambda: app.run_forever(
                sslopt={"context": ssl_ctx}, ping_interval=20, ping_timeout=10
            ),
            daemon=True,
        ).start()
        self._ready.wait(timeout=20)

    def latest(self):
        self.check_staleness_and_reconnect()
        with self._lock:
            return (self._price, self._ts_ms) if (self._price is not None and self._ts_ms is not None) else None

    def price_at_or_after(self, ts_sec):
        self.check_staleness_and_reconnect()
        with self._lock:
            if self._ts_ms is None or self._price is None:
                return None
            if self._ts_ms >= ts_sec * 1000:
                return self._price, self._ts_ms
        return None

    def check_staleness_and_reconnect(self):
        with self._lock:
            if self._ts_ms is not None:
                lag = time.time() - (self._ts_ms / 1000.0)
                if lag > 15.0:
                    log_info(f"⚠️ WS feed stale (lag={lag:.1f}s) — reconnecting...")
                    self._ts_ms = None
                    self._price = None
                    if self._ws_app:
                        try:
                            self._ws_app.close()
                        except Exception:
                            pass

def resolve_market(slug, timeout=10):
    r = requests.get(f"{GAMMA_HOST}/events", params={"slug": slug}, timeout=timeout)
    r.raise_for_status()
    events = r.json()
    if not events:
        return None
    mkt = events[0]["markets"][0]
    token_ids = json.loads(mkt.get("clobTokenIds") or "[]")
    outcomes  = [str(o).lower() for o in json.loads(mkt.get("outcomes") or "[]")]
    up_id = down_id = None
    for i, o in enumerate(outcomes):
        if o in ("up", "yes"):    up_id   = token_ids[i]
        elif o in ("down", "no"): down_id = token_ids[i]
    if not up_id:   up_id   = token_ids[0]
    if not down_id: down_id = token_ids[1]
    return {
        "slug": slug,
        "title": mkt.get("question", slug),
        "up_id": up_id,
        "down_id": down_id,
    }

def probe_book(token_id, timeout=2):
    try:
        r = requests.get(f"{CLOB_HOST}/book", params={"token_id": token_id}, timeout=timeout)
        r.raise_for_status()
        asks = r.json().get("asks", [])
        if not asks:
            return None, 0
        best = float(min(asks, key=lambda a: float(a["price"]))["price"])
        return best, len(asks)
    except Exception:
        return None, 0

def check_resolution(slug):
    try:
        r = requests.get(f"{GAMMA_HOST}/events", params={"slug": slug}, timeout=5)
        r.raise_for_status()
        events = r.json()
        if not events:
            return None, []
        mkt    = events[0]["markets"][0]
        prices = [float(p) for p in json.loads(mkt.get("outcomePrices") or "[]")]
        if prices and max(prices) >= 0.99:
            return prices[0] >= 0.99, prices
        return None, prices
    except Exception:
        return None, []

def get_live_balance(clob_client):
    try:
        from py_clob_client_v2.clob_types import BalanceAllowanceParams, AssetType
        params = BalanceAllowanceParams(asset_type=AssetType.COLLATERAL)
        resp = clob_client.get_balance_allowance(params)
        raw_bal = float(resp.get("balance", 0))
        return raw_bal / 1_000_000.0
    except Exception as e:
        log_info(f"⚠️ Error fetching balance: {e}")
        return None

def run_probe_phase(ws: WSFeed, ptb: float, w_end: int, market: dict, clob_client=None, phase=1):
    global sprint_stake, streak_1_stake, streak_2_stake, btc_price
    
    last_tick_ts       = None
    last_price         = ptb
    probes_done        = set()
    
    s1_decided_side    = None
    s1_entry_price     = None
    
    s2_decided_side    = None
    s2_entry_price     = None

    last_clear_signal  = None

    while True:
        now       = time.time()
        remaining = w_end - now

        if remaining <= -3:
            break

        ws_data = ws.latest()
        if ws_data:
            price, ts_ms = ws_data
            btc_price = price
            if ts_ms != last_tick_ts:
                last_tick_ts = ts_ms
                last_price   = price

        for mark in PROBE_MARKS:
            if mark in probes_done:
                continue
            if remaining <= mark:
                probes_done.add(mark)
                up_ask,   _ = probe_book(market["up_id"])
                down_ask, _ = probe_book(market["down_id"])

                up_str = f"${up_ask:.4f}" if up_ask is not None else "NONE"
                down_str = f"${down_ask:.4f}" if down_ask is not None else "NONE"
                log_info(f"► PROBE T-{mark:>2}s  UP={up_str:<7}  DOWN={down_str:<7}  BTC=${last_price:,.2f}")

                current_move = last_price - ptb
                current_dir = "UP" if current_move > 0 else "DOWN"
                
                if up_ask is not None and down_ask is not None:
                    dominant = max(up_ask, down_ask)
                    new_signal = "UP" if up_ask > down_ask else "DOWN"
                    if dominant >= CONFIDENCE_THRESHOLD:
                        if new_signal != current_dir:
                            last_clear_signal = None
                        else:
                            last_clear_signal = new_signal
                    else:
                        last_clear_signal = None
                else:
                    last_clear_signal = None

                in_bet_window = (BET_WINDOW_END <= mark <= BET_WINDOW_START)
                if in_bet_window:
                    if int(last_price) == int(ptb):
                        continue
                        
                    # ── Phase 1: Sprint Mode ──
                    if phase == 1:
                        if s1_decided_side is None and last_clear_signal is not None:
                            if abs(last_price - ptb) >= MIN_MOVE_REQUIRED and last_clear_signal == current_dir:
                                sig_ask = up_ask if last_clear_signal == "UP" else down_ask
                                if sig_ask is not None:
                                    if sig_ask > MAX_EARLY_PRICE:
                                        log_info(f"⚠️ [SPRINT] Skipping early bet: contract price is too expensive (${sig_ask:.4f} > ${MAX_EARLY_PRICE:.2f}).")
                                        continue

                                    s1_decided_side = last_clear_signal
                                    s1_entry_price  = sig_ask
                                    
                                    order_msg = "PAPER OFF-PEAK SPRINT"
                                    if clob_client is not None:
                                        order_msg = "LIVE OFF-PEAK SPRINT"
                                        log_info(f"🚀 [SPRINT] PLACING LIVE ORDER: {s1_decided_side} outcome...")
                                        try:
                                            from py_clob_client_v2 import MarketOrderArgsV2
                                            token_id = market["up_id"] if s1_decided_side == "UP" else market["down_id"]
                                            resp = clob_client.create_and_post_market_order(
                                                order_args=MarketOrderArgsV2(
                                                    token_id=token_id, amount=sprint_stake, side="BUY"
                                                )
                                            )
                                            log_info(f"✅ [SPRINT] Live order response: {resp}")
                                        except Exception as e:
                                            log_info(f"❌ [SPRINT] Failed to place order: {e}")
                                            s1_decided_side = None
                                            s1_entry_price = None
                                            
                                    if s1_decided_side is not None:
                                        log_info(f"🎯 {order_msg} placed: {s1_decided_side} @ ${s1_entry_price:.4f} (Stake: ${sprint_stake:.2f})")
                                        record_trade("SPRINT", s1_decided_side, sprint_stake, "PENDING")

                    # ── Phase 2: Compounding Mode ──
                    else:
                        # Streak 1 (All-Rules Guarded)
                        if s1_decided_side is None and last_clear_signal is not None:
                            if abs(last_price - ptb) >= MIN_MOVE_REQUIRED and last_clear_signal == current_dir:
                                sig_ask = up_ask if last_clear_signal == "UP" else down_ask
                                if sig_ask is not None:
                                    if sig_ask > MAX_EARLY_PRICE:
                                        log_info(f"⚠️ [S1 OFF-PEAK] Skipping bet: contract price is too expensive (${sig_ask:.4f} > ${MAX_EARLY_PRICE:.2f}).")
                                        continue

                                    s1_decided_side = last_clear_signal
                                    s1_entry_price  = sig_ask
                                    
                                    order_msg = "PAPER BET [S1]"
                                    if clob_client is not None:
                                        order_msg = "LIVE BET [S1]"
                                        log_info(f"🚀 [S1] PLACING LIVE ORDER: {s1_decided_side} outcome...")
                                        try:
                                            from py_clob_client_v2 import MarketOrderArgsV2
                                            token_id = market["up_id"] if s1_decided_side == "UP" else market["down_id"]
                                            resp = clob_client.create_and_post_market_order(
                                                order_args=MarketOrderArgsV2(
                                                    token_id=token_id, amount=streak_1_stake, side="BUY"
                                                )
                                            )
                                            log_info(f"✅ [S1] Live order response: {resp}")
                                        except Exception as e:
                                            log_info(f"❌ [S1] Failed to place order: {e}")
                                            s1_decided_side = None
                                            s1_entry_price = None

                                    if s1_decided_side is not None:
                                        log_info(f"🎯 {order_msg} placed: {s1_decided_side} @ ${s1_entry_price:.4f} (Stake: ${streak_1_stake:.2f})")
                                        record_trade("STREAK 1", s1_decided_side, streak_1_stake, "PENDING")

                        # Streak 2 (Close-Only: T-12s to T-5s)
                        if s2_decided_side is None and (5 <= mark <= 12) and last_clear_signal is not None:
                            if abs(last_price - ptb) >= MIN_MOVE_REQUIRED and last_clear_signal == current_dir:
                                sig_ask = up_ask if last_clear_signal == "UP" else down_ask
                                if sig_ask is not None:
                                    if sig_ask > MAX_EARLY_PRICE:
                                        log_info(f"⚠️ [S2 OFF-PEAK] Skipping bet: contract price is too expensive (${sig_ask:.4f} > ${MAX_EARLY_PRICE:.2f}).")
                                        continue

                                    s2_decided_side = last_clear_signal
                                    s2_entry_price  = sig_ask
                                    
                                    order_msg = "PAPER BET [S2]"
                                    if clob_client is not None:
                                        order_msg = "LIVE BET [S2]"
                                        log_info(f"🚀 [S2] PLACING LIVE ORDER: {s2_decided_side} outcome...")
                                        try:
                                            from py_clob_client_v2 import MarketOrderArgsV2
                                            token_id = market["up_id"] if s2_decided_side == "UP" else market["down_id"]
                                            resp = clob_client.create_and_post_market_order(
                                                order_args=MarketOrderArgsV2(
                                                    token_id=token_id, amount=streak_2_stake, side="BUY"
                                                )
                                            )
                                            log_info(f"✅ [S2] Live order response: {resp}")
                                        except Exception as e:
                                            log_info(f"❌ [S2] Failed to place order: {e}")
                                            s2_decided_side = None
                                            s2_entry_price = None

                                    if s2_decided_side is not None:
                                        log_info(f"🎯 {order_msg} placed: {s2_decided_side} @ ${s2_entry_price:.4f} (Stake: ${streak_2_stake:.2f})")
                                        record_trade("STREAK 2", s2_decided_side, streak_2_stake, "PENDING")

        time.sleep(0.1)

    return s1_decided_side, s1_entry_price, s2_decided_side, s2_entry_price, last_price

def settle(slug, decided_side, entry_price, stake_usd, name="S1"):
    log_info(f"⏳ [{name}] Polling for market resolution ({slug})...")
    for attempt in range(1, SETTLE_MAX_ATTEMPTS + 1):
        time.sleep(SETTLE_POLL_INTERVAL)
        up_won, _ = check_resolution(slug)
        if up_won is not None:
            won = (decided_side == "UP" and up_won) or (decided_side == "DOWN" and not up_won)
            pnl = stake_usd * (1 / entry_price - 1) if won else -stake_usd
            status_text = "WIN" if won else "LOSS"
            
            log_info(f"🏆 RESULT [{name}]: {status_text} | P&L: ${pnl:>+.4f}")
            record_trade(name, decided_side, stake_usd, status_text)
            return won
    log_info(f"⚠️ [{name}] Market did not resolve within wait period.")
    record_trade(name, decided_side, stake_usd, "UNRESOLVED")
    return None

def bot_thread_worker():
    global sprint_stake, sprint_wins, streak_1_stake, streak_1_wins, streak_2_stake, streak_2_wins
    global wallet_balance, current_phase, btc_price, btc_ptb

    POLYMARKET_LIVE_TRADING = os.getenv("POLYMARKET_LIVE_TRADING", "False").lower() in ("true", "1", "yes")
    POLYMARKET_ADDRESS = os.getenv("POLYMARKET_ADDRESS")
    POLYMARKET_API_KEY = os.getenv("POLYMARKET_API_KEY")
    POLYMARKET_API_SECRET = os.getenv("POLYMARKET_API_SECRET")
    POLYMARKET_API_PASSPHRASE = os.getenv("POLYMARKET_API_PASSPHRASE")
    POLYMARKET_PRIVATE_KEY = os.getenv("POLYMARKET_PRIVATE_KEY")

    clob_client = None
    if POLYMARKET_LIVE_TRADING:
        log_info("⚠️ LIVE TRADING ENABLED! Initializing Polymarket CLOB Client...")
        try:
            from py_clob_client_v2 import ClobClient, ApiCreds
            from eth_account import Account
            
            eoa_address = Account.from_key(POLYMARKET_PRIVATE_KEY).address
            sig_type = 0
            funder_addr = None
            if POLYMARKET_ADDRESS and POLYMARKET_ADDRESS.lower() != eoa_address.lower():
                sig_type = 3
                funder_addr = POLYMARKET_ADDRESS

            creds = ApiCreds(
                api_key=POLYMARKET_API_KEY,
                api_secret=POLYMARKET_API_SECRET,
                api_passphrase=POLYMARKET_API_PASSPHRASE
            )
            clob_client = ClobClient(
                host=CLOB_HOST, chain_id=137, key=POLYMARKET_PRIVATE_KEY, creds=creds, signature_type=sig_type, funder=funder_addr
            )
            log_info("✅ CLOB Client initialized.")
        except Exception as e:
            log_info(f"❌ Error initializing CLOB client: {e}")
            sys.exit(1)

    ws = WSFeed()
    ws.start()

    for _ in range(40):
        if ws.latest():
            break
        time.sleep(0.5)
    tick = ws.latest()
    if not tick:
        log_info("❌ No WS data — check connection.")
        sys.exit(1)
    btc_price, _ = tick
    log_info(f"✅ WS connected — BTC/USD: ${btc_price:,.2f}")

    state_loaded = load_state()

    if POLYMARKET_LIVE_TRADING and clob_client is not None:
        bal = get_live_balance(clob_client)
        if bal is not None:
            wallet_balance = bal
            if bal >= 10.00:
                split_stake = max(1.00, round((bal - 0.10) / 2.0, 2))
                streak_1_stake = split_stake
                streak_2_stake = split_stake
                save_state()
            elif bal < 10.00 and (sprint_stake == 1.00 or bal > sprint_stake + 2.00):
                log_info(f"💰 Startup balance verification: ${bal:.2f} pUSD. Adjusting sprint stake...")
                sprint_stake = max(1.00, round(bal - 0.05, 2))
                save_state()
    else:
        save_state()

    while True:
        try:
            now       = time.time()
            w_s       = win_start(now)
            w_e       = win_end(now)
            secs_into = now - w_s

            if POLYMARKET_LIVE_TRADING and clob_client is not None:
                local_bal = get_live_balance(clob_client)
                if local_bal is not None:
                    wallet_balance = local_bal
                else:
                    wallet_balance = (sprint_stake + 0.05) if sprint_stake > 1.00 else 9.99

            current_phase = 1 if wallet_balance < 10.00 else 2

            if secs_into > 10:
                sleep_secs = w_e - time.time() + 0.5
                time.sleep(max(0, sleep_secs))
                now = time.time()
                w_s = win_start(now)
                w_e = w_s + WINDOW_SECS

            slug = slug_for(w_s)
            btc_ptb = None
            deadline = time.time() + 30
            while time.time() < deadline:
                result = ws.price_at_or_after(w_s)
                if result:
                    btc_ptb, ptb_ts_ms = result
                    break
                time.sleep(0.1)

            if btc_ptb is None:
                log_info("❌ Could not capture PTB — skipping window.")
                time.sleep(max(10, w_e - time.time()))
                continue

            market = None
            for _ in range(20):
                try:
                    market = resolve_market(slug)
                    if market:
                        break
                except Exception:
                    pass
                time.sleep(2)

            if not market:
                log_info("❌ Could not resolve market — skipping window.")
                time.sleep(max(10, w_e - time.time()))
                continue

            wake_at   = w_e - WAKE_UP_BEFORE
            wait_secs = wake_at - time.time()
            if wait_secs > 0:
                time.sleep(wait_secs)

            if POLYMARKET_LIVE_TRADING and clob_client is not None:
                new_bal = get_live_balance(clob_client)
                if new_bal is not None:
                    wallet_balance = new_bal
                    current_phase = 1 if wallet_balance < 10.00 else 2
                    if current_phase == 1:
                        sprint_stake = max(1.00, round(wallet_balance - 0.05, 2))
                    else:
                        split_stake = max(1.00, round((wallet_balance - 0.10) / 2.0, 2))
                        if streak_1_wins == 0:
                            streak_1_stake = split_stake
                        if streak_2_wins == 0:
                            streak_2_stake = split_stake
                    save_state()

            s1_side, s1_price, s2_side, s2_price, last_price = run_probe_phase(
                ws, btc_ptb, w_e, market, clob_client=clob_client, phase=current_phase
            )

            actual_direction = "UP" if (last_price > btc_ptb) else "DOWN"

            if current_phase == 1:
                if s1_side and s1_price:
                    won = (s1_side == actual_direction)
                    s_old = sprint_stake
                    if won:
                        sprint_wins += 1
                        sprint_stake = round(s_old / s1_price, 2)
                    else:
                        sprint_stake = 1.00
                        sprint_wins = 0

                    save_state()
                    threading.Thread(target=settle, args=(slug, s1_side, s1_price, s_old, "SPRINT"), daemon=True).start()

            else:
                s1_old = streak_1_stake
                if s1_side and s1_price:
                    s1_won = (s1_side == actual_direction)
                    if s1_won:
                        streak_1_wins += 1
                        streak_1_stake = round(s1_old / s1_price, 2)
                        if streak_1_wins >= STREAK_WIN_CAP:
                            bal = get_live_balance(clob_client) if clob_client else None
                            streak_1_stake = max(1.00, round((bal - 0.10) / 2.0, 2)) if bal else 1.00
                            streak_1_wins = 0
                    else:
                        bal = get_live_balance(clob_client) if clob_client else None
                        streak_1_stake = max(1.00, round((bal - 0.10) / 2.0, 2)) if bal else 1.00
                        streak_1_wins = 0
                    
                    save_state()
                    threading.Thread(target=settle, args=(slug, s1_side, s1_price, s1_old, "S1"), daemon=True).start()

                s2_old = streak_2_stake
                if s2_side and s2_price:
                    s2_won = (s2_side == actual_direction)
                    if s2_won:
                        streak_2_wins += 1
                        streak_2_stake = round(s2_old / s2_price, 2)
                        if streak_2_wins >= STREAK_WIN_CAP:
                            bal = get_live_balance(clob_client) if clob_client else None
                            streak_2_stake = max(1.00, round((bal - 0.10) / 2.0, 2)) if bal else 1.00
                            streak_2_wins = 0
                    else:
                        bal = get_live_balance(clob_client) if clob_client else None
                        streak_2_stake = max(1.00, round((bal - 0.10) / 2.0, 2)) if bal else 1.00
                        streak_2_wins = 0
                    
                    save_state()
                    threading.Thread(target=settle, args=(slug, s2_side, s2_price, s2_old, "S2"), daemon=True).start()

            time.sleep(2)

        except Exception as e:
            log_info(f"❌ Error in main loop: {e}")
            time.sleep(10)

if __name__ == "__main__":
    bot_thread = threading.Thread(target=bot_thread_worker, daemon=True)
    bot_thread.start()

    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
