#!/usr/bin/env python3
"""
Top 10 Crypto Futures RSI + Daily UTC VWAP + 8h Funding — 24/7 Render Bot
-------------------------------------------------------------------------
- Runs 24/7 on Render (Web Service Free Tier or Background Worker)
- Includes a lightweight built-in HTTP health server on $PORT so Render Web
  Services pass port checks and can be kept awake 24/7 via UptimeRobot/cron-job.org
- 4-Source Auto Failover: Binance Vision -> Hyperliquid -> OKX -> Binance Futures
- Wilder's 14 RSI + Daily UTC VWAP + 8h Funding Filter + Hard SL (±2.0% / ±4.8%)
"""

import os
import sys
import time
import math
import threading
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta
import requests

# =============================================================================
# 1. CONFIGURATION (Render Environment Variables se read hoga)
# =============================================================================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

TIMEFRAME = os.getenv("TIMEFRAME", "15m").strip()
LONG_RSI_TH = float(os.getenv("LONG_RSI_TH", "35"))
SHORT_RSI_TH = float(os.getenv("SHORT_RSI_TH", "70"))
VWAP_FILTER_ENABLED = os.getenv("VWAP_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
FUNDING_FILTER_ENABLED = os.getenv("FUNDING_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on")
TG_COOLDOWN_MIN = int(os.getenv("TG_COOLDOWN_MIN", "15"))
POLL_INTERVAL_SEC = int(os.getenv("POLL_INTERVAL_SEC", "20"))
SEND_STARTUP_MSG = os.getenv("SEND_STARTUP_MSG", "true").strip().lower() in ("1", "true", "yes", "on")
PORT = int(os.getenv("PORT", "10000"))

# =============================================================================
# 2. TOP 10 FUTURES COINS & CONSTANTS
# =============================================================================
COINS = [
    {"coin": "BTC",  "pair": "BTCUSDT",  "okx": "BTC-USDT-SWAP"},
    {"coin": "ETH",  "pair": "ETHUSDT",  "okx": "ETH-USDT-SWAP"},
    {"coin": "SOL",  "pair": "SOLUSDT",  "okx": "SOL-USDT-SWAP"},
    {"coin": "XRP",  "pair": "XRPUSDT",  "okx": "XRP-USDT-SWAP"},
    {"coin": "BNB",  "pair": "BNBUSDT",  "okx": "BNB-USDT-SWAP"},
    {"coin": "DOGE", "pair": "DOGEUSDT", "okx": "DOGE-USDT-SWAP"},
    {"coin": "SUI",  "pair": "SUIUSDT",  "okx": "SUI-USDT-SWAP"},
    {"coin": "ADA",  "pair": "ADAUSDT",  "okx": "ADA-USDT-SWAP"},
    {"coin": "AVAX", "pair": "AVAXUSDT", "okx": "AVAX-USDT-SWAP"},
    {"coin": "LINK", "pair": "LINKUSDT", "okx": "LINK-USDT-SWAP"},
]

TF_MS = {"5m": 5 * 60 * 1000, "15m": 15 * 60 * 1000, "1h": 60 * 60 * 1000}
OKX_BAR = {"5m": "5m", "15m": "15m", "1h": "1H"}
RSI_PERIOD = 14
IST = timezone(timedelta(hours=5, minutes=30))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Crypto-RSI-VWAP-RenderBot/2.0"})

BOT_STATUS = {
    "started_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    "last_scan": "Initializing...",
    "scans_completed": 0,
    "alerts_sent": 0,
}


# =============================================================================
# 3. LIGHTWEIGHT HTTP HEALTH SERVER (Render Web Service Port Binding)
# =============================================================================
class HealthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        body = (
            f"OK | Crypto RSI+VWAP+Funding Bot Running 24/7\n"
            f"Timeframe: {TIMEFRAME} | Scans: {BOT_STATUS['scans_completed']} | "
            f"Alerts Sent: {BOT_STATUS['alerts_sent']} | Last Scan: {BOT_STATUS['last_scan']}\n"
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def log_message(self, format, *args):
        # Suppress noisy HTTP access logs
        return


def start_health_server():
    server = HTTPServer(("0.0.0.0", PORT), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    print(f"[HTTP] Health server listening on port {PORT}")


# =============================================================================
# 4. FORMATTING & MATH (WILDER RSI + DAILY UTC VWAP)
# =============================================================================
def fmt_price(v: float) -> str:
    if v is None or math.isnan(v):
        return "--"
    a = abs(v)
    if a == 0:
        d = 2
    elif a < 0.01:
        d = 6
    elif a < 1:
        d = 4
    elif a < 25:
        d = 3
    else:
        d = 2
    return f"{v:,.{d}f}"


def fmt_pct(v: float) -> str:
    if v is None or math.isnan(v):
        return "--"
    return f"{'+' if v > 0 else ''}{v:.2f}%"


def fmt_funding(v) -> str:
    if v is None or math.isnan(v):
        return "N/A"
    pct = v * 100.0
    return f"{'+' if pct > 0 else ''}{pct:.4f}%"


def now_ist_str() -> str:
    return datetime.now(IST).strftime("%H:%M:%S IST")


def get_utc_day_start_ms(ts_ms: int) -> int:
    dt = datetime.fromtimestamp(ts_ms / 1000.0, tz=timezone.utc)
    day_start = datetime(dt.year, dt.month, dt.day, tzinfo=timezone.utc)
    return int(day_start.timestamp() * 1000)


def compute_wilder_rsi(closes: list[float], period: int = RSI_PERIOD):
    if len(closes) <= period:
        return None
    gains = 0.0
    losses = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        if d >= 0:
            gains += d
        else:
            losses -= d
    avg_gain = gains / period
    avg_loss = losses / period

    for i in range(period + 1, len(closes)):
        d = closes[i] - closes[i - 1]
        avg_gain = (avg_gain * (period - 1) + (d if d > 0 else 0.0)) / period
        avg_loss = (avg_loss * (period - 1) + (-d if d < 0 else 0.0)) / period

    if avg_loss == 0:
        return 100.0
    rs = avg_gain / avg_loss
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def compute_daily_vwap(candles: list[dict]):
    if not candles:
        return None, None
    cur = candles[-1]
    day_start = get_utc_day_start_ms(cur["t"])
    pv = 0.0
    vol = 0.0
    for k in candles:
        if k["t"] >= day_start and k["v"] > 0:
            tp = (k["h"] + k["l"] + k["c"]) / 3.0
            pv += tp * k["v"]
            vol += k["v"]
    live_price = cur["c"]
    if vol > 0:
        vwap = pv / vol
        diff_pct = ((live_price - vwap) / vwap) * 100.0
        return vwap, diff_pct
    return live_price, 0.0


# =============================================================================
# 5. 4-SOURCE CANDLE & FUNDING FETCHERS
# =============================================================================
def candle_limit(tf: str) -> int:
    return 300 if tf == "5m" else 120


def fetch_funding_map() -> dict[str, float]:
    funding = {}
    try:
        r = SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=6)
        r.raise_for_status()
        for row in r.json():
            sym = row.get("symbol", "")
            if sym.endswith("USDT") and row.get("lastFundingRate") is not None:
                funding[sym[:-4]] = float(row["lastFundingRate"])
        if funding:
            return funding
    except Exception:
        pass

    try:
        r = SESSION.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "metaAndAssetCtxs"},
            timeout=6,
        )
        r.raise_for_status()
        meta, ctxs = r.json()
        for idx, u in enumerate(meta.get("universe", [])):
            if idx < len(ctxs) and ctxs[idx].get("funding") is not None:
                funding[u["name"]] = float(ctxs[idx]["funding"]) * 8.0
    except Exception:
        pass

    return funding


def fetch_binance(item: dict, tf: str, is_futures: bool = False):
    base = (
        "https://fapi.binance.com/fapi/v1/klines"
        if is_futures
        else "https://data-api.binance.vision/api/v3/klines"
    )
    r = SESSION.get(
        base,
        params={"symbol": item["pair"], "interval": tf, "limit": candle_limit(tf)},
        timeout=8,
    )
    r.raise_for_status()
    raw = r.json()
    if not isinstance(raw, list) or len(raw) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient Binance candles")
    return [
        {"t": int(k[0]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
        for k in raw
    ]


def fetch_hyperliquid(item: dict, tf: str):
    end_ms = int(time.time() * 1000)
    start_ms = end_ms - TF_MS[tf] * candle_limit(tf)
    r = SESSION.post(
        "https://api.hyperliquid.xyz/info",
        json={
            "type": "candleSnapshot",
            "req": {"coin": item["coin"], "interval": tf, "startTime": start_ms, "endTime": end_ms},
        },
        timeout=8,
    )
    r.raise_for_status()
    raw = r.json()
    if not isinstance(raw, list) or len(raw) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient Hyperliquid candles")
    return [
        {"t": int(k["t"]), "h": float(k["h"]), "l": float(k["l"]), "c": float(k["c"]), "v": float(k["v"])}
        for k in raw
    ]


def fetch_okx(item: dict, tf: str):
    r = SESSION.get(
        "https://www.okx.com/api/v5/market/candles",
        params={"instId": item["okx"], "bar": OKX_BAR[tf], "limit": candle_limit(tf)},
        timeout=8,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not isinstance(data, list) or len(data) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient OKX candles")
    data.reverse()
    return [
        {"t": int(k[0]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
        for k in data
    ]


PROVIDERS = [
    ("Binance Vision",  lambda item, tf: fetch_binance(item, tf, is_futures=False)),
    ("Hyperliquid",     fetch_hyperliquid),
    ("OKX Perps",       fetch_okx),
    ("Binance Futures", lambda item, tf: fetch_binance(item, tf, is_futures=True)),
]


def fetch_candles_with_failover(item: dict, tf: str):
    last_err = None
    for name, fn in PROVIDERS:
        try:
            return name, fn(item, tf)
        except Exception as exc:
            last_err = f"{name}: {exc}"
    raise RuntimeError(f"All providers failed for {item['pair']} ({last_err})")


# =============================================================================
# 6. SIGNAL LOGIC & TELEGRAM SENDER
# =============================================================================
def evaluate_signal(price: float, rsi: float, vwap: float, funding) -> str:
    if rsi is None:
        return "WAIT"
    above_vwap = (not VWAP_FILTER_ENABLED) or (vwap is not None and price >= vwap)
    below_vwap = (not VWAP_FILTER_ENABLED) or (vwap is not None and price <= vwap)

    long_funding_ok = (not FUNDING_FILTER_ENABLED) or (funding is None) or (funding <= 0.0001)
    short_funding_ok = (not FUNDING_FILTER_ENABLED) or (funding is None) or (funding >= 0.0)

    if rsi < LONG_RSI_TH and above_vwap and long_funding_ok:
        return "LONG"
    if rsi > SHORT_RSI_TH and below_vwap and short_funding_ok:
        return "SHORT"
    return "WAIT"


def send_telegram(html_text: str) -> bool:
    if not TG_BOT_TOKEN or not TG_CHAT_ID:
        print("[WARN] TG_BOT_TOKEN ya TG_CHAT_ID environment variable missing hai.")
        return False
    url = f"https://api.telegram.org/bot{TG_BOT_TOKEN}/sendMessage"
    try:
        r = SESSION.post(
            url,
            data={"chat_id": TG_CHAT_ID, "text": html_text, "parse_mode": "HTML"},
            timeout=10,
        )
        data = r.json()
        if not r.ok or not data.get("ok"):
            print(f"[TG ERROR] {data.get('description', r.status_code)}")
            return False
        return True
    except Exception as exc:
        print(f"[TG ERROR] {exc}")
        return False


def format_signal_msg(item: dict, side: str, price: float, rsi: float, vwap: float, vwap_diff: float, funding, source: str) -> str:
    is_long = side == "LONG"
    icon = "🟢" if is_long else "🔴"
    tp = price * 1.0628 if is_long else price * 0.9372
    avg = price * 0.9686 if is_long else price * 1.0314
    hard_sl = price * 0.98 if is_long else price * 1.02
    split_sl = price * 0.952 if is_long else price * 1.048

    vwap_pos = "ABOVE" if vwap_diff >= 0 else "BELOW"
    active_filters = []
    if VWAP_FILTER_ENABLED:
        active_filters.append("VWAP")
    if FUNDING_FILTER_ENABLED:
        active_filters.append("Funding")
    filter_tag = f" [{'+'.join(active_filters)} ON]" if active_filters else ""

    return (
        f"{icon} <b>{side} SIGNAL: {item['pair']}</b> ({TIMEFRAME.upper()}){filter_tag}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>RSI (14):</b> {rsi:.2f}\n"
        f"💵 <b>Mark Price:</b> ${fmt_price(price)}\n"
        f"📈 <b>Daily VWAP:</b> ${fmt_price(vwap)} ({vwap_pos} {fmt_pct(vwap_diff)})\n"
        f"💰 <b>8h Funding:</b> {fmt_funding(funding)}\n"
        f"🎯 <b>Target (2π ±6.28%):</b> ${fmt_price(tp)}\n"
        f"🔁 <b>Next Avg (π ±3.14%):</b> ${fmt_price(avg)}\n"
        f"🛑 <b>Hard SL (±2.0%):</b> ${fmt_price(hard_sl)} | <b>Split (±4.8%):</b> ${fmt_price(split_sl)}\n"
        f"📡 <b>Feed:</b> {source} (Render 24/7)\n"
        f"⏰ <b>Time:</b> {now_ist_str()}"
    )


# =============================================================================
# 7. MAIN 24/7 SCANNER LOOP
# =============================================================================
def main():
    if TIMEFRAME not in TF_MS:
        print(f"[ERROR] Unsupported TIMEFRAME: {TIMEFRAME}")
        sys.exit(1)

    start_health_server()

    print(
        f"[{now_ist_str()}] Render 24/7 Bot Started | TF: {TIMEFRAME} | "
        f"Long < {LONG_RSI_TH} | Short > {SHORT_RSI_TH} | "
        f"VWAP Filter: {VWAP_FILTER_ENABLED} | Funding Filter: {FUNDING_FILTER_ENABLED}"
    )

    if SEND_STARTUP_MSG:
        send_telegram(
            f"✅ <b>Render 24/7 Crypto Alert Bot Live!</b>\n"
            f"TF: <b>{TIMEFRAME.upper()}</b> | Long &lt; <b>{LONG_RSI_TH}</b> | Short &gt; <b>{SHORT_RSI_TH}</b>\n"
            f"VWAP Filter: <b>{'ON' if VWAP_FILTER_ENABLED else 'OFF'}</b> | "
            f"Funding Filter: <b>{'ON' if FUNDING_FILTER_ENABLED else 'OFF'}</b>\n"
            f"⏰ {now_ist_str()}"
        )

    last_sent = {}      # (coin, side) -> epoch timestamp
    prev_signals = {}   # coin -> previous signal
    cooldown_sec = max(1, TG_COOLDOWN_MIN) * 60

    while True:
        cycle_start = time.time()
        funding_map = fetch_funding_map()

        for item in COINS:
            try:
                source, candles = fetch_candles_with_failover(item, TIMEFRAME)
                closes = [k["c"] for k in candles]
                price = closes[-1]
                rsi = compute_wilder_rsi(closes, RSI_PERIOD)
                vwap, vwap_diff = compute_daily_vwap(candles)
                funding = funding_map.get(item["coin"])
                sig = evaluate_signal(price, rsi, vwap, funding)

                prev_sig = prev_signals.get(item["coin"])
                prev_signals[item["coin"]] = sig

                if sig in ("LONG", "SHORT"):
                    key = (item["coin"], sig)
                    now_ts = time.time()
                    if now_ts - last_sent.get(key, 0) >= cooldown_sec:
                        msg = format_signal_msg(item, sig, price, rsi, vwap, vwap_diff, funding, source)
                        if send_telegram(msg):
                            last_sent[key] = now_ts
                            BOT_STATUS["alerts_sent"] += 1
                            print(f"[{now_ist_str()}] >>> ALERT SENT: {sig} {item['pair']} (RSI {rsi:.2f})")

            except Exception as exc:
                print(f"[{now_ist_str()}] [ERR] {item['pair']}: {exc}")

        BOT_STATUS["scans_completed"] += 1
        BOT_STATUS["last_scan"] = now_ist_str()
        sys.stdout.flush()

        elapsed = time.time() - cycle_start
        time.sleep(max(2.0, POLL_INTERVAL_SEC - elapsed))


if __name__ == "__main__":
    main()
