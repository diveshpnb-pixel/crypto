#!/usr/bin/env python3
"""
Top 10 Crypto Futures RSI + Daily UTC VWAP + 8h Funding — 2-Second 24/7 Live Web Terminal & Bot
------------------------------------------------------------------------------------------------
- Scans Top 10 Crypto Futures every 2 SECONDS (24/7, even when browser/PC is closed)
- Fast 1-Request Batch Live Price Engine + Concurrent Kline History Sync (Rate-Limit Safe)
- 4-Source Auto Failover: Binance Vision -> Hyperliquid -> OKX -> Binance Futures
- Exact Wilder's 14 RSI + Daily UTC VWAP + 8h Funding Filter + Hard SL (±2.0% / ±4.8%)
- Serves full dark-theme Sci-Fi live trading terminal website on Render ($PORT)
"""

import os
import sys
import json
import time
import math
import threading
from concurrent.futures import ThreadPoolExecutor
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse
from datetime import datetime, timezone, timedelta
import requests

# =============================================================================
# 1. CONFIGURATION (Render Environment Variables)
# =============================================================================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()

CONFIG = {
    "timeframe": os.getenv("TIMEFRAME", "15m").strip() or "15m",
    "long_th": float(os.getenv("LONG_RSI_TH", "35")),
    "short_th": float(os.getenv("SHORT_RSI_TH", "70")),
    "vwap_filter": os.getenv("VWAP_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"),
    "funding_filter": os.getenv("FUNDING_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"),
    "cooldown_min": int(os.getenv("TG_COOLDOWN_MIN", "15")),
}

POLL_INTERVAL_SEC = float(os.getenv("POLL_INTERVAL_SEC", "2"))
HISTORY_SYNC_SEC = int(os.getenv("HISTORY_SYNC_SEC", "30"))
SEND_STARTUP_MSG = os.getenv("SEND_STARTUP_MSG", "true").strip().lower() in ("1", "true", "yes", "on")
PORT = int(os.getenv("PORT", "10000"))

# =============================================================================
# 2. TOP 10 FUTURES COINS & CONSTANTS
# =============================================================================
COINS = [
    {"sr": 1,  "coin": "BTC",  "pair": "BTCUSDT",  "okx": "BTC-USDT-SWAP",  "name": "Bitcoin"},
    {"sr": 2,  "coin": "ETH",  "pair": "ETHUSDT",  "okx": "ETH-USDT-SWAP",  "name": "Ethereum"},
    {"sr": 3,  "coin": "SOL",  "pair": "SOLUSDT",  "okx": "SOL-USDT-SWAP",  "name": "Solana"},
    {"sr": 4,  "coin": "XRP",  "pair": "XRPUSDT",  "okx": "XRP-USDT-SWAP",  "name": "XRP"},
    {"sr": 5,  "coin": "BNB",  "pair": "BNBUSDT",  "okx": "BNB-USDT-SWAP",  "name": "BNB"},
    {"sr": 6,  "coin": "DOGE", "pair": "DOGEUSDT", "okx": "DOGE-USDT-SWAP", "name": "Dogecoin"},
    {"sr": 7,  "coin": "SUI",  "pair": "SUIUSDT",  "okx": "SUI-USDT-SWAP",  "name": "Sui"},
    {"sr": 8,  "coin": "ADA",  "pair": "ADAUSDT",  "okx": "ADA-USDT-SWAP",  "name": "Cardano"},
    {"sr": 9,  "coin": "AVAX", "pair": "AVAXUSDT", "okx": "AVAX-USDT-SWAP", "name": "Avalanche"},
    {"sr": 10, "coin": "LINK", "pair": "LINKUSDT", "okx": "LINK-USDT-SWAP", "name": "Chainlink"},
]

PAIR_TO_COIN = {c["pair"]: c["coin"] for c in COINS}
OKX_TO_COIN = {c["okx"]: c["coin"] for c in COINS}

TF_MS = {"5m": 5 * 60 * 1000, "15m": 15 * 60 * 1000, "1h": 60 * 60 * 1000}
OKX_BAR = {"5m": "5m", "15m": "15m", "1h": "1H"}
RSI_PERIOD = 14
IST = timezone(timedelta(hours=5, minutes=30))

SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "Crypto-RSI-VWAP-Render2s/4.0"})

STATE_LOCK = threading.Lock()
RESCAN_EVENT = threading.Event()
FORCE_HISTORY_SYNC = threading.Event()

SHARED_STATE = {
    "started_at": datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S IST"),
    "last_scan": "Initializing...",
    "active_feed": "Connecting...",
    "poll_interval_sec": POLL_INTERVAL_SEC,
    "scans_completed": 0,
    "alerts_sent": 0,
    "tg_configured": bool(TG_BOT_TOKEN and TG_CHAT_ID),
    "coins": [],
    "alert_history": [],
}


# =============================================================================
# 3. FORMATTING & TECHNICAL MATH (WILDER RSI + DAILY UTC VWAP)
# =============================================================================
def fmt_price(v) -> str:
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


def fmt_pct(v) -> str:
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


def compute_wilder_base(closes: list[float], period: int = RSI_PERIOD):
    if not closes or len(closes) <= period:
        return None, None
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

    return avg_gain, avg_loss


def compute_live_rsi(base_gain: float, base_loss: float, prev_close: float, live_price: float, period: int = RSI_PERIOD):
    if base_gain is None or base_loss is None or prev_close is None or live_price is None:
        return None
    d = live_price - prev_close
    ag = (base_gain * (period - 1) + (d if d > 0 else 0.0)) / period
    al = (base_loss * (period - 1) + (-d if d < 0 else 0.0)) / period
    if al == 0:
        return 100.0
    rs = ag / al
    return round(100.0 - (100.0 / (1.0 + rs)), 2)


def compute_vwap_base(closed_candles: list[dict], day_start_ms: int):
    pv = 0.0
    vol = 0.0
    for k in closed_candles:
        if k["t"] >= day_start_ms and k["v"] > 0:
            tp = (k["h"] + k["l"] + k["c"]) / 3.0
            pv += tp * k["v"]
            vol += k["v"]
    return pv, vol


def compute_live_vwap(cache_entry: dict, live_price: float):
    if live_price is None:
        return None, None
    cur_h = cache_entry.get("cur_high")
    cur_l = cache_entry.get("cur_low")
    cur_v = cache_entry.get("cur_vol", 0.0)

    h = max(cur_h, live_price) if cur_h is not None else live_price
    l = min(cur_l, live_price) if cur_l is not None else live_price
    cache_entry["cur_high"] = h
    cache_entry["cur_low"] = l

    cur_tp = (h + l + live_price) / 3.0
    total_pv = cache_entry.get("vwap_base_pv", 0.0) + cur_tp * cur_v
    total_vol = cache_entry.get("vwap_base_vol", 0.0) + cur_v

    if total_vol > 0:
        vwap = total_pv / total_vol
        diff_pct = ((live_price - vwap) / vwap) * 100.0
        return vwap, diff_pct
    return live_price, 0.0


# =============================================================================
# 4. 4-SOURCE CANDLE HISTORY & 2-SECOND BATCH LIVE TICKER FETCHERS
# =============================================================================
def candle_limit(tf: str) -> int:
    return 300 if tf == "5m" else 120


def fetch_funding_map() -> dict[str, float]:
    funding = {}
    try:
        r = SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=5)
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
            timeout=5,
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
        timeout=6,
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
        timeout=6,
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
        timeout=6,
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


def fetch_live_prices_batch() -> tuple[str, dict[str, float]]:
    try:
        symbols_param = json.dumps([c["pair"] for c in COINS], separators=(",", ":"))
        r = SESSION.get(
            "https://data-api.binance.vision/api/v3/ticker/price",
            params={"symbols": symbols_param},
            timeout=3.5,
        )
        r.raise_for_status()
        prices = {}
        for row in r.json():
            ck = PAIR_TO_COIN.get(row.get("symbol"))
            if ck and row.get("price") is not None:
                prices[ck] = float(row["price"])
        if len(prices) >= 5:
            return "Binance Vision (2s Live)", prices
    except Exception:
        pass

    try:
        r = SESSION.post(
            "https://api.hyperliquid.xyz/info",
            json={"type": "allMids"},
            timeout=3.5,
        )
        r.raise_for_status()
        mids = r.json()
        prices = {}
        for item in COINS:
            if item["coin"] in mids:
                prices[item["coin"]] = float(mids[item["coin"]])
        if len(prices) >= 5:
            return "Hyperliquid Perps (2s Live)", prices
    except Exception:
        pass

    try:
        r = SESSION.get(
            "https://www.okx.com/api/v5/market/tickers",
            params={"instType": "SWAP"},
            timeout=3.5,
        )
        r.raise_for_status()
        prices = {}
        for row in r.json().get("data", []):
            ck = OKX_TO_COIN.get(row.get("instId"))
            if ck and row.get("last") is not None:
                prices[ck] = float(row["last"])
        if len(prices) >= 5:
            return "OKX Perps (2s Live)", prices
    except Exception:
        pass

    try:
        r = SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=3.5)
        r.raise_for_status()
        prices = {}
        for row in r.json():
            ck = PAIR_TO_COIN.get(row.get("symbol"))
            if ck and row.get("markPrice") is not None:
                prices[ck] = float(row["markPrice"])
        if len(prices) >= 5:
            return "Binance Futures (2s Live)", prices
    except Exception:
        pass

    return "", {}


# =============================================================================
# 5. SIGNAL LOGIC & TELEGRAM SENDER
# =============================================================================
def evaluate_signal(price: float, rsi: float, vwap: float, funding, cfg: dict) -> str:
    if rsi is None or price is None:
        return "WAIT"
    above_vwap = (not cfg["vwap_filter"]) or (vwap is not None and price >= vwap)
    below_vwap = (not cfg["vwap_filter"]) or (vwap is not None and price <= vwap)

    long_funding_ok = (not cfg["funding_filter"]) or (funding is None) or (funding <= 0.0001)
    short_funding_ok = (not cfg["funding_filter"]) or (funding is None) or (funding >= 0.0)

    if rsi < cfg["long_th"] and above_vwap and long_funding_ok:
        return "LONG"
    if rsi > cfg["short_th"] and below_vwap and short_funding_ok:
        return "SHORT"
    return "WAIT"


def send_telegram(html_text: str, token: str = None, chat_id: str = None):
    tk = (token or TG_BOT_TOKEN).strip()
    cid = (chat_id or TG_CHAT_ID).strip()
    if not tk or not cid:
        return False, "Missing TG_BOT_TOKEN or TG_CHAT_ID on Render"
    url = f"https://api.telegram.org/bot{tk}/sendMessage"
    try:
        r = SESSION.post(
            url,
            data={"chat_id": cid, "text": html_text, "parse_mode": "HTML"},
            timeout=8,
        )
        data = r.json()
        if not r.ok or not data.get("ok"):
            err = data.get("description", f"HTTP {r.status_code}")
            print(f"[TG ERROR] {err}")
            return False, err
        return True, "OK"
    except Exception as exc:
        print(f"[TG ERROR] {exc}")
        return False, str(exc)


def format_signal_msg(item: dict, side: str, price: float, rsi: float, vwap: float, vwap_diff: float, funding, source: str, cfg: dict) -> str:
    is_long = side == "LONG"
    icon = "🟢" if is_long else "🔴"
    tp = price * 1.0628 if is_long else price * 0.9372
    avg = price * 0.9686 if is_long else price * 1.0314
    hard_sl = price * 0.98 if is_long else price * 1.02
    split_sl = price * 0.952 if is_long else price * 1.048

    vwap_pos = "ABOVE" if vwap_diff >= 0 else "BELOW"
    active_filters = []
    if cfg["vwap_filter"]:
        active_filters.append("VWAP")
    if cfg["funding_filter"]:
        active_filters.append("Funding")
    filter_tag = f" [{'+'.join(active_filters)} ON]" if active_filters else ""

    return (
        f"{icon} <b>{side} SIGNAL: {item['pair']}</b> ({cfg['timeframe'].upper()}){filter_tag}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"📊 <b>RSI (14):</b> {rsi:.2f}\n"
        f"💵 <b>Mark Price:</b> ${fmt_price(price)}\n"
        f"📈 <b>Daily VWAP:</b> ${fmt_price(vwap)} ({vwap_pos} {fmt_pct(vwap_diff)})\n"
        f"💰 <b>8h Funding:</b> {fmt_funding(funding)}\n"
        f"🎯 <b>Target (2π ±6.28%):</b> ${fmt_price(tp)}\n"
        f"🔁 <b>Next Avg (π ±3.14%):</b> ${fmt_price(avg)}\n"
        f"🛑 <b>Hard SL (±2.0%):</b> ${fmt_price(hard_sl)} | <b>Split (±4.8%):</b> ${fmt_price(split_sl)}\n"
        f"📡 <b>Feed:</b> {source}\n"
        f"⏰ <b>Time:</b> {now_ist_str()}"
    )


# =============================================================================
# 6. 2-SECOND 24/7 BACKGROUND SCANNER THREAD
# =============================================================================
def sync_all_candle_histories(tf: str, coin_cache: dict):
    funding_map = fetch_funding_map()

    def load_one(item):
        source, candles = fetch_candles_with_failover(item, tf)
        closed = candles[:-1]
        cur = candles[-1]
        closed_closes = [k["c"] for k in closed]
        prev_close = closed_closes[-1]
        base_gain, base_loss = compute_wilder_base(closed_closes, RSI_PERIOD)
        day_start = get_utc_day_start_ms(cur["t"])
        vwap_base_pv, vwap_base_vol = compute_vwap_base(closed, day_start)
        return item["coin"], {
            "loaded": True,
            "source": source,
            "prev_close": prev_close,
            "base_gain": base_gain,
            "base_loss": base_loss,
            "vwap_day_start": day_start,
            "vwap_base_pv": vwap_base_pv,
            "vwap_base_vol": vwap_base_vol,
            "cur_high": cur["h"],
            "cur_low": cur["l"],
            "cur_vol": cur["v"],
            "last_price": cur["c"],
            "funding": funding_map.get(item["coin"]),
        }

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = [pool.submit(load_one, item) for item in COINS]
        for fut in futures:
            try:
                ck, data = fut.result()
                coin_cache[ck] = data
            except Exception as exc:
                print(f"[{now_ist_str()}] [SYNC ERR] {exc}")


def scanner_worker():
    last_sent = {}
    coin_cache = {}
    last_history_sync = 0.0
    last_synced_tf = None
    last_candle_bucket = 0

    while True:
        cycle_start = time.time()
        with STATE_LOCK:
            cfg = dict(CONFIG)

        tf = cfg["timeframe"]
        cooldown_sec = max(1, cfg["cooldown_min"]) * 60
        now_ms = int(time.time() * 1000)
        cur_bucket = now_ms // TF_MS[tf]

        need_sync = (
            FORCE_HISTORY_SYNC.is_set()
            or (tf != last_synced_tf)
            or (cur_bucket > last_candle_bucket)
            or (time.time() - last_history_sync >= HISTORY_SYNC_SEC)
            or (len(coin_cache) < len(COINS))
        )

        if need_sync:
            FORCE_HISTORY_SYNC.clear()
            sync_all_candle_histories(tf, coin_cache)
            last_history_sync = time.time()
            last_synced_tf = tf
            last_candle_bucket = cur_bucket

        batch_feed, live_prices = fetch_live_prices_batch()

        scanned_rows = []
        for item in COINS:
            ck = item["coin"]
            c = coin_cache.get(ck)
            if not c or not c.get("loaded"):
                continue

            price = live_prices.get(ck, c.get("last_price"))
            if price is None:
                continue
            c["last_price"] = price

            prev_close = c["prev_close"]
            change_pct = ((price - prev_close) / prev_close) * 100.0 if prev_close else 0.0
            rsi = compute_live_rsi(c["base_gain"], c["base_loss"], prev_close, price, RSI_PERIOD)
            vwap, vwap_diff = compute_live_vwap(c, price)
            funding = c.get("funding")
            sig = evaluate_signal(price, rsi, vwap, funding, cfg)
            feed_name = batch_feed or c.get("source", "Live")

            scanned_rows.append({
                "sr": item["sr"],
                "coin": ck,
                "pair": item["pair"],
                "name": item["name"],
                "price": price,
                "prevClose": prev_close,
                "changePct": change_pct,
                "vwap": vwap,
                "vwapDiffPct": vwap_diff,
                "funding": funding,
                "rsi": rsi,
                "signal": sig,
                "source": feed_name,
            })

            if sig in ("LONG", "SHORT"):
                key = (ck, sig, tf)
                now_ts = time.time()
                if now_ts - last_sent.get(key, 0) >= cooldown_sec:
                    msg = format_signal_msg(item, sig, price, rsi, vwap, vwap_diff, funding, feed_name, cfg)
                    ok, _ = send_telegram(msg)
                    if ok:
                        last_sent[key] = now_ts
                        with STATE_LOCK:
                            SHARED_STATE["alerts_sent"] += 1
                            SHARED_STATE["alert_history"].insert(0, {
                                "time": datetime.now(IST).strftime("%d %b %H:%M:%S IST"),
                                "pair": item["pair"],
                                "side": sig,
                                "tf": tf.upper(),
                                "price": price,
                                "rsi": rsi,
                                "vwap": vwap,
                                "funding": funding,
                            })
                            SHARED_STATE["alert_history"] = SHARED_STATE["alert_history"][:30]
                        print(f"[{now_ist_str()}] >>> 2s ALERT SENT: {sig} {item['pair']} (RSI {rsi:.2f})")

        valid_sorted = sorted([r for r in scanned_rows if r["rsi"] is not None], key=lambda x: x["rsi"])
        rank_map = {r["coin"]: idx + 1 for idx, r in enumerate(valid_sorted)}
        for r in scanned_rows:
            r["rsiRank"] = rank_map.get(r["coin"])

        if scanned_rows:
            with STATE_LOCK:
                SHARED_STATE["coins"] = scanned_rows
                SHARED_STATE["scans_completed"] += 1
                SHARED_STATE["last_scan"] = now_ist_str()
                SHARED_STATE["active_feed"] = batch_feed or "Render 2s Live"

        sys.stdout.flush()
        elapsed = time.time() - cycle_start
        wait_sec = max(0.2, POLL_INTERVAL_SEC - elapsed)
        RESCAN_EVENT.wait(timeout=wait_sec)
        RESCAN_EVENT.clear()


# =============================================================================
# 7. EMBEDDED LIVE TRADING TERMINAL WEBSITE (HTML + CSS + VANILLA JS)
# =============================================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Top 10 Crypto Futures RSI + VWAP + Funding — 2s Render Live Terminal</title>
  <style>
    :root {
      --bg-main: #0b1120;
      --bg-header: rgba(11, 17, 32, 0.96);
      --bg-card: #111827;
      --bg-card-hover: #162033;
      --bg-input: #0f172a;
      --border: #1e293b;
      --border-light: #334155;
      --text-main: #f1f5f9;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --green: #10b981;
      --green-bg: rgba(16, 185, 129, 0.14);
      --green-border: rgba(16, 185, 129, 0.38);
      --red: #f43f5e;
      --red-bg: rgba(244, 63, 94, 0.14);
      --red-border: rgba(244, 63, 94, 0.38);
      --amber: #f59e0b;
      --amber-bg: rgba(245, 158, 11, 0.14);
      --amber-border: rgba(245, 158, 11, 0.35);
      --cyan: #06b6d4;
      --cyan-light: #67e8f9;
      --cyan-bg: rgba(6, 182, 212, 0.14);
      --cyan-border: rgba(6, 182, 212, 0.35);
      --purple: #a855f7;
      --purple-light: #d8b4fe;
      --purple-bg: rgba(168, 85, 247, 0.14);
      --purple-border: rgba(168, 85, 247, 0.38);
      --radius: 10px;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, Helvetica, Arial, sans-serif;
      --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    html, body {
      background-color: var(--bg-main);
      color: var(--text-main);
      font-family: var(--font-sans);
      font-size: 14px;
      line-height: 1.45;
      min-height: 100vh;
      -webkit-font-smoothing: antialiased;
    }

    ::-webkit-scrollbar { width: 7px; height: 7px; }
    ::-webkit-scrollbar-track { background: var(--bg-main); }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 99px; }
    ::-webkit-scrollbar-thumb:hover { background: #475569; }

    .mono {
      font-family: var(--font-mono);
      font-variant-numeric: tabular-nums;
      font-feature-settings: "tnum" 1;
    }

    .text-green { color: var(--green); }
    .text-red { color: var(--red); }
    .text-amber { color: var(--amber); }
    .text-cyan { color: var(--cyan-light); }
    .text-purple { color: var(--purple-light); }
    .text-muted { color: var(--text-muted); }
    .text-dim { color: var(--text-dim); }
    .text-right { text-align: right; }
    .text-center { text-align: center; }
    .hidden { display: none !important; }

    @keyframes flashUp {
      0% { background-color: rgba(16, 185, 129, 0.35); color: #6ee7b7; }
      100% { background-color: transparent; }
    }
    @keyframes flashDown {
      0% { background-color: rgba(244, 63, 94, 0.35); color: #fda4af; }
      100% { background-color: transparent; }
    }
    .tick-up { animation: flashUp 0.7s ease-out; }
    .tick-down { animation: flashDown 0.7s ease-out; }

    @keyframes pulseDot {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }
    .pulse-dot { animation: pulseDot 1.4s infinite ease-in-out; }

    /* ==================== SCI-FI HUD SIMULATION BACKGROUND ==================== */
    #sciFiBgCanvas {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
      opacity: 0.68;
    }

    .sci-fi-vignette {
      position: fixed;
      inset: 0;
      z-index: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at 50% 15%, rgba(6, 182, 212, 0.08), transparent 55%),
        radial-gradient(circle at 85% 80%, rgba(16, 185, 129, 0.07), transparent 50%),
        repeating-linear-gradient(0deg, rgba(6, 182, 212, 0.018) 0px, rgba(6, 182, 212, 0.018) 1px, transparent 1px, transparent 4px);
    }

    .header, .main, .toast-wrap {
      position: relative;
      z-index: 2;
    }
    .header {
      position: sticky;
      top: 0;
      z-index: 40;
    }
    .card, .kpi-card {
      background: rgba(17, 24, 39, 0.84) !important;
      backdrop-filter: blur(8px);
      box-shadow: 0 0 24px rgba(6, 182, 212, 0.04);
    }
    .brand-icon {
      position: relative;
      overflow: hidden;
      border: 1px solid rgba(103, 232, 249, 0.55);
      box-shadow: 0 0 18px rgba(6, 182, 212, 0.45), inset 0 0 10px rgba(255, 255, 255, 0.25);
    }

    .header {
      position: sticky;
      top: 0;
      z-index: 40;
      background: var(--bg-header);
      border-bottom: 1px solid var(--border);
      backdrop-filter: blur(10px);
    }

    .container {
      max-width: 1740px;
      margin: 0 auto;
      padding: 0 16px;
    }

    .header-inner {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 12px;
      padding: 12px 0;
    }

    .brand {
      display: flex;
      align-items: center;
      gap: 10px;
    }

    .brand-icon {
      width: 38px;
      height: 38px;
      border-radius: 10px;
      background: linear-gradient(135deg, #10b981, #06b6d4);
      display: flex;
      align-items: center;
      justify-content: center;
      color: #04131c;
      font-weight: 800;
      font-size: 13px;
      flex-shrink: 0;
      box-shadow: 0 4px 16px rgba(16, 185, 129, 0.25);
    }

    .brand-title {
      font-size: 16px;
      font-weight: 700;
      display: flex;
      align-items: center;
      flex-wrap: wrap;
      gap: 6px;
    }

    .brand-sub {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 1px;
    }

    .pill {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      padding: 2px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid var(--border);
    }

    .pill-tf {
      background: var(--cyan-bg);
      color: var(--cyan-light);
      border-color: var(--cyan-border);
    }

    .pill-cloud {
      background: var(--green-bg);
      color: #6ee7b7;
      border-color: var(--green-border);
      font-size: 10px;
    }

    .live-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 5px 12px;
      font-size: 12px;
    }

    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--amber);
      flex-shrink: 0;
    }
    .status-dot.live { background: var(--green); box-shadow: 0 0 8px rgba(16, 185, 129, 0.75); }
    .status-dot.error { background: var(--red); }

    .live-sep { color: var(--border-light); user-select: none; }

    .controls-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      padding: 10px 0;
      border-top: 1px solid rgba(30, 41, 59, 0.6);
    }

    .control-group {
      display: inline-flex;
      align-items: center;
      gap: 6px;
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: 8px;
      padding: 4px 8px;
      font-size: 12px;
    }

    .control-group label {
      color: var(--text-muted);
      font-size: 11px;
      font-weight: 600;
      white-space: nowrap;
    }

    select, input[type="number"], input[type="text"] {
      background: var(--bg-input);
      color: var(--text-main);
      border: 1px solid var(--border-light);
      border-radius: 6px;
      padding: 5px 8px;
      font-size: 12px;
      outline: none;
      transition: border-color 0.15s;
    }

    select:focus, input:focus { border-color: var(--cyan); }

    .input-sm {
      width: 52px;
      text-align: center;
      padding: 3px 4px !important;
      font-weight: 700;
    }

    .btn {
      display: inline-flex;
      align-items: center;
      justify-content: center;
      gap: 6px;
      padding: 6px 12px;
      border-radius: 8px;
      font-size: 12px;
      font-weight: 600;
      cursor: pointer;
      border: 1px solid var(--border-light);
      background: #1e293b;
      color: var(--text-main);
      transition: all 0.15s;
      white-space: nowrap;
    }
    .btn:hover { background: #273549; border-color: #475569; }
    .btn:active { transform: scale(0.97); }

    .btn-tf {
      padding: 5px 10px;
      border: 1px solid transparent;
      background: transparent;
      color: var(--text-muted);
      border-radius: 6px;
    }
    .btn-tf:hover { color: #fff; background: rgba(255,255,255,0.05); }
    .btn-tf.active {
      background: var(--green-bg);
      color: #34d399;
      border-color: var(--green-border);
    }

    .btn-vwap.active {
      background: var(--amber-bg);
      color: #fcd34d;
      border-color: var(--amber-border);
    }

    .btn-funding.active {
      background: var(--purple-bg);
      color: var(--purple-light);
      border-color: var(--purple-border);
    }

    .btn-sound.active {
      background: var(--green-bg);
      color: #34d399;
      border-color: var(--green-border);
    }

    .btn-tg.active {
      background: var(--cyan-bg);
      color: var(--cyan-light);
      border-color: var(--cyan-border);
    }

    .btn-primary {
      background: linear-gradient(135deg, #10b981, #06b6d4);
      color: #04131c;
      border: none;
      font-weight: 700;
    }
    .btn-primary:hover { filter: brightness(1.08); }

    .btn-long {
      padding: 4px 8px;
      font-size: 11px;
      background: var(--green-bg);
      color: #34d399;
      border-color: var(--green-border);
      border-radius: 6px;
    }
    .btn-long:hover { background: rgba(16, 185, 129, 0.25); }

    .btn-short {
      padding: 4px 8px;
      font-size: 11px;
      background: var(--red-bg);
      color: #fb7185;
      border-color: var(--red-border);
      border-radius: 6px;
    }
    .btn-short:hover { background: rgba(244, 63, 94, 0.25); }

    .btn-del {
      padding: 4px 7px;
      font-size: 11px;
      background: transparent;
      color: var(--text-muted);
      border-color: transparent;
    }
    .btn-del:hover { color: var(--red); background: var(--red-bg); }

    .main { padding: 16px 0 36px; }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(5, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 16px;
    }

    .kpi-card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 13px 14px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      transition: border-color 0.15s;
    }
    .kpi-card.clickable { cursor: pointer; }
    .kpi-card.clickable:hover { border-color: var(--border-light); background: var(--bg-card-hover); }

    .kpi-top {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 6px;
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      color: var(--text-muted);
    }

    .kpi-val {
      font-size: 20px;
      font-weight: 700;
      margin-top: 6px;
      display: flex;
      align-items: baseline;
      justify-content: space-between;
      gap: 8px;
    }

    .kpi-sub {
      font-size: 11px;
      color: var(--text-muted);
      margin-top: 4px;
      white-space: nowrap;
      overflow: hidden;
      text-overflow: ellipsis;
    }

    .tabs-row {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      border-bottom: 1px solid var(--border);
      padding-bottom: 10px;
      margin-bottom: 14px;
    }

    .tabs-list { display: flex; flex-wrap: wrap; gap: 6px; }

    .tab-btn {
      display: inline-flex;
      align-items: center;
      gap: 8px;
      padding: 8px 14px;
      border-radius: 8px;
      font-size: 13px;
      font-weight: 600;
      background: transparent;
      color: var(--text-muted);
      border: 1px solid transparent;
      cursor: pointer;
      transition: all 0.15s;
    }
    .tab-btn:hover { color: #fff; background: rgba(255,255,255,0.04); }
    .tab-btn.active {
      background: #1e293b;
      color: #fff;
      border-color: var(--border-light);
    }

    .tab-badge {
      padding: 1px 7px;
      border-radius: 99px;
      font-size: 11px;
      font-weight: 700;
      background: rgba(6, 182, 212, 0.2);
      color: var(--cyan-light);
    }

    .legend-inline {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 14px;
      font-size: 11px;
      color: var(--text-muted);
    }

    .card {
      background: var(--bg-card);
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
    }

    .table-wrap { overflow-x: auto; width: 100%; }

    table {
      width: 100%;
      border-collapse: collapse;
      white-space: nowrap;
      font-size: 13px;
    }

    thead th {
      position: sticky;
      top: 0;
      z-index: 10;
      background: #0f172a;
      color: var(--text-muted);
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding: 11px 12px;
      border-bottom: 1px solid var(--border);
      user-select: none;
    }

    thead th.sortable { cursor: pointer; }
    thead th.sortable:hover { color: #fff; }

    tbody tr {
      border-bottom: 1px solid rgba(30, 41, 59, 0.7);
      transition: background-color 0.12s;
    }
    tbody tr:last-child { border-bottom: none; }
    tbody tr:hover { background: rgba(30, 41, 59, 0.45); }
    tbody tr.row-long { background: rgba(16, 185, 129, 0.06); }
    tbody tr.row-long:hover { background: rgba(16, 185, 129, 0.12); }
    tbody tr.row-short { background: rgba(244, 63, 94, 0.06); }
    tbody tr.row-short:hover { background: rgba(244, 63, 94, 0.12); }

    tbody td { padding: 10px 12px; vertical-align: middle; }

    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid transparent;
    }
    .badge-long { background: var(--green-bg); color: var(--green); border-color: var(--green-border); }
    .badge-short { background: var(--red-bg); color: var(--red); border-color: var(--red-border); }
    .badge-wait { background: var(--amber-bg); color: var(--amber); border-color: var(--amber-border); }
    .badge-be { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .badge-neutral { background: #1e293b; color: #cbd5e1; border-color: #334155; }

    .rsi-cell {
      display: flex;
      align-items: center;
      gap: 8px;
      min-width: 115px;
    }
    .rsi-bar-bg {
      flex: 1;
      height: 6px;
      background: #1e293b;
      border-radius: 99px;
      overflow: hidden;
    }
    .rsi-bar-fill {
      height: 100%;
      border-radius: 99px;
      transition: width 0.3s ease;
    }

    .action-btns { display: inline-flex; gap: 5px; }

    .signals-grid {
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 16px;
    }

    .signal-header {
      padding: 12px 16px;
      display: flex;
      align-items: center;
      justify-content: space-between;
      border-bottom: 1px solid var(--border);
    }
    .signal-header.long-hd { background: rgba(16, 185, 129, 0.09); border-bottom-color: var(--green-border); }
    .signal-header.short-hd { background: rgba(244, 63, 94, 0.09); border-bottom-color: var(--red-border); }

    .empty-state {
      padding: 36px 20px;
      text-align: center;
      color: var(--text-muted);
      font-size: 13px;
    }

    .summary-grid {
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 14px;
    }

    .form-card { padding: 16px; margin-bottom: 14px; }

    .form-header {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      justify-content: space-between;
      gap: 8px;
      margin-bottom: 12px;
    }

    .form-grid {
      display: grid;
      grid-template-columns: repeat(8, minmax(0, 1fr));
      gap: 10px;
      align-items: end;
    }

    .field label {
      display: block;
      font-size: 11px;
      color: var(--text-muted);
      margin-bottom: 4px;
      font-weight: 600;
    }
    .field input, .field select { width: 100%; padding: 7px 9px; }

    .toast-wrap {
      position: fixed;
      bottom: 16px;
      right: 16px;
      z-index: 60;
      display: flex;
      flex-direction: column;
      gap: 8px;
      max-width: 340px;
      pointer-events: none;
    }

    .toast {
      pointer-events: auto;
      padding: 10px 14px;
      border-radius: 10px;
      font-size: 12px;
      font-weight: 600;
      background: #0f172a;
      border: 1px solid var(--border-light);
      color: var(--text-main);
      box-shadow: 0 10px 25px rgba(0,0,0,0.45);
      animation: fadeSlide 0.25s ease-out;
    }
    .toast.long { border-color: var(--green-border); color: #6ee7b7; background: #06281e; }
    .toast.short { border-color: var(--red-border); color: #fda4af; background: #2d0a15; }
    .toast.error { border-color: var(--red-border); color: #fecdd3; background: #2d0a15; }

    @keyframes fadeSlide {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 1280px) {
      .form-grid { grid-template-columns: repeat(4, minmax(0, 1fr)); }
    }
    @media (max-width: 1150px) {
      .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 860px) {
      .signals-grid { grid-template-columns: 1fr; }
      .legend-inline { display: none; }
    }
    @media (max-width: 640px) {
      .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .summary-grid { grid-template-columns: 1fr; }
      .form-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <!-- Sci-Fi Live HUD Simulation Background -->
  <canvas id="sciFiBgCanvas"></canvas>
  <div class="sci-fi-vignette"></div>

  <header class="header">
    <div class="container">
      <div class="header-inner">
        <div class="brand">
          <div class="brand-icon" title="Quantum RVF Live Terminal"><svg width="26" height="26" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
  <defs>
    <linearGradient id="sciGrad" x1="0" y1="0" x2="36" y2="36" gradientUnits="userSpaceOnUse">
      <stop offset="0%" stop-color="#04131c"/>
      <stop offset="100%" stop-color="#022c22"/>
    </linearGradient>
  </defs>
  <polygon points="18,2 32,10 32,26 18,34 4,26 4,10" fill="url(#sciGrad)" stroke="#04131c" stroke-width="1.5"/>
  <circle cx="18" cy="18" r="10.5" stroke="#34d399" stroke-width="1.2" stroke-dasharray="4 3"/>
  <path d="M9 21L14 15L18.5 19.5L27 11" stroke="#67e8f9" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/>
  <circle cx="27" cy="11" r="2.5" fill="#10b981"/>
  <circle cx="14" cy="15" r="1.8" fill="#67e8f9"/>
</svg></div>
          <div>
            <div class="brand-title">
              <span>Top 10 Crypto Futures RSI + VWAP + Funding</span>
              <span id="headerTfBadge" class="pill pill-tf mono">15M</span>
              <span class="pill pill-cloud mono">2S SCAN • 24/7 RENDER</span>
            </div>
            <div class="brand-sub">Har 2 Second Live Scan (Browser band hone par bhi) • Wilder's 14 RSI + Daily UTC VWAP + 8h Funding Filter</div>
          </div>
        </div>

        <div class="live-bar">
          <span id="liveDot" class="status-dot pulse-dot"></span>
          <strong id="liveFeedName" class="mono">CONNECTING TO RENDER...</strong>
          <span class="live-sep">|</span>
          <span class="text-muted">2s Scans: <strong id="liveScanCount" class="mono text-cyan">0</strong></span>
          <span class="live-sep">|</span>
          <span id="liveTimestamp" class="mono text-muted">--:--:-- IST</span>
          <button id="rescanBtn" type="button" class="btn" style="padding: 3px 9px; font-size: 11px;">
            ↻ Sync History
          </button>
        </div>
      </div>

      <div class="controls-bar">
        <div class="control-group" style="padding: 2px;">
          <button type="button" data-tf="5m" class="btn btn-tf">5m</button>
          <button type="button" data-tf="15m" class="btn btn-tf active">15m</button>
          <button type="button" data-tf="1h" class="btn btn-tf">1h</button>
        </div>

        <div class="control-group">
          <label for="longThInput" class="text-green">Long RSI &lt;</label>
          <input id="longThInput" type="number" min="10" max="55" value="35" class="input-sm mono text-green" />
        </div>

        <div class="control-group">
          <label for="shortThInput" class="text-red">Short RSI &gt;</label>
          <input id="shortThInput" type="number" min="45" max="90" value="70" class="input-sm mono text-red" />
        </div>

        <button id="vwapFilterToggleBtn" type="button" class="btn btn-vwap active" title="Render 2s Scanner par LONG sirf Price >= VWAP aur SHORT sirf Price <= VWAP">
          <span>⚡</span>
          <span id="vwapFilterLabel">VWAP Filter: ON</span>
        </button>

        <button id="fundingFilterToggleBtn" type="button" class="btn btn-funding active" title="Render 2s Scanner par LONG sirf Funding <= +0.01% aur SHORT sirf Funding >= 0%">
          <span>💰</span>
          <span id="fundingFilterLabel">Funding Filter: ON</span>
        </button>

        <button id="soundToggleBtn" type="button" class="btn btn-sound">
          <span id="soundIcon">🔇</span>
          <span id="soundLabel">Sound: OFF</span>
        </button>

        <button id="tgTestBtn" type="button" class="btn btn-tg active" title="Render 24/7 Telegram Bot Test Message bhejein">
          <span>✈️</span>
          <span id="tgStatusLabel">24/7 TG Bot: READY (Test)</span>
        </button>
      </div>
    </div>
  </header>

  <main class="main container">
    <section class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-top">
          <span>Render 2s Live Sync</span>
          <span id="kpiTfTag" class="mono text-cyan">15m</span>
        </div>
        <div id="kpiSyncTime" class="kpi-val mono" style="font-size: 16px;">--:--:-- IST</div>
        <div id="kpiFeedDetail" class="kpi-sub">Loading server snapshot...</div>
      </div>

      <div id="kpiLongCard" class="kpi-card clickable">
        <div class="kpi-top">
          <span>Long Signals (&lt; <span class="long-th-txt">35</span>)</span>
          <span class="text-green">View →</span>
        </div>
        <div class="kpi-val mono">
          <span id="kpiLongCount" class="text-green">0</span>
          <span class="text-dim" style="font-size: 12px;">/ 10</span>
        </div>
        <div id="kpiLongSub" class="kpi-sub">Oversold + Above VWAP + Fund ≤ +0.01%</div>
      </div>

      <div id="kpiShortCard" class="kpi-card clickable">
        <div class="kpi-top">
          <span>Short Signals (&gt; <span class="short-th-txt">70</span>)</span>
          <span class="text-red">View →</span>
        </div>
        <div class="kpi-val mono">
          <span id="kpiShortCount" class="text-red">0</span>
          <span class="text-dim" style="font-size: 12px;">/ 10</span>
        </div>
        <div id="kpiShortSub" class="kpi-sub">Overbought + Below VWAP + Fund ≥ 0%</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Most Oversold</span>
          <span class="text-green mono">Lowest RSI</span>
        </div>
        <div class="kpi-val">
          <span id="kpiOversoldCoin" style="font-size: 16px;">--</span>
          <span id="kpiOversoldRsi" class="mono text-green">--</span>
        </div>
        <div id="kpiOversoldPrice" class="kpi-sub mono">Mark: --</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Most Overbought</span>
          <span class="text-red mono">Highest RSI</span>
        </div>
        <div class="kpi-val">
          <span id="kpiOverboughtCoin" style="font-size: 16px;">--</span>
          <span id="kpiOverboughtRsi" class="mono text-red">--</span>
        </div>
        <div id="kpiOverboughtPrice" class="kpi-sub mono">Mark: --</div>
      </div>
    </section>

    <div class="tabs-row">
      <div class="tabs-list">
        <button type="button" data-tab="screener" class="tab-btn active">Top 10 Futures Screener</button>
        <button type="button" data-tab="signals" class="tab-btn">
          <span>Active Signals (LONG &amp; SHORT)</span>
          <span id="tabSignalsBadge" class="tab-badge mono">0</span>
        </button>
        <button type="button" data-tab="tradelog" class="tab-btn">
          <span>Futures Trade Log</span>
          <span id="tabTradesBadge" class="tab-badge mono">0</span>
        </button>
        <button type="button" data-tab="alerts" class="tab-btn">
          <span>24/7 Telegram Alert History</span>
          <span id="tabAlertsBadge" class="tab-badge mono">0</span>
        </button>
      </div>

      <div class="legend-inline">
        <span><strong class="text-cyan">●</strong> VWAP: Daily (00:00 UTC)</span>
        <span><strong class="text-green">●</strong> TP: ±6.28% (2π)</span>
        <span><strong class="text-amber">●</strong> Avg: ±3.14% (π)</span>
        <span><strong class="text-red">●</strong> Hard SL: ±2.0% (Split ±4.8%)</span>
      </div>
    </div>

    <section id="tab-screener" class="tab-panel">
      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th class="sortable" data-sort="sr">Rank <span class="sort-ind"></span></th>
                <th class="sortable" data-sort="coin">Coin / Pair <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="price">Live Mark Price <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="vwapDiffPct">Daily VWAP (UTC) <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="prevClose">Prev Close <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="changePct">Candle Chg % <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="funding">8h Funding <span class="sort-ind"></span></th>
                <th class="sortable" data-sort="rsi">14-Period RSI <span class="sort-ind"></span></th>
                <th class="sortable text-center" data-sort="signal">Signal <span class="sort-ind"></span></th>
                <th class="sortable text-center" data-sort="rsiRank">RSI Rank <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="target">Target ±6.28% (2π) <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="nextAvg">Next Avg ±3.14% (π) <span class="sort-ind"></span></th>
                <th class="sortable text-right" data-sort="hardSl">Hard SL ±2.0% (Split ±4.8%) <span class="sort-ind"></span></th>
                <th class="text-center">Quick Trade</th>
              </tr>
            </thead>
            <tbody id="screenerTbody">
              <tr><td colspan="14" class="empty-state">Fetching 2-second live data from Render server...</td></tr>
            </tbody>
          </table>
        </div>
      </div>
    </section>

    <section id="tab-signals" class="tab-panel hidden">
      <div class="signals-grid">
        <div class="card">
          <div class="signal-header long-hd">
            <div>
              <strong class="text-green">LONG Signals (RSI &lt; <span class="long-th-txt">35</span><span class="active-filter-tags-long"></span>)</strong>
              <div class="text-muted" style="font-size: 11px;">Sorted lowest RSI first • +6.28% TP / -3.14% Avg / -2.0% SL</div>
            </div>
            <span id="longSigCount" class="badge badge-long mono">0</span>
          </div>
          <div id="longSigEmpty" class="empty-state hidden">No coins currently match the LONG criteria.</div>
          <div id="longSigWrap" class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Coin</th>
                  <th class="text-right">RSI</th>
                  <th class="text-right">Mark Price</th>
                  <th class="text-right">Daily VWAP</th>
                  <th class="text-right">8h Fund</th>
                  <th class="text-right">TP (+6.28%)</th>
                  <th class="text-right">Avg (-3.14%)</th>
                  <th class="text-right">Hard SL (-2.0%)</th>
                  <th class="text-center">Action</th>
                </tr>
              </thead>
              <tbody id="longSigTbody"></tbody>
            </table>
          </div>
        </div>

        <div class="card">
          <div class="signal-header short-hd">
            <div>
              <strong class="text-red">SHORT Signals (RSI &gt; <span class="short-th-txt">70</span><span class="active-filter-tags-short"></span>)</strong>
              <div class="text-muted" style="font-size: 11px;">Sorted highest RSI first • -6.28% TP / +3.14% Avg / +2.0% SL</div>
            </div>
            <span id="shortSigCount" class="badge badge-short mono">0</span>
          </div>
          <div id="shortSigEmpty" class="empty-state hidden">No coins currently match the SHORT criteria.</div>
          <div id="shortSigWrap" class="table-wrap">
            <table>
              <thead>
                <tr>
                  <th>Coin</th>
                  <th class="text-right">RSI</th>
                  <th class="text-right">Mark Price</th>
                  <th class="text-right">Daily VWAP</th>
                  <th class="text-right">8h Fund</th>
                  <th class="text-right">TP (-6.28%)</th>
                  <th class="text-right">Avg (+3.14%)</th>
                  <th class="text-right">Hard SL (+2.0%)</th>
                  <th class="text-center">Action</th>
                </tr>
              </thead>
              <tbody id="shortSigTbody"></tbody>
            </table>
          </div>
        </div>
      </div>
    </section>

    <section id="tab-tradelog" class="tab-panel hidden">
      <div class="summary-grid">
        <div class="kpi-card">
          <div class="kpi-top"><span>Total Margin</span></div>
          <div id="sumMargin" class="kpi-val mono">$0.00</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-top"><span>Unrealized P&amp;L</span></div>
          <div id="sumPnl" class="kpi-val mono">$0.00</div>
        </div>
        <div class="kpi-card" style="flex-direction: row; align-items: center; justify-content: space-between;">
          <div>
            <div class="kpi-top"><span>Overall ROE %</span></div>
            <div id="sumRoe" class="kpi-val mono">0.00%</div>
          </div>
          <button id="exportCsvBtn" type="button" class="btn btn-long" style="padding: 7px 12px; font-size: 12px;">⬇ Export CSV</button>
        </div>
      </div>

      <form id="tradeForm" class="card form-card">
        <div class="form-header">
          <strong>Add Futures Trade (Isolated Leverage + Hard SL &amp; Breakeven Calculator)</strong>
          <div style="display: flex; align-items: center; gap: 10px;">
            <span id="posSizePreview" class="mono text-muted" style="font-size: 12px;">Pos Size: $500.00</span>
            <button id="autoFillBtn" type="button" class="btn" style="padding: 4px 9px; font-size: 11px;">Fill Live Price</button>
          </div>
        </div>

        <div class="form-grid">
          <div class="field">
            <label for="fCoin">Coin</label>
            <select id="fCoin" required></select>
          </div>
          <div class="field">
            <label for="fSide">Side</label>
            <select id="fSide" required>
              <option value="LONG">LONG</option>
              <option value="SHORT">SHORT</option>
            </select>
          </div>
          <div class="field">
            <label for="fMode">Entry Mode (SL)</label>
            <select id="fMode" required>
              <option value="single">Single Entry (-2.0% SL)</option>
              <option value="split">Split Avg (-4.8% SL)</option>
            </select>
          </div>
          <div class="field">
            <label for="fEntry">Entry Price (USDT)</label>
            <input id="fEntry" type="number" step="any" min="0" required placeholder="0.00" class="mono" />
          </div>
          <div class="field">
            <label for="fMargin">Margin (USDT)</label>
            <input id="fMargin" type="number" step="any" min="1" required value="100" class="mono" />
          </div>
          <div class="field">
            <label for="fLev">Leverage (1x–50x)</label>
            <input id="fLev" type="number" step="1" min="1" max="50" required value="5" class="mono" />
          </div>
          <div class="field">
            <label for="fRsi">Entry RSI</label>
            <input id="fRsi" type="number" step="0.01" min="0" max="100" placeholder="32.50" class="mono" />
          </div>
          <div class="field">
            <button type="submit" class="btn btn-primary" style="width: 100%; padding: 8px;">+ Add Trade</button>
          </div>
        </div>
      </form>

      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Coin</th>
                <th class="text-center">Side / Lev</th>
                <th class="text-right">Entry Price</th>
                <th class="text-right">Margin</th>
                <th class="text-right">Pos Size</th>
                <th class="text-right">Live Price</th>
                <th class="text-right">Unrealized P&amp;L</th>
                <th class="text-right">ROE %</th>
                <th class="text-right">Hard SL</th>
                <th class="text-right">Est. Liq Price</th>
                <th class="text-right">TP (±6.28%)</th>
                <th class="text-right">Avg (±3.14%)</th>
                <th class="text-center">Status</th>
                <th class="text-center">Del</th>
              </tr>
            </thead>
            <tbody id="tradeLogTbody"></tbody>
          </table>
        </div>
        <div id="tradeLogEmpty" class="empty-state hidden">No futures trades logged yet. Click <strong>+ Long</strong> or <strong>+ Short</strong> on any coin in the Screener.</div>
      </div>
    </section>

    <section id="tab-alerts" class="tab-panel hidden">
      <div class="card">
        <div class="signal-header">
          <div>
            <strong class="text-cyan">24/7 Server Telegram Alerts Log</strong>
            <div class="text-muted" style="font-size: 11px;">Ye alerts Render server ne tab bhi bheje hain jab aapka browser band tha</div>
          </div>
          <span id="alertsHistoryCount" class="badge badge-be mono">0</span>
        </div>
        <div id="alertsEmpty" class="empty-state">Abhi tak server start hone ke baad koi naya LONG/SHORT alert trigger nahi hua hai.</div>
        <div id="alertsWrap" class="table-wrap hidden">
          <table>
            <thead>
              <tr>
                <th>Time (IST)</th>
                <th>Pair</th>
                <th class="text-center">Signal</th>
                <th class="text-center">TF</th>
                <th class="text-right">Trigger Price</th>
                <th class="text-right">RSI (14)</th>
                <th class="text-right">Daily VWAP</th>
                <th class="text-right">8h Funding</th>
              </tr>
            </thead>
            <tbody id="alertsHistoryTbody"></tbody>
          </table>
        </div>
      </div>
    </section>
  </main>

  <div id="toastWrap" class="toast-wrap"></div>

  <script>
    (() => {
      'use strict';

      const STORAGE_TRADES = 'top10_futures_rsi_v3_trades';

      const state = {
        config: {
          timeframe: '15m',
          long_th: 35,
          short_th: 70,
          vwap_filter: true,
          funding_filter: true,
          cooldown_min: 15
        },
        coins: [],
        prevPrices: new Map(),
        prevSignals: new Map(),
        alertHistory: [],
        trades: [],
        activeTab: 'screener',
        sortKey: 'rsi',
        sortDir: 'asc',
        soundEnabled: false,
        audioCtx: null
      };

      function fmtPrice(v) {
        if (v === null || v === undefined || isNaN(v)) return '--';
        const a = Math.abs(v);
        let d = 2;
        if (a === 0) d = 2;
        else if (a < 0.01) d = 6;
        else if (a < 1) d = 4;
        else if (a < 25) d = 3;
        return v.toLocaleString('en-US', { minimumFractionDigits: d, maximumFractionDigits: d });
      }

      function fmtUSDT(v) {
        if (v === null || v === undefined || isNaN(v)) return '--';
        return (v < 0 ? '-$' : '$') + Math.abs(v).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      }

      function fmtPct(v) {
        if (v === null || v === undefined || isNaN(v)) return '--';
        return (v > 0 ? '+' : '') + v.toFixed(2) + '%';
      }

      function fmtFunding(v) {
        if (v === null || v === undefined || isNaN(v)) return '—';
        const pct = v * 100;
        return (pct > 0 ? '+' : '') + pct.toFixed(4) + '%';
      }

      function signalBadge(sig, rsi) {
        if (!sig || rsi === null) return { label: 'LOAD', cls: 'badge-neutral', bar: '#475569' };
        if (sig === 'LONG') return { label: 'LONG', cls: 'badge-long', bar: '#10b981' };
        if (sig === 'SHORT') return { label: 'SHORT', cls: 'badge-short', bar: '#f43f5e' };
        return { label: 'WAIT', cls: 'badge-wait', bar: '#06b6d4' };
      }

      function playChime(isShort = false) {
        if (!state.soundEnabled) return;
        try {
          if (!state.audioCtx) {
            const Ctx = window.AudioContext || window.webkitAudioContext;
            state.audioCtx = new Ctx();
          }
          if (state.audioCtx.state === 'suspended') state.audioCtx.resume();
          const ctx = state.audioCtx;
          const now = ctx.currentTime;
          const freqs = isShort ? [784, 523.25] : [587.33, 880];
          freqs.forEach((f, i) => {
            const osc = ctx.createOscillator();
            const g = ctx.createGain();
            osc.type = 'sine';
            osc.frequency.setValueAtTime(f, now + i * 0.11);
            g.gain.setValueAtTime(0.001, now + i * 0.11);
            g.gain.exponentialRampToValueAtTime(0.08, now + i * 0.11 + 0.02);
            g.gain.exponentialRampToValueAtTime(0.0001, now + i * 0.11 + 0.3);
            osc.connect(g);
            g.connect(ctx.destination);
            osc.start(now + i * 0.11);
            osc.stop(now + i * 0.11 + 0.32);
          });
        } catch {}
      }

      function showToast(msg, type = 'info') {
        const wrap = document.getElementById('toastWrap');
        const el = document.createElement('div');
        el.className = `toast ${type}`;
        el.textContent = msg;
        wrap.appendChild(el);
        setTimeout(() => el.remove(), 3400);
      }

      async function fetchServerState() {
        try {
          const res = await fetch('/api/state', { cache: 'no-store' });
          if (!res.ok) throw new Error('HTTP ' + res.status);
          const data = await res.json();

          document.getElementById('liveDot').className = 'status-dot live pulse-dot';
          document.getElementById('liveFeedName').textContent = data.active_feed || 'LIVE • 2s Scan';
          document.getElementById('liveScanCount').textContent = (data.scans_completed || 0).toLocaleString();
          document.getElementById('liveTimestamp').textContent = data.last_scan || '--:--:-- IST';
          document.getElementById('kpiSyncTime').textContent = data.last_scan || '--:--:-- IST';
          document.getElementById('kpiFeedDetail').textContent = data.active_feed || 'Connected';

          const tgBtn = document.getElementById('tgTestBtn');
          if (data.tg_configured) {
            tgBtn.classList.add('active');
            document.getElementById('tgStatusLabel').textContent = `24/7 TG Bot: ON (${data.alerts_sent || 0} sent)`;
          } else {
            tgBtn.classList.remove('active');
            document.getElementById('tgStatusLabel').textContent = '24/7 TG Bot: Set Env Vars';
          }

          if (data.config) {
            state.config = data.config;
            syncControlsFromConfig();
          }

          if (Array.isArray(data.coins)) {
            data.coins.forEach(c => {
              const prevSig = state.prevSignals.get(c.coin);
              if (prevSig && prevSig !== c.signal) {
                if (c.signal === 'LONG') {
                  playChime(false);
                  showToast(`LONG Signal: ${c.pair} RSI ${c.rsi.toFixed(2)}`, 'long');
                } else if (c.signal === 'SHORT') {
                  playChime(true);
                  showToast(`SHORT Signal: ${c.pair} RSI ${c.rsi.toFixed(2)}`, 'short');
                }
              }
              state.prevSignals.set(c.coin, c.signal);
            });
            state.coins = data.coins;
          }

          if (Array.isArray(data.alert_history)) {
            state.alertHistory = data.alert_history;
          }

          populateCoinSelectIfNeeded();
          renderAll();
        } catch (err) {
          document.getElementById('liveDot').className = 'status-dot error';
          document.getElementById('liveFeedName').textContent = 'RECONNECTING...';
        }
      }

      async function pushConfigUpdate(patch, forceHistory = false) {
        try {
          const res = await fetch('/api/config', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ ...patch, rescan: true, force_history: forceHistory })
          });
          if (res.ok) {
            const data = await res.json();
            if (data.config) {
              state.config = data.config;
              syncControlsFromConfig();
            }
            setTimeout(fetchServerState, 800);
          }
        } catch {
          showToast('Server config update failed.', 'error');
        }
      }

      function syncControlsFromConfig() {
        const cfg = state.config;
        document.getElementById('headerTfBadge').textContent = cfg.timeframe.toUpperCase();
        document.getElementById('kpiTfTag').textContent = cfg.timeframe;
        document.querySelectorAll('.btn-tf').forEach(b => b.classList.toggle('active', b.dataset.tf === cfg.timeframe));

        const longIn = document.getElementById('longThInput');
        if (document.activeElement !== longIn) longIn.value = cfg.long_th;
        const shortIn = document.getElementById('shortThInput');
        if (document.activeElement !== shortIn) shortIn.value = cfg.short_th;

        document.querySelectorAll('.long-th-txt').forEach(el => el.textContent = cfg.long_th);
        document.querySelectorAll('.short-th-txt').forEach(el => el.textContent = cfg.short_th);

        const vBtn = document.getElementById('vwapFilterToggleBtn');
        vBtn.classList.toggle('active', cfg.vwap_filter);
        document.getElementById('vwapFilterLabel').textContent = cfg.vwap_filter ? 'VWAP Filter: ON' : 'VWAP Filter: OFF';

        const fBtn = document.getElementById('fundingFilterToggleBtn');
        fBtn.classList.toggle('active', cfg.funding_filter);
        document.getElementById('fundingFilterLabel').textContent = cfg.funding_filter ? 'Funding Filter: ON' : 'Funding Filter: OFF';

        const longParts = [];
        if (cfg.vwap_filter) longParts.push('Above VWAP');
        if (cfg.funding_filter) longParts.push('Fund ≤ +0.01%');

        const shortParts = [];
        if (cfg.vwap_filter) shortParts.push('Below VWAP');
        if (cfg.funding_filter) shortParts.push('Fund ≥ 0%');

        document.getElementById('kpiLongSub').textContent = longParts.length
          ? `Oversold + ${longParts.join(' + ')}`
          : 'Oversold dip setups';
        document.getElementById('kpiShortSub').textContent = shortParts.length
          ? `Overbought + ${shortParts.join(' + ')}`
          : 'Overbought reversal setups';

        document.querySelectorAll('.active-filter-tags-long').forEach(el => {
          el.textContent = longParts.length ? ' + ' + longParts.join(' + ') : '';
        });
        document.querySelectorAll('.active-filter-tags-short').forEach(el => {
          el.textContent = shortParts.length ? ' + ' + shortParts.join(' + ') : '';
        });
      }

      function populateCoinSelectIfNeeded() {
        const sel = document.getElementById('fCoin');
        if (sel.options.length === 0 && state.coins.length > 0) {
          sel.innerHTML = state.coins.map(c =>
            `<option value="${c.coin}">${c.pair} — ${c.name}</option>`
          ).join('');
        }
      }

      function renderKPIs() {
        const loaded = state.coins.filter(c => c.rsi !== null && c.rsi !== undefined);
        const longs = loaded.filter(c => c.signal === 'LONG');
        const shorts = loaded.filter(c => c.signal === 'SHORT');

        document.getElementById('kpiLongCount').textContent = longs.length;
        document.getElementById('kpiShortCount').textContent = shorts.length;
        document.getElementById('tabSignalsBadge').textContent = longs.length + shorts.length;
        document.getElementById('tabTradesBadge').textContent = state.trades.length;
        document.getElementById('tabAlertsBadge').textContent = state.alertHistory.length;

        if (loaded.length > 0) {
          const sorted = loaded.slice().sort((a, b) => a.rsi - b.rsi);
          const low = sorted[0];
          const high = sorted[sorted.length - 1];

          document.getElementById('kpiOversoldCoin').textContent = low.pair;
          document.getElementById('kpiOversoldRsi').textContent = low.rsi.toFixed(2);
          document.getElementById('kpiOversoldPrice').textContent = `Mark: $${fmtPrice(low.price)}`;

          document.getElementById('kpiOverboughtCoin').textContent = high.pair;
          document.getElementById('kpiOverboughtRsi').textContent = high.rsi.toFixed(2);
          document.getElementById('kpiOverboughtPrice').textContent = `Mark: $${fmtPrice(high.price)}`;
        }
      }

      function rsiGaugeHtml(rsi, sigObj) {
        if (rsi === null || rsi === undefined || isNaN(rsi)) return '<span class="mono text-dim">--</span>';
        const w = Math.max(0, Math.min(100, rsi));
        const clr = rsi < state.config.long_th ? 'text-green' : rsi > state.config.short_th ? 'text-red' : '';
        return `
          <div class="rsi-cell">
            <span class="mono ${clr}" style="font-weight:700; width:44px;">${rsi.toFixed(2)}</span>
            <div class="rsi-bar-bg"><div class="rsi-bar-fill" style="width:${w}%; background:${sigObj.bar};"></div></div>
          </div>
        `;
      }

      function vwapCellHtml(c) {
        if (c.vwap === null || c.vwap === undefined || isNaN(c.vwap)) return '<span class="mono text-dim">--</span>';
        const isAbove = c.vwapDiffPct >= 0;
        const diffCls = isAbove ? 'text-green' : 'text-red';
        const tag = isAbove ? 'ABOVE' : 'BELOW';
        return `
          <div class="mono text-cyan" style="font-weight:600;">${fmtPrice(c.vwap)}</div>
          <div class="mono ${diffCls}" style="font-size:10px; font-weight:700;">${tag} ${fmtPct(c.vwapDiffPct)}</div>
        `;
      }

      function renderScreener() {
        const tbody = document.getElementById('screenerTbody');
        if (state.coins.length === 0) return;

        const list = state.coins.slice();
        const dir = state.sortDir === 'asc' ? 1 : -1;

        list.sort((a, b) => {
          let va, vb;
          switch (state.sortKey) {
            case 'sr':          va = a.sr; vb = b.sr; break;
            case 'coin':        va = a.coin; vb = b.coin; break;
            case 'price':       va = a.price; vb = b.price; break;
            case 'vwapDiffPct': va = a.vwapDiffPct; vb = b.vwapDiffPct; break;
            case 'prevClose':   va = a.prevClose; vb = b.prevClose; break;
            case 'changePct':   va = a.changePct; vb = b.changePct; break;
            case 'funding':     va = a.funding; vb = b.funding; break;
            case 'rsi':         va = a.rsi; vb = b.rsi; break;
            case 'rsiRank':     va = a.rsiRank; vb = b.rsiRank; break;
            case 'target':      va = a.price ? a.price * 1.0628 : null; vb = b.price ? b.price * 1.0628 : null; break;
            case 'nextAvg':     va = a.price ? a.price * 0.9686 : null; vb = b.price ? b.price * 0.9686 : null; break;
            case 'hardSl':      va = a.price ? a.price * 0.98 : null; vb = b.price ? b.price * 0.98 : null; break;
            case 'signal':      va = a.signal; vb = b.signal; break;
            default:            va = a.rsi; vb = b.rsi;
          }
          if (va === null || va === undefined) return 1;
          if (vb === null || vb === undefined) return -1;
          if (typeof va === 'string') return va.localeCompare(vb) * dir;
          return (va - vb) * dir;
        });

        tbody.innerHTML = list.map(c => {
          const sigObj = signalBadge(c.signal, c.rsi);
          const isShortBias = c.signal === 'SHORT' || (c.rsi !== null && c.rsi > state.config.short_th);
          const tp = c.price !== null ? (isShortBias ? c.price * 0.9372 : c.price * 1.0628) : null;
          const avg = c.price !== null ? (isShortBias ? c.price * 1.0314 : c.price * 0.9686) : null;
          const hardSl = c.price !== null ? (isShortBias ? c.price * 1.02 : c.price * 0.98) : null;
          const splitSl = c.price !== null ? (isShortBias ? c.price * 1.048 : c.price * 0.952) : null;

          const prevP = state.prevPrices.get(c.coin);
          let flashCls = '';
          if (prevP !== undefined && c.price !== prevP) {
            flashCls = c.price > prevP ? 'tick-up' : 'tick-down';
          }
          state.prevPrices.set(c.coin, c.price);

          const chgCls = c.changePct > 0 ? 'text-green' : c.changePct < 0 ? 'text-red' : 'text-muted';
          const fundCls = c.funding > 0.0001 ? 'text-red' : c.funding > 0 ? 'text-amber' : c.funding < 0 ? 'text-cyan' : 'text-dim';
          const rowCls = c.signal === 'LONG' ? 'row-long' : c.signal === 'SHORT' ? 'row-short' : '';

          return `
            <tr class="${rowCls}">
              <td class="mono text-dim">#${c.sr}</td>
              <td>
                <strong class="mono">${c.pair}</strong>
                <div class="text-muted" style="font-size:11px;">${c.name}</div>
              </td>
              <td class="text-right mono ${flashCls}" style="font-weight:700;">${fmtPrice(c.price)}</td>
              <td class="text-right">${vwapCellHtml(c)}</td>
              <td class="text-right mono text-muted">${fmtPrice(c.prevClose)}</td>
              <td class="text-right mono ${chgCls}" style="font-weight:600;">${fmtPct(c.changePct)}</td>
              <td class="text-right mono ${fundCls}" style="font-size:12px;">${fmtFunding(c.funding)}</td>
              <td>${rsiGaugeHtml(c.rsi, sigObj)}</td>
              <td class="text-center"><span class="badge ${sigObj.cls}">${sigObj.label}</span></td>
              <td class="text-center mono ${c.rsiRank && c.rsiRank <= 2 ? 'text-green' : 'text-muted'}">${c.rsiRank ? '#' + c.rsiRank : '--'}</td>
              <td class="text-right mono text-green">${fmtPrice(tp)}</td>
              <td class="text-right mono text-amber">${fmtPrice(avg)}</td>
              <td class="text-right">
                <div class="mono text-red" style="font-weight:700;">${fmtPrice(hardSl)}</div>
                <div class="mono text-dim" style="font-size:10px;">Split: ${fmtPrice(splitSl)}</div>
              </td>
              <td class="text-center">
                <div class="action-btns">
                  <button type="button" class="btn btn-long" data-quick-coin="${c.coin}" data-quick-side="LONG">+ Long</button>
                  <button type="button" class="btn btn-short" data-quick-coin="${c.coin}" data-quick-side="SHORT">+ Short</button>
                </div>
              </td>
            </tr>
          `;
        }).join('');
      }

      function renderSignals() {
        const longs = state.coins.filter(c => c.signal === 'LONG').sort((a, b) => a.rsi - b.rsi);
        const shorts = state.coins.filter(c => c.signal === 'SHORT').sort((a, b) => b.rsi - a.rsi);

        document.getElementById('longSigCount').textContent = longs.length;
        document.getElementById('shortSigCount').textContent = shorts.length;

        const longEmpty = document.getElementById('longSigEmpty');
        const longWrap = document.getElementById('longSigWrap');
        if (longs.length === 0) {
          longEmpty.classList.remove('hidden');
          longWrap.classList.add('hidden');
        } else {
          longEmpty.classList.add('hidden');
          longWrap.classList.remove('hidden');
          document.getElementById('longSigTbody').innerHTML = longs.map(c => `
            <tr class="row-long">
              <td><strong class="mono">${c.pair}</strong></td>
              <td class="text-right mono text-green" style="font-weight:700;">${c.rsi.toFixed(2)}</td>
              <td class="text-right mono">${fmtPrice(c.price)}</td>
              <td class="text-right">${vwapCellHtml(c)}</td>
              <td class="text-right mono text-cyan">${fmtFunding(c.funding)}</td>
              <td class="text-right mono text-green">${fmtPrice(c.price * 1.0628)}</td>
              <td class="text-right mono text-amber">${fmtPrice(c.price * 0.9686)}</td>
              <td class="text-right">
                <div class="mono text-red" style="font-weight:700;">${fmtPrice(c.price * 0.98)}</div>
                <div class="mono text-dim" style="font-size:10px;">Split: ${fmtPrice(c.price * 0.952)}</div>
              </td>
              <td class="text-center"><button type="button" class="btn btn-long" data-quick-coin="${c.coin}" data-quick-side="LONG">+ Long</button></td>
            </tr>
          `).join('');
        }

        const shortEmpty = document.getElementById('shortSigEmpty');
        const shortWrap = document.getElementById('shortSigWrap');
        if (shorts.length === 0) {
          shortEmpty.classList.remove('hidden');
          shortWrap.classList.add('hidden');
        } else {
          shortEmpty.classList.add('hidden');
          shortWrap.classList.remove('hidden');
          document.getElementById('shortSigTbody').innerHTML = shorts.map(c => `
            <tr class="row-short">
              <td><strong class="mono">${c.pair}</strong></td>
              <td class="text-right mono text-red" style="font-weight:700;">${c.rsi.toFixed(2)}</td>
              <td class="text-right mono">${fmtPrice(c.price)}</td>
              <td class="text-right">${vwapCellHtml(c)}</td>
              <td class="text-right mono text-amber">${fmtFunding(c.funding)}</td>
              <td class="text-right mono text-green">${fmtPrice(c.price * 0.9372)}</td>
              <td class="text-right mono text-amber">${fmtPrice(c.price * 1.0314)}</td>
              <td class="text-right">
                <div class="mono text-red" style="font-weight:700;">${fmtPrice(c.price * 1.02)}</div>
                <div class="mono text-dim" style="font-size:10px;">Split: ${fmtPrice(c.price * 1.048)}</div>
              </td>
              <td class="text-center"><button type="button" class="btn btn-short" data-quick-coin="${c.coin}" data-quick-side="SHORT">+ Short</button></td>
            </tr>
          `).join('');
        }
      }

      function renderTradeLog() {
        const tbody = document.getElementById('tradeLogTbody');
        const emptyEl = document.getElementById('tradeLogEmpty');

        let totalMargin = 0;
        let totalPnl = 0;
        const coinMap = new Map(state.coins.map(c => [c.coin, c]));

        if (state.trades.length === 0) {
          tbody.innerHTML = '';
          emptyEl.classList.remove('hidden');
        } else {
          emptyEl.classList.add('hidden');
          tbody.innerHTML = state.trades.map(t => {
            const c = coinMap.get(t.coin);
            const live = c && c.price !== null ? c.price : t.entry;
            const posSize = t.margin * t.lev;
            const qty = t.entry > 0 ? posSize / t.entry : 0;
            const isLong = t.side === 'LONG';
            const isSplit = t.mode === 'split';

            const pnl = isLong ? (live - t.entry) * qty : (t.entry - live) * qty;
            const roe = t.margin > 0 ? (pnl / t.margin) * 100 : 0;

            const liq = isLong
              ? Math.max(0, t.entry * (1 - 0.95 / t.lev))
              : t.entry * (1 + 0.95 / t.lev);

            const slPct = isSplit ? 0.048 : 0.02;
            const hardSl = isLong ? t.entry * (1 - slPct) : t.entry * (1 + slPct);
            const beTrigger = isLong ? t.entry * 1.0314 : t.entry * 0.9686;

            const tp = isLong ? t.entry * 1.0628 : t.entry * 0.9372;
            const avg = isLong ? t.entry * 0.9686 : t.entry * 1.0314;

            let status = 'HOLD';
            let stCls = 'badge-neutral';
            if ((isLong && live >= tp) || (!isLong && live <= tp)) {
              status = 'BOOK PROFIT';
              stCls = 'badge-long';
            } else if ((isLong && live <= liq) || (!isLong && live >= liq)) {
              status = 'LIQ RISK';
              stCls = 'badge-short';
            } else if ((isLong && live <= hardSl) || (!isLong && live >= hardSl)) {
              status = 'SL HIT';
              stCls = 'badge-short';
            } else if ((isLong && live >= beTrigger) || (!isLong && live <= beTrigger)) {
              status = 'MOVE SL → BE';
              stCls = 'badge-be';
            } else if (isSplit && ((isLong && live <= avg) || (!isLong && live >= avg))) {
              status = 'AVG ZONE';
              stCls = 'badge-wait';
            }

            totalMargin += t.margin;
            totalPnl += pnl;
            const pnlCls = pnl > 0 ? 'text-green' : pnl < 0 ? 'text-red' : 'text-muted';

            return `
              <tr>
                <td>
                  <strong class="mono">${t.coin}USDT</strong>
                  <div class="text-dim" style="font-size:10px;">${isSplit ? 'Split (-4.8% SL)' : 'Single (-2.0% SL)'}</div>
                </td>
                <td class="text-center"><span class="badge ${isLong ? 'badge-long' : 'badge-short'} mono">${t.side} ${t.lev}x</span></td>
                <td class="text-right mono">${fmtPrice(t.entry)}</td>
                <td class="text-right mono">${fmtUSDT(t.margin)}</td>
                <td class="text-right mono text-muted">${fmtUSDT(posSize)}</td>
                <td class="text-right mono text-cyan" style="font-weight:700;">${fmtPrice(live)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${fmtUSDT(pnl)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${fmtPct(roe)}</td>
                <td class="text-right mono text-red" style="font-weight:700;">${fmtPrice(hardSl)}</td>
                <td class="text-right mono text-dim">${fmtPrice(liq)}</td>
                <td class="text-right mono text-green">${fmtPrice(tp)}</td>
                <td class="text-right mono text-amber">${fmtPrice(avg)}</td>
                <td class="text-center"><span class="badge ${stCls}">${status}</span></td>
                <td class="text-center"><button type="button" class="btn btn-del" data-del-trade="${t.id}">✕</button></td>
              </tr>
            `;
          }).join('');
        }

        const totalRoe = totalMargin > 0 ? (totalPnl / totalMargin) * 100 : 0;
        document.getElementById('sumMargin').textContent = fmtUSDT(totalMargin);
        const pnlEl = document.getElementById('sumPnl');
        const roeEl = document.getElementById('sumRoe');
        pnlEl.textContent = fmtUSDT(totalPnl);
        roeEl.textContent = fmtPct(totalRoe);
        pnlEl.className = `kpi-val mono ${totalPnl > 0 ? 'text-green' : totalPnl < 0 ? 'text-red' : ''}`;
        roeEl.className = `kpi-val mono ${totalRoe > 0 ? 'text-green' : totalRoe < 0 ? 'text-red' : ''}`;
      }

      function renderAlertHistory() {
        document.getElementById('alertsHistoryCount').textContent = state.alertHistory.length;
        const emptyEl = document.getElementById('alertsEmpty');
        const wrapEl = document.getElementById('alertsWrap');
        if (state.alertHistory.length === 0) {
          emptyEl.classList.remove('hidden');
          wrapEl.classList.add('hidden');
          return;
        }
        emptyEl.classList.add('hidden');
        wrapEl.classList.remove('hidden');
        document.getElementById('alertsHistoryTbody').innerHTML = state.alertHistory.map(a => {
          const isLong = a.side === 'LONG';
          return `
            <tr class="${isLong ? 'row-long' : 'row-short'}">
              <td class="mono text-muted">${a.time}</td>
              <td><strong class="mono">${a.pair}</strong></td>
              <td class="text-center"><span class="badge ${isLong ? 'badge-long' : 'badge-short'}">${a.side}</span></td>
              <td class="text-center mono text-cyan">${a.tf}</td>
              <td class="text-right mono" style="font-weight:700;">$${fmtPrice(a.price)}</td>
              <td class="text-right mono ${isLong ? 'text-green' : 'text-red'}" style="font-weight:700;">${Number(a.rsi).toFixed(2)}</td>
              <td class="text-right mono text-cyan">$${fmtPrice(a.vwap)}</td>
              <td class="text-right mono">${fmtFunding(a.funding)}</td>
            </tr>
          `;
        }).join('');
      }

      function renderAll() {
        renderKPIs();
        if (state.activeTab === 'screener') renderScreener();
        else if (state.activeTab === 'signals') renderSignals();
        else if (state.activeTab === 'tradelog') renderTradeLog();
        else if (state.activeTab === 'alerts') renderAlertHistory();
      }

      function loadTrades() {
        try {
          const t = JSON.parse(localStorage.getItem(STORAGE_TRADES) || '[]');
          if (Array.isArray(t)) state.trades = t;
        } catch {}
      }

      function saveTrades() {
        try { localStorage.setItem(STORAGE_TRADES, JSON.stringify(state.trades)); } catch {}
      }

      function exportCSV() {
        if (state.trades.length === 0) { showToast('No trades to export yet.'); return; }
        const coinMap = new Map(state.coins.map(c => [c.coin, c]));
        const headers = ['Pair', 'Side', 'Mode', 'Leverage', 'Entry Price', 'Margin', 'Pos Size', 'Live Price', 'Hard SL', 'Unrealized PnL', 'ROE %', 'Entry RSI'];
        const rows = state.trades.map(t => {
          const c = coinMap.get(t.coin);
          const live = c && c.price !== null ? c.price : t.entry;
          const pos = t.margin * t.lev;
          const qty = pos / t.entry;
          const isLong = t.side === 'LONG';
          const slPct = t.mode === 'split' ? 0.048 : 0.02;
          const hardSl = isLong ? t.entry * (1 - slPct) : t.entry * (1 + slPct);
          const pnl = isLong ? (live - t.entry) * qty : (t.entry - live) * qty;
          const roe = (pnl / t.margin) * 100;
          return [t.coin + 'USDT', t.side, t.mode || 'single', t.lev + 'x', t.entry, t.margin, pos.toFixed(2), live, hardSl.toFixed(4), pnl.toFixed(2), roe.toFixed(2) + '%', t.rsi ?? ''].join(',');
        });
        const blob = new Blob([[headers.join(','), ...rows].join('
')], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `top10-futures-log-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        showToast('Futures trade log exported as CSV.', 'long');
      }

      function updatePosPreview() {
        const m = parseFloat(document.getElementById('fMargin').value) || 0;
        const l = parseFloat(document.getElementById('fLev').value) || 1;
        document.getElementById('posSizePreview').textContent = `Pos Size: ${fmtUSDT(m * l)}`;
      }

      function switchTab(tab) {
        state.activeTab = tab;
        document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
        document.getElementById(`tab-${tab}`).classList.remove('hidden');
        document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
        renderAll();
      }

      function updateSortIndicators() {
        document.querySelectorAll('th[data-sort]').forEach(th => {
          const ind = th.querySelector('.sort-ind');
          if (!ind) return;
          if (th.dataset.sort === state.sortKey) {
            ind.textContent = state.sortDir === 'asc' ? ' ▲' : ' ▼';
            ind.style.color = '#67e8f9';
          } else {
            ind.textContent = '';
          }
        });
      }

      function prefillForm(coinKey, side) {
        const c = state.coins.find(x => x.coin === coinKey);
        document.getElementById('fCoin').value = coinKey;
        document.getElementById('fSide').value = side;
        if (c && c.price !== null) document.getElementById('fEntry').value = c.price;
        if (c && c.rsi !== null) document.getElementById('fRsi').value = c.rsi.toFixed(2);
        updatePosPreview();
        switchTab('tradelog');
        document.getElementById('fMargin').focus();
        showToast(`Pre-filled ${side} ${coinKey}USDT @ $${fmtPrice(c?.price)}`, side === 'LONG' ? 'long' : 'short');
      }

      function bindEvents() {
        document.querySelectorAll('.tab-btn').forEach(b => b.addEventListener('click', () => switchTab(b.dataset.tab)));
        document.getElementById('kpiLongCard').addEventListener('click', () => switchTab('signals'));
        document.getElementById('kpiShortCard').addEventListener('click', () => switchTab('signals'));

        document.getElementById('rescanBtn').addEventListener('click', () => {
          showToast('Syncing candle history & funding...');
          pushConfigUpdate({}, true);
        });

        document.querySelectorAll('.btn-tf').forEach(btn => {
          btn.addEventListener('click', () => {
            const tf = btn.dataset.tf;
            if (tf === state.timeframe) return;
            showToast(`Switching 2s scanner to ${tf.toUpperCase()}...`, 'long');
            pushConfigUpdate({ timeframe: tf }, true);
          });
        });

        let thTimer = null;
        document.getElementById('longThInput').addEventListener('input', e => {
          const v = parseFloat(e.target.value);
          if (!isNaN(v) && v >= 10 && v <= 55) {
            clearTimeout(thTimer);
            thTimer = setTimeout(() => {
              pushConfigUpdate({ long_th: v }, false);
              showToast(`Updated Long RSI < ${v} on 2s scanner`, 'long');
            }, 500);
          }
        });

        document.getElementById('shortThInput').addEventListener('input', e => {
          const v = parseFloat(e.target.value);
          if (!isNaN(v) && v >= 45 && v <= 90) {
            clearTimeout(thTimer);
            thTimer = setTimeout(() => {
              pushConfigUpdate({ short_th: v }, false);
              showToast(`Updated Short RSI > ${v} on 2s scanner`, 'short');
            }, 500);
          }
        });

        document.getElementById('vwapFilterToggleBtn').addEventListener('click', () => {
          const next = !state.config.vwap_filter;
          pushConfigUpdate({ vwap_filter: next }, false);
          showToast(`2s Scanner VWAP Filter: ${next ? 'ON' : 'OFF'}`, next ? 'long' : 'info');
        });

        document.getElementById('fundingFilterToggleBtn').addEventListener('click', () => {
          const next = !state.config.funding_filter;
          pushConfigUpdate({ funding_filter: next }, false);
          showToast(`2s Scanner Funding Filter: ${next ? 'ON' : 'OFF'}`, next ? 'long' : 'info');
        });

        document.getElementById('soundToggleBtn').addEventListener('click', () => {
          state.soundEnabled = !state.soundEnabled;
          const btn = document.getElementById('soundToggleBtn');
          btn.classList.toggle('active', state.soundEnabled);
          document.getElementById('soundIcon').textContent = state.soundEnabled ? '🔊' : '🔇';
          document.getElementById('soundLabel').textContent = state.soundEnabled ? 'Sound: ON' : 'Sound: OFF';
          if (state.soundEnabled) {
            playChime(false);
            showToast('Browser sound alerts ON', 'long');
          }
        });

        document.getElementById('tgTestBtn').addEventListener('click', async () => {
          try {
            const res = await fetch('/api/test-tg', { method: 'POST' });
            const data = await res.json();
            if (data.ok) {
              showToast('Test alert Render se Telegram par bhej diya gaya!', 'long');
            } else {
              showToast(`Telegram Error: ${data.error}`, 'error');
            }
          } catch {
            showToast('Request failed.', 'error');
          }
        });

        document.querySelectorAll('th[data-sort]').forEach(th => {
          th.addEventListener('click', () => {
            const key = th.dataset.sort;
            if (state.sortKey === key) state.sortDir = state.sortDir === 'asc' ? 'desc' : 'asc';
            else { state.sortKey = key; state.sortDir = (key === 'price' || key === 'changePct' || key === 'vwapDiffPct') ? 'desc' : 'asc'; }
            updateSortIndicators();
            renderScreener();
          });
        });

        document.addEventListener('click', e => {
          const qBtn = e.target.closest('[data-quick-coin]');
          if (qBtn) {
            prefillForm(qBtn.dataset.quickCoin, qBtn.dataset.quickSide);
            return;
          }
          const dBtn = e.target.closest('[data-del-trade]');
          if (dBtn) {
            state.trades = state.trades.filter(t => t.id !== dBtn.dataset.delTrade);
            saveTrades();
            renderKPIs();
            renderTradeLog();
            showToast('Trade removed.');
          }
        });

        document.getElementById('fMargin').addEventListener('input', updatePosPreview);
        document.getElementById('fLev').addEventListener('input', updatePosPreview);
        document.getElementById('autoFillBtn').addEventListener('click', () => {
          const ck = document.getElementById('fCoin').value;
          const c = state.coins.find(x => x.coin === ck);
          if (c && c.price !== null) {
            document.getElementById('fEntry').value = c.price;
            if (c.rsi !== null) document.getElementById('fRsi').value = c.rsi.toFixed(2);
          }
        });

        document.getElementById('tradeForm').addEventListener('submit', e => {
          e.preventDefault();
          const coin = document.getElementById('fCoin').value;
          const side = document.getElementById('fSide').value;
          const mode = document.getElementById('fMode').value;
          const entry = parseFloat(document.getElementById('fEntry').value);
          const margin = parseFloat(document.getElementById('fMargin').value);
          const lev = Math.min(50, Math.max(1, parseInt(document.getElementById('fLev').value, 10) || 1));
          const rsiVal = document.getElementById('fRsi').value;

          if (!coin || isNaN(entry) || entry <= 0 || isNaN(margin) || margin <= 0) {
            showToast('Enter a valid Entry Price and Margin.', 'error');
            return;
          }

          state.trades.push({
            id: 'ft_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
            coin, side, mode, entry, margin, lev,
            rsi: rsiVal !== '' ? parseFloat(rsiVal) : null
          });
          saveTrades();
          renderKPIs();
          renderTradeLog();
          showToast(`Logged ${side} ${coin}USDT (${lev}x • ${fmtUSDT(margin)})`, side === 'LONG' ? 'long' : 'short');
        });

        document.getElementById('exportCsvBtn').addEventListener('click', exportCSV);
      }

      function init() {
        loadTrades();
        updatePosPreview();
        updateSortIndicators();
        bindEvents();
        fetchServerState();
        setInterval(fetchServerState, 1500);
      }

      // =========================================================================
      // SCI-FI LIVE HUD BACKGROUND SIMULATION (Radar + Neural Nodes + Telemetry)
      // =========================================================================
      (function initSciFiSimulation() {
        const canvas = document.getElementById('sciFiBgCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let w = 0, h = 0, dpr = Math.min(window.devicePixelRatio || 1, 2);

        const nodes = [];
        const pulses = [];
        const NODE_COUNT = 38;
        let angle = 0;

        function resize() {
          w = window.innerWidth;
          h = window.innerHeight;
          canvas.width = w * dpr;
          canvas.height = h * dpr;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function seedNodes() {
          nodes.length = 0;
          for (let i = 0; i < NODE_COUNT; i++) {
            nodes.push({
              x: Math.random() * (w || 1400),
              y: Math.random() * (h || 900),
              vx: (Math.random() - 0.5) * 0.35,
              vy: (Math.random() - 0.5) * 0.35,
              r: 1.3 + Math.random() * 1.8,
              cyan: Math.random() > 0.35
            });
          }
        }

        function spawnPulse() {
          if (nodes.length < 2) return;
          const a = nodes[Math.floor(Math.random() * nodes.length)];
          const b = nodes[Math.floor(Math.random() * nodes.length)];
          if (a !== b && Math.hypot(a.x - b.x, a.y - b.y) < 230) {
            pulses.push({ ax: a.x, ay: a.y, bx: b.x, by: b.y, t: 0, speed: 0.018 + Math.random() * 0.02 });
          }
        }

        resize();
        seedNodes();
        window.addEventListener('resize', resize);

        function drawFrame() {
          ctx.clearRect(0, 0, w, h);

          const grid = 64;
          ctx.strokeStyle = 'rgba(6, 182, 212, 0.045)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let x = 0; x < w; x += grid) {
            ctx.moveTo(x, 0);
            ctx.lineTo(x, h);
          }
          for (let y = 0; y < h; y += grid) {
            ctx.moveTo(0, y);
            ctx.lineTo(w, y);
          }
          ctx.stroke();

          const cx = w * 0.82;
          const cy = h * 0.28;
          const baseR = Math.min(w, h) * 0.24;
          angle += 0.0045;

          [baseR * 0.55, baseR * 0.85, baseR * 1.15].forEach((rad, idx) => {
            ctx.save();
            ctx.translate(cx, cy);
            ctx.rotate(angle * (idx % 2 === 0 ? 1 : -0.7));
            ctx.strokeStyle = idx === 1 ? 'rgba(16, 185, 129, 0.11)' : 'rgba(6, 182, 212, 0.09)';
            ctx.lineWidth = 1.1;
            ctx.setLineDash(idx === 0 ? [8, 10] : idx === 1 ? [22, 14] : [4, 12]);
            ctx.beginPath();
            ctx.arc(0, 0, rad, 0, Math.PI * 2);
            ctx.stroke();
            ctx.restore();
          });

          ctx.save();
          ctx.translate(cx, cy);
          ctx.rotate(angle * 2.2);
          const grad = ctx.createRadialGradient(0, 0, 5, 0, 0, baseR * 1.15);
          grad.addColorStop(0, 'rgba(6, 182, 212, 0.14)');
          grad.addColorStop(1, 'rgba(6, 182, 212, 0)');
          ctx.fillStyle = grad;
          ctx.beginPath();
          ctx.moveTo(0, 0);
          ctx.arc(0, 0, baseR * 1.15, 0, 0.42);
          ctx.closePath();
          ctx.fill();
          ctx.restore();

          for (let i = 0; i < nodes.length; i++) {
            const n = nodes[i];
            n.x += n.vx;
            n.y += n.vy;
            if (n.x < 0 || n.x > w) n.vx *= -1;
            if (n.y < 0 || n.y > h) n.vy *= -1;

            for (let j = i + 1; j < nodes.length; j++) {
              const m = nodes[j];
              const dist = Math.hypot(n.x - m.x, n.y - m.y);
              if (dist < 175) {
                const alpha = (1 - dist / 175) * 0.16;
                ctx.strokeStyle = `rgba(6, 182, 212, ${alpha.toFixed(3)})`;
                ctx.lineWidth = 0.8;
                ctx.beginPath();
                ctx.moveTo(n.x, n.y);
                ctx.lineTo(m.x, m.y);
                ctx.stroke();
              }
            }

            ctx.fillStyle = n.cyan ? 'rgba(103, 232, 249, 0.45)' : 'rgba(16, 185, 129, 0.45)';
            ctx.beginPath();
            ctx.arc(n.x, n.y, n.r, 0, Math.PI * 2);
            ctx.fill();
          }

          if (Math.random() < 0.08) spawnPulse();
          for (let i = pulses.length - 1; i >= 0; i--) {
            const p = pulses[i];
            p.t += p.speed;
            if (p.t >= 1) {
              pulses.splice(i, 1);
              continue;
            }
            const px = p.ax + (p.bx - p.ax) * p.t;
            const py = p.ay + (p.by - p.ay) * p.t;
            ctx.fillStyle = 'rgba(110, 231, 183, 0.85)';
            ctx.beginPath();
            ctx.arc(px, py, 2.1, 0, Math.PI * 2);
            ctx.fill();
          }

          const waveY = h * 0.86;
          ctx.strokeStyle = 'rgba(16, 185, 129, 0.14)';
          ctx.lineWidth = 1.2;
          ctx.beginPath();
          for (let x = 0; x <= w * 0.42; x += 6) {
            const y = waveY + Math.sin(x * 0.025 + angle * 5) * 14 * Math.sin(x * 0.008);
            if (x === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
          }
          ctx.stroke();

          requestAnimationFrame(drawFrame);
        }

        requestAnimationFrame(drawFrame);
      })();

      if (document.readyState === 'loading') {
        window.addEventListener('DOMContentLoaded', init);
      } else {
        init();
      }
    })();
  </script>
</body>
</html>
"""


# =============================================================================
# 8. HTTP SERVER ROUTES (SERVES LIVE WEBSITE + JSON API + HEALTH CHECK)
# =============================================================================
class WebTerminalHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload: dict, status: int = 200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        path = urlparse(self.path).path

        if path == "/health":
            with STATE_LOCK:
                txt = (
                    f"OK | 2s Scans: {SHARED_STATE['scans_completed']} | "
                    f"Alerts: {SHARED_STATE['alerts_sent']} | Last: {SHARED_STATE['last_scan']}\n"
                ).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            self.send_header("Content-Length", str(len(txt)))
            self.end_headers()
            self.wfile.write(txt)
            return

        if path == "/api/state":
            with STATE_LOCK:
                snapshot = {
                    "started_at": SHARED_STATE["started_at"],
                    "last_scan": SHARED_STATE["last_scan"],
                    "active_feed": SHARED_STATE["active_feed"],
                    "poll_interval_sec": POLL_INTERVAL_SEC,
                    "scans_completed": SHARED_STATE["scans_completed"],
                    "alerts_sent": SHARED_STATE["alerts_sent"],
                    "tg_configured": SHARED_STATE["tg_configured"],
                    "config": dict(CONFIG),
                    "coins": list(SHARED_STATE["coins"]),
                    "alert_history": list(SHARED_STATE["alert_history"]),
                }
            self._send_json(snapshot)
            return

        html_bytes = DASHBOARD_HTML.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

    def do_HEAD(self):
        self.send_response(200)
        self.end_headers()

    def do_POST(self):
        path = urlparse(self.path).path
        content_len = int(self.headers.get("Content-Length", "0"))
        raw_body = self.rfile.read(content_len) if content_len > 0 else b"{}"
        try:
            body = json.loads(raw_body.decode("utf-8"))
        except Exception:
            body = {}

        if path == "/api/config":
            with STATE_LOCK:
                if body.get("timeframe") in TF_MS:
                    CONFIG["timeframe"] = body["timeframe"]
                if "long_th" in body:
                    v = float(body["long_th"])
                    if 10 <= v <= 55:
                        CONFIG["long_th"] = v
                if "short_th" in body:
                    v = float(body["short_th"])
                    if 45 <= v <= 90:
                        CONFIG["short_th"] = v
                if "vwap_filter" in body:
                    CONFIG["vwap_filter"] = bool(body["vwap_filter"])
                if "funding_filter" in body:
                    CONFIG["funding_filter"] = bool(body["funding_filter"])
                cfg_copy = dict(CONFIG)

            if body.get("force_history"):
                FORCE_HISTORY_SYNC.set()
            if body.get("rescan"):
                RESCAN_EVENT.set()
            self._send_json({"ok": True, "config": cfg_copy})
            return

        if path == "/api/test-tg":
            with STATE_LOCK:
                cfg = dict(CONFIG)
            ok, err = send_telegram(
                f"✅ <b>Render 2-Second Live Web Terminal Test OK!</b>\n"
                f"TF: <b>{cfg['timeframe'].upper()}</b> | Long &lt; <b>{cfg['long_th']}</b> | Short &gt; <b>{cfg['short_th']}</b>\n"
                f"VWAP Filter: <b>{'ON' if cfg['vwap_filter'] else 'OFF'}</b> | "
                f"Funding Filter: <b>{'ON' if cfg['funding_filter'] else 'OFF'}</b>\n"
                f"⏰ {now_ist_str()}"
            )
            self._send_json({"ok": ok, "error": None if ok else err})
            return

        self._send_json({"ok": False, "error": "Not found"}, status=404)

    def log_message(self, format, *args):
        return


def main():
    if CONFIG["timeframe"] not in TF_MS:
        print(f"[ERROR] Unsupported TIMEFRAME: {CONFIG['timeframe']}")
        sys.exit(1)

    t = threading.Thread(target=scanner_worker, daemon=True)
    t.start()

    if SEND_STARTUP_MSG:
        send_telegram(
            f"✅ <b>Render 2-Second 24/7 Crypto Scanner Started!</b>\n"
            f"TF: <b>{CONFIG['timeframe'].upper()}</b> | Long &lt; <b>{CONFIG['long_th']}</b> | Short &gt; <b>{CONFIG['short_th']}</b>\n"
            f"VWAP Filter: <b>{'ON' if CONFIG['vwap_filter'] else 'OFF'}</b> | "
            f"Funding Filter: <b>{'ON' if CONFIG['funding_filter'] else 'OFF'}</b>\n"
            f"⏰ {now_ist_str()}"
        )

    server = ThreadingHTTPServer(("0.0.0.0", PORT), WebTerminalHandler)
    print(f"[{now_ist_str()}] 2-Second Live Web Terminal + 24/7 Crypto Scanner listening on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
