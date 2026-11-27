#!/usr/bin/env python3
"""
J.A.R.V.I.S. Combined VWAP + 9/21 EMA + 14-RSI Scalping Terminal (`render_bot.py`)
------------------------------------------------------------------------------------
- Strict 15m Pullback + Closed-Candle Momentum / Rejection Confirmation
- High-Contrast Transformers-Style Live Wave Background + Transparent Rows/Columns
- Interactive Wave Density & Ribbon Light Toggles + AI Voice Alert + 24/7 TG Alerts
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
# 1. CONFIGURATION (Environment Variables)
# =============================================================================
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN", "").strip()
TG_CHAT_ID = os.getenv("TG_CHAT_ID", "").strip()
TG_BOT_TOKEN_2 = os.getenv("TG_BOT_TOKEN_2", "").strip()
TG_CHAT_ID_2 = os.getenv("TG_CHAT_ID_2", "").strip()

CONFIG = {
    "timeframe": os.getenv("TIMEFRAME", "15m").strip() or "15m",
    "long_th": float(os.getenv("LONG_RSI_TH", "35")),
    "short_th": float(os.getenv("SHORT_RSI_TH", "70")),
    "pullback_pct": float(os.getenv("PULLBACK_PCT", "0.15")),
    "vwap_filter": os.getenv("VWAP_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"),
    "funding_filter": os.getenv("FUNDING_FILTER_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"),
    "scalp_alerts": os.getenv("SCALP_ALERTS_ENABLED", "true").strip().lower() in ("1", "true", "yes", "on"),
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
SESSION.headers.update({"User-Agent": "JARVIS-Strict-15m-Bot/2.3"})

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
    "tg_configured": bool((TG_BOT_TOKEN and TG_CHAT_ID) or (TG_BOT_TOKEN_2 and TG_CHAT_ID_2)),
    "coins": [],
    "alert_history": [],
}


# =============================================================================
# 3. FORMATTING & TECHNICAL MATH
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


def compute_ema_series(closes: list[float], period: int) -> list[float]:
    if not closes:
        return []
    if len(closes) < period:
        avg = sum(closes) / len(closes)
        return [avg] * len(closes)
    k = 2.0 / (period + 1)
    out = [sum(closes[:period]) / period]
    for px in closes[period:]:
        out.append(px * k + out[-1] * (1.0 - k))
    return out


def compute_live_ema(base_ema: float, live_price: float, period: int):
    if base_ema is None or live_price is None:
        return base_ema
    k = 2.0 / (period + 1)
    return live_price * k + base_ema * (1.0 - k)


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


def evaluate_closed_candle_confirmation(last_closed: dict, prev_closed: dict, last_ema21: float) -> dict:
    if not last_closed or not last_ema21 or last_ema21 <= 0:
        return {"bull_confirmed": False, "bear_confirmed": False, "note": "No candle"}

    o = last_closed["c"] if "o" not in last_closed else last_closed["o"]
    h, l, c = last_closed["h"], last_closed["l"], last_closed["c"]
    rng = max(h - l, 1e-9)
    lower_wick_ratio = (min(o, c) - l) / rng
    upper_wick_ratio = (h - max(o, c)) / rng

    touched_ema21_low = (abs(l - last_ema21) / last_ema21 <= 0.0025) or (l <= last_ema21)
    touched_ema21_high = (abs(h - last_ema21) / last_ema21 <= 0.0025) or (h >= last_ema21)

    bull_ok = touched_ema21_low and (c >= o) and (lower_wick_ratio >= 0.25)
    bear_ok = touched_ema21_high and (c <= o) and (upper_wick_ratio >= 0.25)

    return {
        "bull_confirmed": bull_ok,
        "bear_confirmed": bear_ok,
        "note": "Wick+Close OK" if (bull_ok or bear_ok) else "Awaiting Wick/Close",
    }


# =============================================================================
# 4. MULTI-SOURCE CANDLE & 2-SECOND BATCH TICKER FETCHERS
# =============================================================================
def candle_limit(tf: str) -> int:
    return 300 if tf == "5m" else 120


def fetch_funding_map() -> dict[str, float]:
    funding = {}
    try:
        r = SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=4)
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
            timeout=4,
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
        timeout=5,
    )
    r.raise_for_status()
    raw = r.json()
    if not isinstance(raw, list) or len(raw) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient Binance candles")
    return [
        {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
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
        timeout=5,
    )
    r.raise_for_status()
    raw = r.json()
    if not isinstance(raw, list) or len(raw) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient Hyperliquid candles")
    return [
        {"t": int(k["t"]), "o": float(k["o"]), "h": float(k["h"]), "l": float(k["l"]), "c": float(k["c"]), "v": float(k["v"])}
        for k in raw
    ]


def fetch_okx(item: dict, tf: str):
    r = SESSION.get(
        "https://www.okx.com/api/v5/market/candles",
        params={"instId": item["okx"], "bar": OKX_BAR[tf], "limit": candle_limit(tf)},
        timeout=5,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    if not isinstance(data, list) or len(data) <= RSI_PERIOD + 1:
        raise ValueError("Insufficient OKX candles")
    data.reverse()
    return [
        {"t": int(k[0]), "o": float(k[1]), "h": float(k[2]), "l": float(k[3]), "c": float(k[4]), "v": float(k[5])}
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
            timeout=2.5,
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
            timeout=2.5,
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
            timeout=2.5,
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
        r = SESSION.get("https://fapi.binance.com/fapi/v1/premiumIndex", timeout=2.5)
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
# 5. STRICT SIGNAL ENGINE & DUAL TELEGRAM SENDER
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


def evaluate_scalp_setup(price: float, rsi: float, vwap: float, ema9: float, ema21: float, candle_conf: dict, cfg: dict) -> dict:
    if None in (price, rsi, vwap, ema9, ema21) or price <= 0 or ema21 <= 0:
        return {"code": "WAIT", "label": "SYNCING...", "trend": "NEUTRAL"}

    pb_zone = float(cfg.get("pullback_pct", 0.15))
    ema_spread_pct = abs(ema9 - ema21) / ema21 * 100.0
    not_flat = ema_spread_pct >= 0.03

    bull_trend = (price > vwap) and (ema9 > ema21) and not_flat
    bear_trend = (price < vwap) and (ema9 < ema21) and not_flat

    dist_ema21_pct = abs(price - ema21) / price * 100.0
    dist_vwap_pct = abs(price - vwap) / price * 100.0
    near_zone = (dist_ema21_pct <= pb_zone) or (dist_vwap_pct <= pb_zone)

    bull_ok = bull_trend and near_zone and (48.0 <= rsi <= 62.0) and bool(candle_conf.get("bull_confirmed"))
    bear_ok = bear_trend and near_zone and (38.0 <= rsi <= 52.0) and bool(candle_conf.get("bear_confirmed"))

    if bull_ok:
        return {"code": "SCALP_LONG", "label": "LONG • STRICT PULLBACK", "trend": "BULL"}
    if bear_ok:
        return {"code": "SCALP_SHORT", "label": "SHORT • STRICT PULLBACK", "trend": "BEAR"}
    if bull_trend and near_zone:
        return {"code": "WATCH_LONG", "label": "WATCH LONG (WICK/RSI)", "trend": "BULL"}
    if bear_trend and near_zone:
        return {"code": "WATCH_SHORT", "label": "WATCH SHORT (WICK/RSI)", "trend": "BEAR"}
    if bull_trend:
        return {"code": "BULL_TREND", "label": "BULL TREND", "trend": "BULL"}
    if bear_trend:
        return {"code": "BEAR_TREND", "label": "BEAR TREND", "trend": "BEAR"}
    return {"code": "CHOP", "label": "VWAP CHOP", "trend": "CHOP"}


def _post_tg_single(tk: str, cid: str, html_text: str):
    url = f"https://api.telegram.org/bot{tk}/sendMessage"
    try:
        r = SESSION.post(
            url,
            data={"chat_id": cid, "text": html_text, "parse_mode": "HTML"},
            timeout=6,
        )
        data = r.json()
        if not r.ok or not data.get("ok"):
            err = data.get("description", f"HTTP {r.status_code}")
            print(f"[TG ERROR] ({cid}): {err}")
            return False, err
        return True, "OK"
    except Exception as exc:
        print(f"[TG ERROR] ({cid}): {exc}")
        return False, str(exc)


def send_telegram(html_text: str, token: str = None, chat_id: str = None):
    targets = []
    tk1 = (token or TG_BOT_TOKEN).strip()
    cid1 = (chat_id or TG_CHAT_ID).strip()
    if tk1 and cid1:
        targets.append((tk1, cid1))

    tk2 = TG_BOT_TOKEN_2.strip() or tk1
    cid2 = TG_CHAT_ID_2.strip()
    if tk2 and cid2 and (tk2, cid2) not in targets:
        targets.append((tk2, cid2))

    if not targets:
        return False, "Missing TG_BOT_TOKEN / TG_CHAT_ID in Environment"

    any_ok = False
    errors = []
    for tk, cid in targets:
        ok, err = _post_tg_single(tk, cid, html_text)
        if ok:
            any_ok = True
        else:
            errors.append(f"{cid}: {err}")

    if any_ok:
        return True, "OK"
    return False, " | ".join(errors)


def format_signal_msg(item: dict, side: str, price: float, rsi: float, vwap: float, vwap_diff: float, ema9: float, ema21: float, scalp_lbl: str, funding, source: str, cfg: dict) -> str:
    is_long = "LONG" in side
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
        f"{icon} <b>{side}: {item['pair']}</b> ({cfg['timeframe'].upper()}){filter_tag}\n"
        f"━━━━━━━━━━━━━━━━━━\n"
        f"⚡ <b>Setup:</b> {scalp_lbl} (±{cfg['pullback_pct']:.2f}%)\n"
        f"📊 <b>RSI (14):</b> {rsi:.2f}\n"
        f"💵 <b>Mark Price:</b> ${fmt_price(price)}\n"
        f"📈 <b>Daily VWAP:</b> ${fmt_price(vwap)} ({vwap_pos} {fmt_pct(vwap_diff)})\n"
        f"〰️ <b>9 / 21 EMA:</b> ${fmt_price(ema9)} / ${fmt_price(ema21)}\n"
        f"💰 <b>8h Funding:</b> {fmt_funding(funding)}\n"
        f"🎯 <b>Target (±6.28%):</b> ${fmt_price(tp)}\n"
        f"🔁 <b>Next Avg (±3.14%):</b> ${fmt_price(avg)}\n"
        f"🛑 <b>Hard SL (±2.0%):</b> ${fmt_price(hard_sl)} | <b>Split (±4.8%):</b> ${fmt_price(split_sl)}\n"
        f"📡 <b>Feed:</b> {source}\n"
        f"⏰ <b>Time:</b> {now_ist_str()}"
    )


# =============================================================================
# 6. 2-SECOND 24/7 BACKGROUND SCANNER THREAD
# =============================================================================
def seed_initial_snapshot():
    feed, prices = fetch_live_prices_batch()
    if not prices:
        return
    rows = []
    for item in COINS:
        ck = item["coin"]
        px = prices.get(ck)
        if px is None:
            continue
        rows.append({
            "sr": item["sr"],
            "coin": ck,
            "pair": item["pair"],
            "name": item["name"],
            "price": px,
            "prevClose": px,
            "changePct": 0.0,
            "vwap": px,
            "vwapDiffPct": 0.0,
            "ema9": px,
            "ema21": px,
            "emaTrend": "NEUTRAL",
            "scalpCode": "WAIT",
            "scalpLabel": "SYNCING...",
            "candleNote": "Syncing...",
            "funding": None,
            "rsi": None,
            "rsiRank": None,
            "signal": "LOAD",
            "source": feed or "Live",
        })
    if rows:
        with STATE_LOCK:
            if not SHARED_STATE["coins"]:
                SHARED_STATE["coins"] = rows
                SHARED_STATE["last_scan"] = now_ist_str()
                SHARED_STATE["active_feed"] = feed or "Live"


def sync_all_candle_histories(tf: str, coin_cache: dict):
    funding_map = fetch_funding_map()

    def load_one(item):
        source, candles = fetch_candles_with_failover(item, tf)
        closed = candles[:-1]
        cur = candles[-1]
        closed_closes = [k["c"] for k in closed]
        prev_close = closed_closes[-1]
        base_gain, base_loss = compute_wilder_base(closed_closes, RSI_PERIOD)
        ema9_series = compute_ema_series(closed_closes, 9)
        ema21_series = compute_ema_series(closed_closes, 21)
        last_ema9 = ema9_series[-1] if ema9_series else prev_close
        last_ema21 = ema21_series[-1] if ema21_series else prev_close
        last_closed_candle = closed[-1] if closed else None
        prev_closed_candle = closed[-2] if len(closed) >= 2 else None
        candle_conf = evaluate_closed_candle_confirmation(last_closed_candle, prev_closed_candle, last_ema21)

        day_start = get_utc_day_start_ms(cur["t"])
        vwap_base_pv, vwap_base_vol = compute_vwap_base(closed, day_start)
        return item["coin"], {
            "loaded": True,
            "source": source,
            "prev_close": prev_close,
            "base_gain": base_gain,
            "base_loss": base_loss,
            "base_ema9": last_ema9,
            "base_ema21": last_ema21,
            "candle_conf": candle_conf,
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
    seed_initial_snapshot()

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
            ema9 = compute_live_ema(c.get("base_ema9"), price, 9)
            ema21 = compute_live_ema(c.get("base_ema21"), price, 21)
            candle_conf = c.get("candle_conf") or {}
            funding = c.get("funding")

            sig = evaluate_signal(price, rsi, vwap, funding, cfg)
            scalp = evaluate_scalp_setup(price, rsi, vwap, ema9, ema21, candle_conf, cfg)
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
                "ema9": ema9,
                "ema21": ema21,
                "emaTrend": scalp["trend"],
                "scalpCode": scalp["code"],
                "scalpLabel": scalp["label"],
                "candleNote": candle_conf.get("note", ""),
                "funding": funding,
                "rsi": rsi,
                "signal": sig,
                "source": feed_name,
            })

            alert_side = None
            if sig in ("LONG", "SHORT"):
                alert_side = f"{sig} SIGNAL"
            elif cfg.get("scalp_alerts") and scalp["code"] in ("SCALP_LONG", "SCALP_SHORT"):
                alert_side = "SCALP LONG" if scalp["code"] == "SCALP_LONG" else "SCALP SHORT"

            if alert_side:
                key = (ck, alert_side, tf)
                now_ts = time.time()
                if now_ts - last_sent.get(key, 0) >= cooldown_sec:
                    msg = format_signal_msg(
                        item, alert_side, price, rsi, vwap, vwap_diff,
                        ema9, ema21, scalp["label"], funding, feed_name, cfg
                    )
                    ok, _ = send_telegram(msg)
                    if ok:
                        last_sent[key] = now_ts
                        with STATE_LOCK:
                            SHARED_STATE["alerts_sent"] += 1
                            SHARED_STATE["alert_history"].insert(0, {
                                "time": datetime.now(IST).strftime("%d %b %H:%M:%S IST"),
                                "pair": item["pair"],
                                "side": alert_side,
                                "tf": tf.upper(),
                                "price": price,
                                "rsi": rsi,
                                "vwap": vwap,
                                "ema9": ema9,
                                "ema21": ema21,
                                "funding": funding,
                            })
                            SHARED_STATE["alert_history"] = SHARED_STATE["alert_history"][:30]
                        print(f"[{now_ist_str()}] >>> 2s ALERT SENT: {alert_side} {item['pair']} (RSI {rsi:.2f})")

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
# 7. EMBEDDED HTML DASHBOARD (DENSITY + RIBBON LIGHT CONTROLS)
# =============================================================================
DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Strict 15m VWAP + 9/21 EMA + 14-RSI Terminal — J.A.R.V.I.S.</title>
  <style>
    :root {
      --bg-main: #020712; --border: rgba(56, 189, 248, 0.24); --border-light: rgba(103, 232, 249, 0.38);
      --text-main: #f8fafc; --text-muted: #cbd5e1; --text-dim: #94a3b8;
      --green: #10b981; --green-bg: rgba(16, 185, 129, 0.16); --green-border: rgba(16, 185, 129, 0.45);
      --red: #f43f5e; --red-bg: rgba(244, 63, 94, 0.16); --red-border: rgba(244, 63, 94, 0.45);
      --amber: #f59e0b; --amber-bg: rgba(245, 158, 11, 0.16); --amber-border: rgba(245, 158, 11, 0.42);
      --cyan: #06b6d4; --cyan-light: #67e8f9; --cyan-bg: rgba(6, 182, 212, 0.16); --cyan-border: rgba(6, 182, 212, 0.42);
      --purple-light: #d8b4fe; --purple-bg: rgba(168, 85, 247, 0.16); --purple-border: rgba(168, 85, 247, 0.42);
      --radius: 10px;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, Helvetica, Arial, sans-serif;
      --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body {
      background-color: transparent;
      color: var(--text-main); font-family: var(--font-sans); font-size: 14px; line-height: 1.45; min-height: 100vh;
    }
    #waveBgCanvas {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
    }
    .header, .main, .toast-wrap {
      position: relative;
      z-index: 2;
      background: transparent;
    }
    .mono { font-family: var(--font-mono); font-variant-numeric: tabular-nums; text-shadow: 0 1px 6px rgba(0, 0, 0, 0.85); }
    .text-green { color: var(--green); } .text-red { color: var(--red); } .text-amber { color: var(--amber); }
    .text-cyan { color: var(--cyan-light); } .text-purple { color: var(--purple-light); } .text-muted { color: var(--text-muted); } .text-dim { color: var(--text-dim); }
    .text-right { text-align: right; } .text-center { text-align: center; }
    @keyframes pulseDot { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
    .pulse-dot { animation: pulseDot 1.4s infinite ease-in-out; }
    .header { position: sticky; top: 0; z-index: 40; background: rgba(4, 12, 24, 0.35); border-bottom: 1px solid var(--border); backdrop-filter: blur(4px); }
    .card, .kpi-card, .control-group, .live-bar { background: rgba(6, 15, 30, 0.14); border-color: var(--border); }
    .container { max-width: 1780px; margin: 0 auto; padding: 0 16px; }
    .header-inner { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 0; }
    .brand { display: flex; align-items: center; gap: 10px; }
    .brand-icon { width: 38px; height: 38px; border-radius: 10px; display: flex; align-items: center; justify-content: center; background: rgba(6, 182, 212, 0.18); border: 1px solid #67e8f9; font-weight: 800; color: #67e8f9; }
    .brand-title { font-size: 16px; font-weight: 700; display: flex; align-items: center; flex-wrap: wrap; gap: 6px; text-shadow: 0 1px 8px rgba(0, 0, 0, 0.9); }
    .brand-sub { font-size: 11px; color: var(--text-muted); margin-top: 1px; text-shadow: 0 1px 6px rgba(0, 0, 0, 0.9); }
    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 2px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid var(--border); }
    .pill-tf { background: rgba(6, 182, 212, 0.14); color: var(--cyan-light); border-color: var(--cyan-border); }
    .pill-cloud { background: rgba(16, 185, 129, 0.14); color: #6ee7b7; border-color: var(--green-border); font-size: 10px; }
    .live-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; border: 1px solid var(--border); border-radius: 999px; padding: 5px 12px; font-size: 12px; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); }
    .live-sep { color: var(--border-light); }
    .controls-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 10px 0; border-top: 1px solid rgba(56, 189, 248, 0.16); }
    .control-group { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border); border-radius: 8px; padding: 4px 8px; font-size: 12px; }
    input[type="number"] { background: rgba(4, 12, 24, 0.38); color: var(--text-main); border: 1px solid var(--border-light); border-radius: 6px; padding: 3px 4px; width: 54px; text-align: center; font-weight: 700; }
    .btn { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 6px 12px; border-radius: 8px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid var(--border-light); background: rgba(6, 15, 30, 0.25); color: var(--text-main); text-decoration: none; }
    .btn-tf { padding: 5px 10px; border: 1px solid transparent; background: transparent; color: var(--text-muted); border-radius: 6px; }
    .btn-tf.active { background: rgba(16, 185, 129, 0.18); color: #34d399; border-color: var(--green-border); }
    .btn-vwap.active { background: rgba(245, 158, 11, 0.16); color: #fcd34d; border-color: var(--amber-border); }
    .btn-funding.active { background: rgba(168, 85, 247, 0.16); color: var(--purple-light); border-color: var(--purple-border); }
    .btn-scalp.active { background: rgba(16, 185, 129, 0.16); color: #6ee7b7; border-color: var(--green-border); }
    .btn-voice.active { background: rgba(6, 182, 212, 0.16); color: var(--cyan-light); border-color: var(--cyan-border); }
    .btn-wave { background: rgba(56, 189, 248, 0.14); color: #bae6fd; border-color: rgba(125, 211, 252, 0.45); }
    .btn-light { background: rgba(250, 204, 21, 0.14); color: #fef08a; border-color: rgba(253, 224, 71, 0.45); }
    .btn-tg.active { background: rgba(6, 182, 212, 0.16); color: var(--cyan-light); border-color: var(--cyan-border); }
    .btn-long { padding: 4px 8px; font-size: 11px; background: rgba(16, 185, 129, 0.15); color: #34d399; border-color: var(--green-border); border-radius: 6px; }
    .btn-short { padding: 4px 8px; font-size: 11px; background: rgba(244, 63, 94, 0.15); color: #fb7185; border-color: var(--red-border); border-radius: 6px; }
    .main { padding: 16px 0 36px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    @media (max-width: 1320px) { .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
    @media (max-width: 680px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    .kpi-card { border: 1px solid var(--border); border-radius: var(--radius); padding: 13px 14px; display: flex; flex-direction: column; justify-content: space-between; }
    .kpi-top { display: flex; align-items: center; justify-content: space-between; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--text-muted); text-shadow: 0 1px 6px rgba(0, 0, 0, 0.85); }
    .kpi-val { font-size: 20px; font-weight: 700; margin-top: 6px; display: flex; align-items: baseline; justify-content: space-between; }
    .kpi-sub { font-size: 11px; color: var(--text-muted); margin-top: 4px; text-shadow: 0 1px 6px rgba(0, 0, 0, 0.85); }
    .card { border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
    .table-wrap { overflow-x: auto; width: 100%; background: transparent; }
    table { width: 100%; border-collapse: collapse; white-space: nowrap; font-size: 13px; background: transparent; }
    thead, thead tr, thead th {
      background: transparent !important;
      border-bottom: 1px solid rgba(103, 232, 249, 0.28);
      color: #a5f3fc;
      text-shadow: 0 1px 8px rgba(0, 0, 0, 0.95);
    }
    thead th { padding: 11px 12px; font-size: 11px; font-weight: 700; text-transform: uppercase; }
    tbody, tbody tr, tbody td {
      background: transparent !important;
    }
    tbody tr { border-bottom: 1px solid rgba(103, 232, 249, 0.14); }
    tbody td { padding: 10px 12px; vertical-align: middle; text-shadow: 0 1px 7px rgba(0, 0, 0, 0.9); }
    .badge { display: inline-block; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid transparent; background: transparent; }
    .badge-long { background: rgba(16, 185, 129, 0.14); color: #34d399; border-color: var(--green-border); }
    .badge-short { background: rgba(244, 63, 94, 0.14); color: #fb7185; border-color: var(--red-border); }
    .badge-wait { background: rgba(245, 158, 11, 0.14); color: #fcd34d; border-color: var(--amber-border); }
    .badge-cyan { background: rgba(6, 182, 212, 0.14); color: var(--cyan-light); border-color: var(--cyan-border); }
    .badge-neutral { background: rgba(15, 23, 42, 0.2); color: #e2e8f0; border-color: rgba(148, 163, 184, 0.35); }
    .toast-wrap { position: fixed; bottom: 16px; right: 16px; z-index: 60; display: flex; flex-direction: column; gap: 8px; }
    .toast { padding: 10px 14px; border-radius: 10px; font-size: 12px; font-weight: 600; background: rgba(6, 15, 30, 0.78); border: 1px solid var(--border-light); color: var(--text-main); }
  </style>
</head>
<body>
  <canvas id="waveBgCanvas"></canvas>
  <header class="header">
    <div class="container">
      <div class="header-inner">
        <div class="brand">
          <div class="brand-icon mono">J.A</div>
          <div>
            <div class="brand-title">
              <span>Strict 15m VWAP + 9/21 EMA + 14-RSI Terminal</span>
              <span id="headerTfBadge" class="pill pill-tf mono">15M</span>
              <span class="pill pill-cloud mono">CLEAR WAVE + TRANSPARENT GRID</span>
            </div>
            <div class="brand-sub">Session VWAP (UTC) + 9/21 EMA (±0.15%) + Closed Candle Wick/Close + Wilder's 14-RSI + AI Voice Alerts</div>
          </div>
        </div>
        <div class="live-bar">
          <span class="status-dot pulse-dot"></span>
          <strong id="liveFeedName" class="mono">CONNECTING...</strong>
          <span class="live-sep">|</span>
          <span class="text-muted">Scans: <strong id="liveScanCount" class="mono text-cyan">0</strong></span>
          <span class="live-sep">|</span>
          <span id="liveTimestamp" class="mono text-muted">--:--:-- IST</span>
        </div>
      </div>
      <div class="controls-bar">
        <div class="control-group" style="padding: 2px;">
          <button type="button" data-tf="5m" class="btn btn-tf">5m</button>
          <button type="button" data-tf="15m" class="btn btn-tf active">15m</button>
          <button type="button" data-tf="1h" class="btn btn-tf">1h</button>
        </div>
        <div class="control-group"><label for="longThInput" class="text-green">Long RSI &lt;</label><input id="longThInput" type="number" min="10" max="55" value="35" class="mono text-green" /></div>
        <div class="control-group"><label for="shortThInput" class="text-red">Short RSI &gt;</label><input id="shortThInput" type="number" min="45" max="90" value="70" class="mono text-red" /></div>
        <div class="control-group"><label for="pullbackInput" class="text-cyan">Pullback ± %</label><input id="pullbackInput" type="number" step="0.05" min="0.05" max="2.0" value="0.15" class="mono text-cyan" /></div>
        <button id="vwapFilterToggleBtn" type="button" class="btn btn-vwap active"><span id="vwapFilterLabel">⚡ VWAP Filter: ON</span></button>
        <button id="fundingFilterToggleBtn" type="button" class="btn btn-funding active"><span id="fundingFilterLabel">💰 Funding Filter: ON</span></button>
        <button id="scalpAlertToggleBtn" type="button" class="btn btn-scalp active"><span id="scalpAlertLabel">🎯 Strict TG: ON</span></button>
        <button id="waveDensityBtn" type="button" class="btn btn-wave"><span id="waveDensityLabel">🌊 Density: HIGH</span></button>
        <button id="ribbonLightBtn" type="button" class="btn btn-light"><span id="ribbonLightLabel">💡 Ribbon Light: HIGH</span></button>
        <button id="voiceToggleBtn" type="button" class="btn btn-voice active"><span id="voiceLabel">🔊 AI Voice: ON</span></button>
        <button id="tgTestBtn" type="button" class="btn btn-tg active"><span>✈️ Test TG</span></button>
      </div>
    </div>
  </header>
  <main class="main container">
    <section class="kpi-grid">
      <div class="kpi-card"><div class="kpi-top"><span>Live Sync</span><span id="kpiTfTag" class="mono text-cyan">15m</span></div><div id="kpiSyncTime" class="kpi-val mono" style="font-size: 16px;">--:--:-- IST</div><div id="kpiFeedDetail" class="kpi-sub">Loading...</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Strict Pullbacks</span><span id="kpiPbTag" class="text-cyan mono">±0.15%</span></div><div class="kpi-val mono"><span id="kpiScalpCount" class="text-cyan">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div id="kpiScalpSub" class="kpi-sub">RSI 48–62 L / 38–52 S + Wick</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>RSI Long Signals</span><span class="text-green">LIVE</span></div><div class="kpi-val mono"><span id="kpiLongCount" class="text-green">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div class="kpi-sub">Oversold + VWAP + Funding</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>RSI Short Signals</span><span class="text-red">LIVE</span></div><div class="kpi-val mono"><span id="kpiShortCount" class="text-red">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div class="kpi-sub">Overbought + VWAP + Funding</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Oversold</span><span class="text-green mono">Lowest RSI</span></div><div class="kpi-val"><span id="kpiOversoldCoin" style="font-size: 16px;">--</span><span id="kpiOversoldRsi" class="mono text-green">--</span></div><div id="kpiOversoldPrice" class="kpi-sub mono">Mark: --</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Overbought</span><span class="text-red mono">Highest RSI</span></div><div class="kpi-val"><span id="kpiOverboughtCoin" style="font-size: 16px;">--</span><span id="kpiOverboughtRsi" class="mono text-red">--</span></div><div id="kpiOverboughtPrice" class="kpi-sub mono">Mark: --</div></div>
    </section>
    <div class="card"><div class="table-wrap"><table><thead><tr><th>Rank</th><th>Pair</th><th class="text-right">Mark Price</th><th class="text-right">Session VWAP (UTC)</th><th class="text-right">9 / 21 EMA</th><th class="text-right">14-RSI</th><th class="text-center">Strict Scalp Setup</th><th class="text-center">Candle Check</th><th class="text-center">RSI Signal</th><th class="text-right">Target ±6.28%</th><th class="text-right">Hard SL ±2.0%</th><th class="text-center">Trade Log</th></tr></thead><tbody id="screenerTbody"><tr><td colspan="12" style="padding:28px;text-align:center;">Connecting to live feed...</td></tr></tbody></table></div></div>
  </main>
  <div id="toastWrap" class="toast-wrap"></div>
  <script>
    (() => {
      'use strict';
      const STORAGE_TRADES = 'top10_futures_rsi_v3_trades';
      const WAVE_MODES = ['HIGH', 'MED', 'LOW'];
      const LIGHT_MODES = ['HIGH', 'MED', 'LOW'];
      let cfg = { timeframe: '15m', long_th: 35, short_th: 70, pullback_pct: 0.15, vwap_filter: true, funding_filter: true, scalp_alerts: true };
      let coins = [];
      let voiceEnabled = true;
      let userUnlockedAudio = false;
      let waveDensityIdx = 0; // 0: HIGH, 1: MED, 2: LOW
      let ribbonLightIdx = 0; // 0: HIGH, 1: MED, 2: LOW
      const announcedAlertKeys = new Set();

      const fmtP = v => v == null ? '--' : v.toLocaleString('en-US', { minimumFractionDigits: Math.abs(v) < 1 ? 4 : 2, maximumFractionDigits: Math.abs(v) < 1 ? 4 : 2 });
      const fmtPct = v => v == null ? '--' : (v > 0 ? '+' : '') + v.toFixed(2) + '%';

      function speakAiAlert(text) {
        if (!voiceEnabled || !userUnlockedAudio) return;
        try {
          if (!('speechSynthesis' in window)) return;
          window.speechSynthesis.cancel();
          const utter = new SpeechSynthesisUtterance(text);
          const voices = window.speechSynthesis.getVoices() || [];
          const preferred = voices.find(v => /en-IN|hi-IN|en-GB|Google/i.test(v.lang + ' ' + v.name));
          if (preferred) utter.voice = preferred;
          utter.rate = 1.02;
          utter.pitch = 1.05;
          utter.volume = 1.0;
          window.speechSynthesis.speak(utter);
        } catch {}
      }

      function toast(m, speakText = null) {
        const w = document.getElementById('toastWrap');
        const e = document.createElement('div');
        e.className = 'toast';
        e.textContent = m;
        w.appendChild(e);
        setTimeout(() => e.remove(), 3500);
        if (speakText) speakAiAlert(speakText);
      }

      document.addEventListener('pointerdown', () => {
        userUnlockedAudio = true;
        if ('speechSynthesis' in window) window.speechSynthesis.getVoices();
      }, { passive: true });

      async function poll() {
        try {
          const r = await fetch('/api/state', { cache: 'no-store' });
          const d = await r.json();
          document.getElementById('liveFeedName').textContent = d.active_feed || 'LIVE';
          document.getElementById('liveScanCount').textContent = (d.scans_completed || 0).toLocaleString();
          document.getElementById('liveTimestamp').textContent = d.last_scan || '--';
          document.getElementById('kpiSyncTime').textContent = d.last_scan || '--';
          document.getElementById('kpiFeedDetail').textContent = d.active_feed || 'Connected';
          if (d.config) {
            cfg = d.config;
            const pb = Number(cfg.pullback_pct ?? 0.15);
            document.getElementById('headerTfBadge').textContent = cfg.timeframe.toUpperCase();
            document.getElementById('kpiTfTag').textContent = cfg.timeframe;
            document.getElementById('kpiPbTag').textContent = `±${pb.toFixed(2)}%`;
            if (document.activeElement !== document.getElementById('pullbackInput')) {
              document.getElementById('pullbackInput').value = pb.toFixed(2);
            }
            document.querySelectorAll('.btn-tf').forEach(b => b.classList.toggle('active', b.dataset.tf === cfg.timeframe));
            document.getElementById('vwapFilterLabel').textContent = `⚡ VWAP Filter: ${cfg.vwap_filter ? 'ON' : 'OFF'}`;
            document.getElementById('fundingFilterLabel').textContent = `💰 Funding Filter: ${cfg.funding_filter ? 'ON' : 'OFF'}`;
            document.getElementById('scalpAlertLabel').textContent = `🎯 Strict TG: ${cfg.scalp_alerts ? 'ON' : 'OFF'}`;
          }
          if (Array.isArray(d.alert_history) && d.alert_history.length > 0) {
            const latest = d.alert_history[0];
            const alertKey = `${latest.time}|${latest.pair}|${latest.side}|${latest.tf}`;
            if (!announcedAlertKeys.has(alertKey)) {
              const isFirstLoad = announcedAlertKeys.size === 0 && (d.scans_completed || 0) <= 2;
              announcedAlertKeys.add(alertKey);
              if (!isFirstLoad) {
                const cleanPair = String(latest.pair || '').replace('USDT', '');
                const spoken = `Alert! ${latest.side} on ${cleanPair} at price ${fmtP(latest.price)}, RSI ${Number(latest.rsi || 0).toFixed(1)}`;
                toast(`🔔 ${latest.side}: ${latest.pair} ($${fmtP(latest.price)})`, spoken);
              }
            }
          }
          if (Array.isArray(d.coins)) {
            coins = d.coins;
            const valid = coins.filter(c => c.rsi != null);
            const scalpHits = valid.filter(c => c.scalpCode === 'SCALP_LONG' || c.scalpCode === 'SCALP_SHORT');
            document.getElementById('kpiScalpCount').textContent = scalpHits.length;
            document.getElementById('kpiScalpSub').textContent = scalpHits.length
              ? scalpHits.map(x => `${x.coin} (${x.scalpCode === 'SCALP_LONG' ? 'LONG' : 'SHORT'})`).join(', ')
              : `RSI 48–62 L / 38–52 S (±${Number(cfg.pullback_pct ?? 0.15).toFixed(2)}%)`;
            document.getElementById('kpiLongCount').textContent = valid.filter(c => c.signal === 'LONG').length;
            document.getElementById('kpiShortCount').textContent = valid.filter(c => c.signal === 'SHORT').length;
            if (valid.length) {
              const s = valid.slice().sort((a, b) => a.rsi - b.rsi);
              document.getElementById('kpiOversoldCoin').textContent = s[0].pair;
              document.getElementById('kpiOversoldRsi').textContent = s[0].rsi.toFixed(2);
              document.getElementById('kpiOversoldPrice').textContent = 'Mark: $' + fmtP(s[0].price);
              const h = s[s.length - 1];
              document.getElementById('kpiOverboughtCoin').textContent = h.pair;
              document.getElementById('kpiOverboughtRsi').textContent = h.rsi.toFixed(2);
              document.getElementById('kpiOverboughtPrice').textContent = 'Mark: $' + fmtP(h.price);
            }
            const priority = { SCALP_LONG: 0, SCALP_SHORT: 0, LONG: 1, SHORT: 1, WATCH_LONG: 2, WATCH_SHORT: 2, BULL_TREND: 3, BEAR_TREND: 3, CHOP: 4, WAIT: 5 };
            const sorted = coins.slice().sort((a, b) => {
              const pa = priority[a.scalpCode] ?? 5;
              const pb = priority[b.scalpCode] ?? 5;
              if (pa !== pb) return pa - pb;
              return (a.rsi ?? 999) - (b.rsi ?? 999);
            });
            document.getElementById('screenerTbody').innerHTML = sorted.map(c => {
              const isShort = c.signal === 'SHORT' || c.scalpCode === 'SCALP_SHORT';
              const tp = c.price ? (isShort ? c.price * 0.9372 : c.price * 1.0628) : null;
              const sl = c.price ? (isShort ? c.price * 1.02 : c.price * 0.98) : null;
              const cls = c.signal === 'LONG' ? 'badge-long' : c.signal === 'SHORT' ? 'badge-short' : 'badge-wait';
              const scalpCls = c.scalpCode === 'SCALP_LONG' ? 'badge-long'
                : c.scalpCode === 'SCALP_SHORT' ? 'badge-short'
                : (c.scalpCode === 'WATCH_LONG' || c.scalpCode === 'WATCH_SHORT') ? 'badge-wait'
                : (c.scalpCode === 'BULL_TREND' || c.scalpCode === 'BEAR_TREND') ? 'badge-cyan'
                : 'badge-neutral';
              const emaBull = c.ema9 != null && c.ema21 != null && c.ema9 >= c.ema21;
              return `<tr>
                <td class="mono text-dim">#${c.sr}</td>
                <td><strong class="mono">${c.pair}</strong><div class="text-muted" style="font-size:11px;">${c.name}</div></td>
                <td class="text-right mono" style="font-weight:700;">$${fmtP(c.price)}</td>
                <td class="text-right mono ${c.vwapDiffPct >= 0 ? 'text-green' : 'text-red'}">$${fmtP(c.vwap)} (${fmtPct(c.vwapDiffPct)})</td>
                <td class="text-right mono">
                  <span class="${emaBull ? 'text-green' : 'text-red'}">$${fmtP(c.ema9)}</span>
                  <span class="text-dim"> / </span>
                  <span class="text-amber">$${fmtP(c.ema21)}</span>
                </td>
                <td class="text-right mono" style="font-weight:700;">${c.rsi != null ? c.rsi.toFixed(2) : '--'}</td>
                <td class="text-center"><span class="badge ${scalpCls}">${c.scalpLabel || 'WAIT'}</span></td>
                <td class="text-center text-muted" style="font-size:11px;">${c.candleNote || '--'}</td>
                <td class="text-center"><span class="badge ${cls}">${c.signal}</span></td>
                <td class="text-right mono text-green">$${fmtP(tp)}</td>
                <td class="text-right mono text-red">$${fmtP(sl)}</td>
                <td class="text-center"><button type="button" class="btn btn-long" data-add-c="${c.coin}" data-add-s="LONG">+ Long</button> <button type="button" class="btn btn-short" data-add-c="${c.coin}" data-add-s="SHORT">+ Short</button></td>
              </tr>`;
            }).join('');
          }
        } catch {}
      }

      async function pushCfg(patch) { await fetch('/api/config', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ ...patch, rescan: true, force_history: true }) }); poll(); }
      document.querySelectorAll('.btn-tf').forEach(b => b.addEventListener('click', () => pushCfg({ timeframe: b.dataset.tf })));
      document.getElementById('longThInput').addEventListener('change', e => pushCfg({ long_th: parseFloat(e.target.value) }));
      document.getElementById('shortThInput').addEventListener('change', e => pushCfg({ short_th: parseFloat(e.target.value) }));
      document.getElementById('pullbackInput').addEventListener('change', e => {
        const v = parseFloat(e.target.value);
        if (!isNaN(v) && v >= 0.05 && v <= 2.0) {
          pushCfg({ pullback_pct: v });
          toast(`Pullback zone set to ±${v.toFixed(2)}%`);
        }
      });
      document.getElementById('vwapFilterToggleBtn').addEventListener('click', () => pushCfg({ vwap_filter: !cfg.vwap_filter }));
      document.getElementById('fundingFilterToggleBtn').addEventListener('click', () => pushCfg({ funding_filter: !cfg.funding_filter }));
      document.getElementById('scalpAlertToggleBtn').addEventListener('click', () => pushCfg({ scalp_alerts: !cfg.scalp_alerts }));

      document.getElementById('waveDensityBtn').addEventListener('click', () => {
        userUnlockedAudio = true;
        waveDensityIdx = (waveDensityIdx + 1) % WAVE_MODES.length;
        const mode = WAVE_MODES[waveDensityIdx];
        document.getElementById('waveDensityLabel').textContent = `🌊 Density: ${mode}`;
        toast(`Wave Density: ${mode}`);
      });

      document.getElementById('ribbonLightBtn').addEventListener('click', () => {
        userUnlockedAudio = true;
        ribbonLightIdx = (ribbonLightIdx + 1) % LIGHT_MODES.length;
        const mode = LIGHT_MODES[ribbonLightIdx];
        document.getElementById('ribbonLightLabel').textContent = `💡 Ribbon Light: ${mode}`;
        toast(`Ribbon Light: ${mode}`);
      });

      const voiceBtn = document.getElementById('voiceToggleBtn');
      voiceBtn.addEventListener('click', () => {
        userUnlockedAudio = true;
        voiceEnabled = !voiceEnabled;
        voiceBtn.classList.toggle('active', voiceEnabled);
        document.getElementById('voiceLabel').textContent = `🔊 AI Voice: ${voiceEnabled ? 'ON' : 'OFF'}`;
        if (voiceEnabled) speakAiAlert('AI voice alert system online');
      });

      document.getElementById('tgTestBtn').addEventListener('click', async () => {
        userUnlockedAudio = true;
        const r = await fetch('/api/test-tg', { method: 'POST' });
        const d = await r.json();
        toast(d.ok ? 'Test sent to TG Bots!' : 'Error: ' + d.error, 'Telegram alert test completed');
      });

      document.addEventListener('click', e => {
        const b = e.target.closest('[data-add-c]');
        if (b) {
          const c = coins.find(x => x.coin === b.dataset.addC);
          if (!c) return;
          const trades = JSON.parse(localStorage.getItem(STORAGE_TRADES) || '[]');
          trades.push({ id: 'ft_' + Date.now(), coin: c.coin, side: b.dataset.addS, mode: 'single', entry: c.price, margin: 100, lev: 5, rsi: c.rsi });
          localStorage.setItem(STORAGE_TRADES, JSON.stringify(trades));
          toast(`Added ${b.dataset.addS} ${c.pair} ($100 • 5x)`, `${b.dataset.addS} position logged for ${c.coin}`);
        }
      });

      // High-contrast Transformers-style live wave background with Density & Ribbon Light controls
      (function initTransformersWaveBg() {
        const canvas = document.getElementById('waveBgCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let w = 0, h = 0, dpr = Math.min(window.devicePixelRatio || 1, 2);
        let t = 0;

        function resize() {
          w = window.innerWidth;
          h = window.innerHeight;
          canvas.width = Math.max(1, w) * dpr;
          canvas.height = Math.max(1, h) * dpr;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function getDensitySettings() {
          const mode = WAVE_MODES[waveDensityIdx] || 'HIGH';
          if (mode === 'LOW') return { ribbonCount: 2, gridStep: 112, sparkCount: 4, ampScale: 0.72 };
          if (mode === 'MED') return { ribbonCount: 4, gridStep: 84, sparkCount: 7, ampScale: 0.90 };
          return { ribbonCount: 6, gridStep: 60, sparkCount: 12, ampScale: 1.12 };
        }

        function getLightSettings() {
          const mode = LIGHT_MODES[ribbonLightIdx] || 'HIGH';
          if (mode === 'LOW') return { alphaScale: 0.35, widthScale: 0.75, glowScale: 0.35 };
          if (mode === 'MED') return { alphaScale: 0.68, widthScale: 0.92, glowScale: 0.70 };
          return { alphaScale: 1.0, widthScale: 1.15, glowScale: 1.25 };
        }

        function rgbaWithScale(rgb, baseAlpha, scale) {
          const a = Math.max(0.03, Math.min(1, baseAlpha * scale));
          return `rgba(${rgb}, ${a.toFixed(3)})`;
        }

        function drawWaveRibbon(baseY, amp, freq, speed, phase, rgb, baseAlpha, lineW, glowBlur, light) {
          ctx.save();
          ctx.beginPath();
          const step = 4;
          for (let x = 0; x <= w + step; x += step) {
            const normX = x / Math.max(1, w);
            const envelope = Math.pow(Math.sin(normX * Math.PI), 0.52);
            const surge = 1 + 0.42 * Math.sin(normX * 8 - t * speed * 1.7);
            const y =
              baseY +
              Math.sin(x * freq + t * speed + phase) * amp * envelope * surge +
              Math.cos(x * freq * 2.2 - t * speed * 1.4 + phase) * (amp * 0.42) * envelope;
            if (x === 0) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
          }
          const strokeCol = rgbaWithScale(rgb, baseAlpha, light.alphaScale);
          ctx.strokeStyle = strokeCol;
          ctx.lineWidth = Math.max(1, lineW * light.widthScale);
          if (glowBlur > 0 && light.glowScale > 0.05) {
            ctx.shadowColor = strokeCol;
            ctx.shadowBlur = glowBlur * light.glowScale;
          }
          ctx.stroke();
          ctx.restore();
        }

        resize();
        window.addEventListener('resize', resize);

        function frame() {
          t += 0.028;
          ctx.clearRect(0, 0, w, h);

          const density = getDensitySettings();
          const light = getLightSettings();

          const bgGrad = ctx.createLinearGradient(0, 0, 0, h);
          bgGrad.addColorStop(0, '#020712');
          bgGrad.addColorStop(0.5, '#04142b');
          bgGrad.addColorStop(1, '#020610');
          ctx.fillStyle = bgGrad;
          ctx.fillRect(0, 0, w, h);

          // Cyber-Grid scaled by Wave Density & Ribbon Light
          ctx.strokeStyle = rgbaWithScale('103, 232, 249', 0.08, light.alphaScale);
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let gx = 0; gx < w; gx += density.gridStep) {
            ctx.moveTo(gx, 0);
            ctx.lineTo(gx, h);
          }
          for (let gy = 0; gy < h; gy += density.gridStep) {
            ctx.moveTo(0, gy);
            ctx.lineTo(w, gy);
          }
          ctx.stroke();

          // Layered energy ribbons
          const midY = h * 0.54;
          const ribbons = [
            { y: midY,       amp: 118 * density.ampScale, freq: 0.0110, spd:  3.0, ph: 2.2, rgb: '125, 211, 252', a: 0.96, lw: 3.4, gl: 28 },
            { y: midY + 55,  amp:  88 * density.ampScale, freq: 0.0095, spd: -2.2, ph: 3.3, rgb: '52, 211, 153',  a: 0.80, lw: 2.8, gl: 22 },
            { y: midY - 55,  amp:  88 * density.ampScale, freq: 0.0090, spd:  2.4, ph: 1.1, rgb: '103, 232, 249', a: 0.80, lw: 2.8, gl: 22 },
            { y: midY + 110, amp:  64 * density.ampScale, freq: 0.0075, spd: -1.7, ph: 4.4, rgb: '192, 132, 252', a: 0.62, lw: 2.2, gl: 16 },
            { y: midY - 110, amp:  62 * density.ampScale, freq: 0.0070, spd:  1.9, ph: 0.0, rgb: '34, 211, 238',  a: 0.60, lw: 2.2, gl: 16 },
            { y: midY + 155, amp:  46 * density.ampScale, freq: 0.0062, spd:  1.4, ph: 5.1, rgb: '56, 189, 248',  a: 0.48, lw: 1.8, gl: 12 },
          ];

          for (let i = 0; i < Math.min(density.ribbonCount, ribbons.length); i++) {
            const rb = ribbons[i];
            drawWaveRibbon(rb.y, rb.amp, rb.freq, rb.spd, rb.ph, rb.rgb, rb.a, rb.lw, rb.gl, light);
          }

          // Traveling pulse sparks along the main wave
          const mainAmp = ribbons[0].amp;
          for (let s = 0; s < density.sparkCount; s++) {
            const prog = ((t * 0.16 + s / Math.max(1, density.sparkCount)) % 1 + 1) % 1;
            const sx = prog * w;
            const env = Math.pow(Math.sin(prog * Math.PI), 0.52);
            const surge = 1 + 0.42 * Math.sin(prog * 8 - t * 3.0 * 1.7);
            const sy =
              midY +
              Math.sin(sx * 0.0110 + t * 3.0 + 2.2) * mainAmp * env * surge +
              Math.cos(sx * 0.0242 - t * 4.2 + 2.2) * (mainAmp * 0.42) * env;

            ctx.save();
            const isCyan = s % 2 === 0;
            const sparkCol = rgbaWithScale(isCyan ? '165, 243, 252' : '110, 231, 183', 0.95, Math.max(0.45, light.alphaScale));
            ctx.fillStyle = sparkCol;
            ctx.shadowColor = sparkCol;
            ctx.shadowBlur = 18 * light.glowScale;
            ctx.beginPath();
            ctx.arc(sx, sy, Math.max(2.2, 3.4 * light.widthScale), 0, Math.PI * 2);
            ctx.fill();
            ctx.restore();
          }

          requestAnimationFrame(frame);
        }

        requestAnimationFrame(frame);
      })();

      poll();
      setInterval(poll, 1500);
    })();
  </script>
</body>
</html>
