#!/usr/bin/env python3
"""
J.A.R.V.I.S. Live Trading Terminal — Crypto (/), Nifty 50 (/nifty) & Portfolio (/portfolio)
-------------------------------------------------------------------------------------------
- "/"          : Top 10 Crypto Futures 2-Second Live Scalping Terminal (VWAP + 9/21 EMA + 14-RSI) + 24/7 TG Alerts
- "/nifty"     : Dedicated Nifty 50 Live RSI Screener (5m to 1Y) + Dual Render TG Alerts
- "/portfolio" : Unified Live Portfolio, Capital Allocation, Realized P&L & Win Rate Tracker
- Transparent Glass Tile HUD + Holographic Arc Reactor Canvas Simulation
- Dual Telegram Bot / Group / Recipient Support (TG_BOT_TOKEN + TG_CHAT_ID & TG_BOT_TOKEN_2 + TG_CHAT_ID_2)
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
TG_BOT_TOKEN_2 = os.getenv("TG_BOT_TOKEN_2", "").strip()
TG_CHAT_ID_2 = os.getenv("TG_CHAT_ID_2", "").strip()

CONFIG = {
    "timeframe": os.getenv("TIMEFRAME", "15m").strip() or "15m",
    "long_th": float(os.getenv("LONG_RSI_TH", "35")),
    "short_th": float(os.getenv("SHORT_RSI_TH", "70")),
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
SESSION.headers.update({"User-Agent": "JARVIS-Crypto-Nifty-Portfolio/8.0"})

STATE_LOCK = threading.Lock()
RESCAN_EVENT = threading.Event()
FORCE_HISTORY_SYNC = threading.Event()
NIFTY_COOLDOWNS = {}

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
# 3. FORMATTING & TECHNICAL MATH (RSI + DAILY VWAP + 9/21 EMA)
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


def compute_ema_base(closes: list[float], period: int):
    if not closes:
        return None
    if len(closes) < period:
        return sum(closes) / len(closes)
    k = 2.0 / (period + 1)
    ema = sum(closes[:period]) / period
    for px in closes[period:]:
        ema = px * k + ema * (1.0 - k)
    return ema


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


# =============================================================================
# 4. 4-SOURCE CRYPTO CANDLE & 2-SECOND BATCH TICKER FETCHERS
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
        timeout=5,
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
        timeout=5,
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
# 5. SIGNAL LOGIC (RSI + VWAP + FUNDING + 9/21 EMA SCALPING) & DUAL TG
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


def evaluate_scalp_setup(price: float, rsi: float, vwap: float, ema9: float, ema21: float) -> dict:
    """
    Intraday & Scalping (5m - 15m) Pullback Engine:
    - Trend Filter: Price vs Session VWAP & 9 EMA vs 21 EMA
    - Pullback Zone: Price within +-0.40% of 21 EMA or Session VWAP
    - Momentum Filter: RSI(14) > 45 for Bullish pullback, RSI(14) < 55 for Bearish rejection
    """
    if None in (price, rsi, vwap, ema9, ema21) or price <= 0:
        return {"code": "WAIT", "label": "SYNCING...", "trend": "NEUTRAL"}

    bull_trend = (price > vwap) and (ema9 > ema21)
    bear_trend = (price < vwap) and (ema9 < ema21)

    dist_ema21_pct = abs(price - ema21) / price * 100.0
    dist_vwap_pct = abs(price - vwap) / price * 100.0
    near_zone = (dist_ema21_pct <= 0.40) or (dist_vwap_pct <= 0.40)

    if bull_trend and near_zone and (44.0 <= rsi <= 68.0):
        return {"code": "SCALP_LONG", "label": "LONG • PULLBACK", "trend": "BULL"}
    if bear_trend and near_zone and (32.0 <= rsi <= 56.0):
        return {"code": "SCALP_SHORT", "label": "SHORT • PULLBACK", "trend": "BEAR"}
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
        return False, "Missing TG_BOT_TOKEN / TG_CHAT_ID in Render Environment"

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
        f"⚡ <b>Scalp Setup:</b> {scalp_lbl}\n"
        f"📊 <b>RSI (14):</b> {rsi:.2f}\n"
        f"💵 <b>Mark Price:</b> ${fmt_price(price)}\n"
        f"📈 <b>Daily VWAP:</b> ${fmt_price(vwap)} ({vwap_pos} {fmt_pct(vwap_diff)})\n"
        f"〰️ <b>9 / 21 EMA:</b> ${fmt_price(ema9)} / ${fmt_price(ema21)}\n"
        f"💰 <b>8h Funding:</b> {fmt_funding(funding)}\n"
        f"🎯 <b>Target (2π ±6.28%):</b> ${fmt_price(tp)}\n"
        f"🔁 <b>Next Avg (π ±3.14%):</b> ${fmt_price(avg)}\n"
        f"🛑 <b>Hard SL (±2.0%):</b> ${fmt_price(hard_sl)} | <b>Split (±4.8%):</b> ${fmt_price(split_sl)}\n"
        f"📡 <b>Feed:</b> {source}\n"
        f"⏰ <b>Time:</b> {now_ist_str()}"
    )


# =============================================================================
# 6. 2-SECOND 24/7 CRYPTO BACKGROUND SCANNER THREAD
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
        base_ema9 = compute_ema_base(closed_closes, 9)
        base_ema21 = compute_ema_base(closed_closes, 21)
        day_start = get_utc_day_start_ms(cur["t"])
        vwap_base_pv, vwap_base_vol = compute_vwap_base(closed, day_start)
        return item["coin"], {
            "loaded": True,
            "source": source,
            "prev_close": prev_close,
            "base_gain": base_gain,
            "base_loss": base_loss,
            "base_ema9": base_ema9,
            "base_ema21": base_ema21,
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
            funding = c.get("funding")

            sig = evaluate_signal(price, rsi, vwap, funding, cfg)
            scalp = evaluate_scalp_setup(price, rsi, vwap, ema9, ema21)
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
                "funding": funding,
                "rsi": rsi,
                "signal": sig,
                "source": feed_name,
            })

            # Trigger Telegram alert on either RSI extreme OR 9/21 EMA + VWAP Scalping Pullback
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
# 7. EMBEDDED HTML PAGES ("/", "/nifty", "/portfolio")
# =============================================================================
PORTFOLIO_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>J.A.R.V.I.S. Live Portfolio &amp; P&amp;L Terminal — Crypto + Nifty 50</title>
  <style>
    :root {
      --bg-main: #030914;
      --bg-header: rgba(5, 13, 27, 0.72);
      --bg-card: rgba(8, 18, 36, 0.34);
      --bg-card-hover: rgba(14, 116, 144, 0.22);
      --bg-input: rgba(6, 15, 30, 0.75);
      --border: rgba(56, 189, 248, 0.22);
      --border-light: rgba(103, 232, 249, 0.35);
      --text-main: #f1f5f9;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --green: #10b981;
      --green-bg: rgba(16, 185, 129, 0.16);
      --green-border: rgba(16, 185, 129, 0.42);
      --red: #f43f5e;
      --red-bg: rgba(244, 63, 94, 0.16);
      --red-border: rgba(244, 63, 94, 0.42);
      --amber: #f59e0b;
      --amber-bg: rgba(245, 158, 11, 0.16);
      --amber-border: rgba(245, 158, 11, 0.38);
      --cyan: #06b6d4;
      --cyan-light: #67e8f9;
      --cyan-bg: rgba(6, 182, 212, 0.16);
      --cyan-border: rgba(6, 182, 212, 0.38);
      --purple: #a855f7;
      --purple-light: #d8b4fe;
      --purple-bg: rgba(168, 85, 247, 0.16);
      --purple-border: rgba(168, 85, 247, 0.42);
      --radius: 10px;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, Helvetica, Arial, sans-serif;
      --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }

    * { box-sizing: border-box; margin: 0; padding: 0; }

    html, body {
      background-color: #030914 !important;
      background-image:
        radial-gradient(circle at 50% 22%, rgba(0, 212, 255, 0.11), transparent 48%),
        radial-gradient(circle at 18% 82%, rgba(16, 185, 129, 0.08), transparent 45%),
        radial-gradient(circle at 85% 78%, rgba(56, 189, 248, 0.09), transparent 45%) !important;
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

    @keyframes pulseDot {
      0%, 100% { opacity: 1; transform: scale(1); }
      50% { opacity: 0.4; transform: scale(0.85); }
    }
    .pulse-dot { animation: pulseDot 1.4s infinite ease-in-out; }

    #sciFiBgCanvas {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
      opacity: 0.92;
    }

    .sci-fi-vignette {
      position: fixed;
      inset: 0;
      z-index: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at 50% 35%, transparent 38%, rgba(2, 6, 15, 0.72) 100%),
        repeating-linear-gradient(
          0deg,
          rgba(56, 189, 248, 0.022) 0px,
          rgba(56, 189, 248, 0.022) 1px,
          transparent 1px,
          transparent 3px
        );
    }

    .header, .main, .toast-wrap {
      position: relative;
      z-index: 2;
    }

    .header {
      position: sticky;
      top: 0;
      z-index: 40;
      background: rgba(5, 13, 27, 0.62) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.24) !important;
      backdrop-filter: blur(14px) saturate(140%);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45), 0 0 24px rgba(6, 182, 212, 0.08);
    }

    .card, .kpi-card, .control-group, .live-bar {
      background: rgba(8, 18, 36, 0.34) !important;
      border-color: rgba(56, 189, 248, 0.22) !important;
      backdrop-filter: blur(10px) saturate(135%);
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.14),
        0 8px 28px rgba(2, 8, 20, 0.38);
    }

    .kpi-card:hover, .card:hover {
      border-color: rgba(103, 232, 249, 0.45) !important;
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.22),
        0 0 24px rgba(6, 182, 212, 0.14);
    }

    thead th {
      background: rgba(6, 15, 30, 0.72) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.25) !important;
      color: #7dd3fc !important;
    }

    tbody tr {
      background: rgba(8, 18, 36, 0.18);
      border-bottom: 1px solid rgba(56, 189, 248, 0.11) !important;
      transition: background-color 0.12s;
    }

    tbody tr:hover {
      background: rgba(14, 116, 144, 0.22) !important;
    }

    tbody tr.row-profit { background: rgba(16, 185, 129, 0.10) !important; }
    tbody tr.row-loss   { background: rgba(244, 63, 94, 0.10) !important; }

    .container {
      max-width: 1680px;
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
      display: flex;
      align-items: center;
      justify-content: center;
      color: #04131c;
      font-weight: 800;
      font-size: 13px;
      flex-shrink: 0;
      position: relative;
      overflow: hidden;
      background: linear-gradient(135deg, rgba(6, 182, 212, 0.35), rgba(16, 185, 129, 0.28)) !important;
      border: 1px solid rgba(103, 232, 249, 0.75) !important;
      box-shadow: 0 0 22px rgba(6, 182, 212, 0.55), inset 0 0 12px rgba(103, 232, 249, 0.35) !important;
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

    .pill-cyan {
      background: var(--cyan-bg);
      color: var(--cyan-light);
      border-color: var(--cyan-border);
    }

    .pill-purple {
      background: var(--purple-bg);
      color: var(--purple-light);
      border-color: var(--purple-border);
      font-size: 10px;
    }

    .live-bar {
      display: flex;
      flex-wrap: wrap;
      align-items: center;
      gap: 8px;
      border: 1px solid var(--border);
      border-radius: 999px;
      padding: 5px 12px;
      font-size: 12px;
    }

    .status-dot {
      width: 8px;
      height: 8px;
      border-radius: 50%;
      background: var(--green);
      box-shadow: 0 0 8px rgba(16, 185, 129, 0.75);
      flex-shrink: 0;
    }

    .live-sep { color: var(--border-light); user-select: none; }

    select, input[type="number"], input[type="text"] {
      background: var(--bg-input);
      color: var(--text-main);
      border: 1px solid var(--border-light);
      border-radius: 6px;
      padding: 6px 9px;
      font-size: 12px;
      outline: none;
      transition: border-color 0.15s;
    }
    select:focus, input:focus { border-color: var(--cyan); }

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
      background: rgba(15, 23, 42, 0.65);
      color: var(--text-main);
      transition: all 0.15s;
      white-space: nowrap;
      text-decoration: none;
    }
    .btn:hover { background: rgba(14, 116, 144, 0.35); border-color: #67e8f9; }
    .btn:active { transform: scale(0.97); }

    .btn-primary {
      background: linear-gradient(135deg, #10b981, #06b6d4);
      color: #04131c;
      border: none;
      font-weight: 700;
    }
    .btn-primary:hover { filter: brightness(1.08); }

    .btn-long {
      padding: 5px 10px;
      font-size: 12px;
      background: var(--green-bg);
      color: #34d399;
      border-color: var(--green-border);
      border-radius: 7px;
    }
    .btn-long:hover { background: rgba(16, 185, 129, 0.25); }

    .btn-short {
      padding: 5px 10px;
      font-size: 12px;
      background: var(--red-bg);
      color: #fb7185;
      border-color: var(--red-border);
      border-radius: 7px;
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

    .main { padding: 18px 0 40px; }

    .kpi-grid {
      display: grid;
      grid-template-columns: repeat(6, minmax(0, 1fr));
      gap: 12px;
      margin-bottom: 18px;
    }

    .kpi-card {
      border: 1px solid var(--border);
      border-radius: var(--radius);
      padding: 13px 14px;
      display: flex;
      flex-direction: column;
      justify-content: space-between;
      transition: all 0.15s;
    }

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
      font-size: 19px;
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

    .analytics-grid {
      display: grid;
      grid-template-columns: 1.15fr 1fr;
      gap: 14px;
      margin-bottom: 16px;
    }

    .panel-box {
      padding: 15px 16px;
    }

    .panel-title {
      display: flex;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 12px;
      font-size: 13px;
      font-weight: 700;
    }

    .alloc-bar {
      height: 12px;
      width: 100%;
      background: rgba(15, 23, 42, 0.85);
      border: 1px solid var(--border);
      border-radius: 99px;
      overflow: hidden;
      display: flex;
      margin-bottom: 10px;
    }
    .alloc-seg-crypto { background: linear-gradient(90deg, #06b6d4, #3b82f6); height: 100%; transition: width 0.35s ease; }
    .alloc-seg-nifty  { background: linear-gradient(90deg, #10b981, #34d399); height: 100%; transition: width 0.35s ease; }

    .alloc-legend {
      display: flex;
      flex-wrap: wrap;
      justify-content: space-between;
      gap: 10px;
      font-size: 12px;
    }

    .risk-bars {
      display: flex;
      flex-direction: column;
      gap: 8px;
    }
    .risk-row {
      display: flex;
      align-items: center;
      justify-content: space-between;
      gap: 10px;
      font-size: 12px;
    }
    .risk-track {
      flex: 1;
      height: 7px;
      background: rgba(15, 23, 42, 0.8);
      border-radius: 99px;
      overflow: hidden;
    }
    .risk-fill {
      height: 100%;
      border-radius: 99px;
      transition: width 0.3s ease;
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
    .tab-btn:hover { color: #fff; background: rgba(56, 189, 248, 0.08); }
    .tab-btn.active {
      background: rgba(14, 116, 144, 0.28);
      color: #fff;
      border-color: var(--border-light);
    }

    .tab-badge {
      padding: 1px 7px;
      border-radius: 99px;
      font-size: 11px;
      font-weight: 700;
      background: rgba(6, 182, 212, 0.24);
      color: var(--cyan-light);
    }

    .card {
      border: 1px solid var(--border);
      border-radius: var(--radius);
      overflow: hidden;
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
      grid-template-columns: repeat(9, minmax(0, 1fr));
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
    .field input, .field select { width: 100%; padding: 7px 8px; }

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
      font-size: 11px;
      font-weight: 600;
      text-transform: uppercase;
      letter-spacing: 0.04em;
      padding: 11px 12px;
      user-select: none;
    }

    tbody td { padding: 10px 12px; vertical-align: middle; }

    .badge {
      display: inline-block;
      padding: 3px 8px;
      border-radius: 6px;
      font-size: 11px;
      font-weight: 700;
      border: 1px solid transparent;
    }
    .badge-long    { background: var(--green-bg); color: var(--green); border-color: var(--green-border); }
    .badge-short   { background: var(--red-bg); color: var(--red); border-color: var(--red-border); }
    .badge-wait    { background: var(--amber-bg); color: var(--amber); border-color: var(--amber-border); }
    .badge-be      { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .badge-crypto  { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .badge-nifty   { background: var(--purple-bg); color: var(--purple-light); border-color: var(--purple-border); }
    .badge-neutral { background: rgba(30, 41, 59, 0.65); color: #cbd5e1; border-color: #334155; }

    .empty-state {
      padding: 36px 20px;
      text-align: center;
      color: var(--text-muted);
      font-size: 13px;
    }

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
      background: rgba(8, 18, 36, 0.88);
      backdrop-filter: blur(10px);
      border: 1px solid var(--border-light);
      color: var(--text-main);
      box-shadow: 0 10px 25px rgba(0,0,0,0.45);
      animation: fadeSlide 0.25s ease-out;
    }
    .toast.long  { border-color: var(--green-border); color: #6ee7b7; background: rgba(6, 40, 30, 0.88); }
    .toast.short { border-color: var(--red-border); color: #fda4af; background: rgba(45, 10, 21, 0.88); }
    .toast.error { border-color: var(--red-border); color: #fecdd3; background: rgba(45, 10, 21, 0.88); }

    @keyframes fadeSlide {
      from { opacity: 0; transform: translateY(10px); }
      to { opacity: 1; transform: translateY(0); }
    }

    @media (max-width: 1360px) {
      .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
      .form-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); }
    }
    @media (max-width: 900px) {
      .analytics-grid { grid-template-columns: 1fr; }
    }
    @media (max-width: 640px) {
      .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
      .form-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); }
    }
  </style>
</head>
<body>
  <canvas id="sciFiBgCanvas"></canvas>
  <div class="sci-fi-vignette"></div>

  <header class="header">
    <div class="container">
      <div class="header-inner">
        <div class="brand">
          <div class="brand-icon" title="J.A.R.V.I.S. Portfolio & P&L Engine">
            <svg width="26" height="26" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg" aria-hidden="true">
              <defs>
                <linearGradient id="sciGradPf" x1="0" y1="0" x2="36" y2="36" gradientUnits="userSpaceOnUse">
                  <stop offset="0%" stop-color="#04131c"/>
                  <stop offset="100%" stop-color="#022c22"/>
                </linearGradient>
              </defs>
              <polygon points="18,2 32,10 32,26 18,34 4,26 4,10" fill="url(#sciGradPf)" stroke="#67e8f9" stroke-width="1.5"/>
              <circle cx="18" cy="18" r="10.5" stroke="#34d399" stroke-width="1.2" stroke-dasharray="4 3"/>
              <path d="M9 21L14 15L18.5 19.5L27 11" stroke="#67e8f9" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/>
              <circle cx="27" cy="11" r="2.5" fill="#10b981"/>
              <circle cx="14" cy="15" r="1.8" fill="#67e8f9"/>
            </svg>
          </div>
          <div>
            <div class="brand-title">
              <span>J.A.R.V.I.S. Unified Live Portfolio &amp; P&amp;L</span>
              <span class="pill pill-cyan mono">CRYPTO + NIFTY 50</span>
              <span class="pill pill-purple mono">REAL-TIME SYNC</span>
              <a href="/" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--cyan-border); color: var(--cyan-light);">🪙 Crypto Futures</a>
              <a href="/nifty" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--amber-border); color: #fcd34d;">🇮🇳 Nifty 50 NSE</a>
            </div>
            <div class="brand-sub">Combined Crypto Futures (USDT) &amp; Nifty 50 Equity (INR) Live Positions • Realized P&amp;L • Hard SL &amp; Breakeven Tracker</div>
          </div>
        </div>

        <div class="live-bar">
          <span class="status-dot pulse-dot"></span>
          <strong class="mono text-cyan">PORTFOLIO LIVE</strong>
          <span class="live-sep">|</span>
          <span class="text-muted">USD/INR:</span>
          <input id="fxRateInput" type="number" step="0.1" min="50" max="150" value="84.5" class="mono" style="width: 64px; padding: 2px 6px; text-align: center; font-weight: 700;" title="USD to INR conversion rate for unified portfolio summary" />
          <span class="live-sep">|</span>
          <span id="liveClock" class="mono text-muted">--:--:-- IST</span>
        </div>
      </div>
    </div>
  </header>

  <main class="main container">
    <section class="kpi-grid">
      <div class="kpi-card">
        <div class="kpi-top">
          <span>Crypto Margin (USDT)</span>
          <span class="text-cyan mono">Futures</span>
        </div>
        <div id="kpiCryptoCap" class="kpi-val mono">$0.00</div>
        <div id="kpiCryptoPnl" class="kpi-sub mono">Unrealized: $0.00 (0.00%)</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Nifty 50 Capital (INR)</span>
          <span class="text-purple mono">NSE EQ</span>
        </div>
        <div id="kpiNiftyCap" class="kpi-val mono">₹0.00</div>
        <div id="kpiNiftyPnl" class="kpi-sub mono">Unrealized: ₹0.00 (0.00%)</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Net Combined Value</span>
          <span class="text-cyan mono">In INR</span>
        </div>
        <div id="kpiTotalInr" class="kpi-val mono">₹0.00</div>
        <div id="kpiTotalUnrealizedInr" class="kpi-sub mono">Open P&amp;L: ₹0.00</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Realized Closed P&amp;L</span>
          <span class="text-green mono">Booked</span>
        </div>
        <div id="kpiRealizedInr" class="kpi-val mono">₹0.00</div>
        <div id="kpiClosedCount" class="kpi-sub mono">0 closed trades</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Win Rate</span>
          <span class="text-amber mono">Accuracy</span>
        </div>
        <div id="kpiWinRate" class="kpi-val mono">0.0%</div>
        <div id="kpiWinLossSplit" class="kpi-sub mono">0 Wins / 0 Losses</div>
      </div>

      <div class="kpi-card">
        <div class="kpi-top">
          <span>Active Positions</span>
          <span class="text-cyan mono">Open</span>
        </div>
        <div id="kpiOpenCount" class="kpi-val mono">0</div>
        <div id="kpiRiskAlert" class="kpi-sub mono">All positions healthy</div>
      </div>
    </section>

    <section class="analytics-grid">
      <div class="card panel-box">
        <div class="panel-title">
          <span>Capital Allocation (Crypto Futures vs Nifty 50 Equity)</span>
          <span id="allocTotalTag" class="mono text-cyan">₹0.00 Total</span>
        </div>
        <div class="alloc-bar">
          <div id="allocCryptoBar" class="alloc-seg-crypto" style="width: 50%;"></div>
          <div id="allocNiftyBar" class="alloc-seg-nifty" style="width: 50%;"></div>
        </div>
        <div class="alloc-legend">
          <span><strong class="text-cyan">● Crypto Futures:</strong> <span id="allocCryptoTxt" class="mono">50.0% (₹0)</span></span>
          <span><strong class="text-green">● Nifty 50 NSE:</strong> <span id="allocNiftyTxt" class="mono">50.0% (₹0)</span></span>
        </div>
      </div>

      <div class="card panel-box">
        <div class="panel-title">
          <span>Live Position Health &amp; Target Progress</span>
          <span class="mono text-muted" style="font-size: 11px;">TP ±6.28% • Hard SL ±2.0% / ±4.8%</span>
        </div>
        <div class="risk-bars">
          <div class="risk-row">
            <span class="text-green" style="width: 115px;">In Profit / TP Zone</span>
            <div class="risk-track"><div id="barInProfit" class="risk-fill" style="width: 0%; background: var(--green);"></div></div>
            <span id="cntInProfit" class="mono" style="width: 32px; text-align: right;">0</span>
          </div>
          <div class="risk-row">
            <span class="text-amber" style="width: 115px;">Neutral / Hold</span>
            <div class="risk-track"><div id="barInHold" class="risk-fill" style="width: 0%; background: var(--cyan);"></div></div>
            <span id="cntInHold" class="mono" style="width: 32px; text-align: right;">0</span>
          </div>
          <div class="risk-row">
            <span class="text-red" style="width: 115px;">Drawdown / SL Risk</span>
            <div class="risk-track"><div id="barInLoss" class="risk-fill" style="width: 0%; background: var(--red);"></div></div>
            <span id="cntInLoss" class="mono" style="width: 32px; text-align: right;">0</span>
          </div>
        </div>
      </div>
    </section>

    <div class="tabs-row">
      <div class="tabs-list">
        <button type="button" data-tab="open" class="tab-btn active">
          <span>Open Live Positions</span>
          <span id="tabOpenBadge" class="tab-badge mono">0</span>
        </button>
        <button type="button" data-tab="closed" class="tab-btn">
          <span>Closed History &amp; Realized P&amp;L</span>
          <span id="tabClosedBadge" class="tab-badge mono">0</span>
        </button>
      </div>

      <div style="display: flex; flex-wrap: wrap; gap: 8px;">
        <select id="marketFilter" style="padding: 6px 10px;">
          <option value="ALL">All Markets (Crypto + Nifty)</option>
          <option value="CRYPTO">Crypto Futures Only</option>
          <option value="NIFTY">Nifty 50 Equity Only</option>
        </select>
        <button id="exportAllCsvBtn" type="button" class="btn btn-long">⬇ Export Full CSV</button>
      </div>
    </div>

    <form id="portfolioForm" class="card form-card">
      <div class="form-header">
        <strong>+ Add New Position to Portfolio (Auto-syncs with Crypto &amp; Nifty 50 Trade Logs)</strong>
        <div style="display: flex; align-items: center; gap: 10px;">
          <span id="formPreviewTxt" class="mono text-muted" style="font-size: 12px;">Exposure: $500.00</span>
          <button id="fillLiveBtn" type="button" class="btn" style="padding: 4px 9px; font-size: 11px;">Fill Live Price</button>
        </div>
      </div>

      <div class="form-grid">
        <div class="field">
          <label for="pMarket">Market</label>
          <select id="pMarket" required>
            <option value="CRYPTO">Crypto Futures (USDT)</option>
            <option value="NIFTY">Nifty 50 Equity (INR)</option>
          </select>
        </div>
        <div class="field">
          <label for="pSymbol">Asset / Symbol</label>
          <select id="pSymbol" required></select>
        </div>
        <div class="field">
          <label for="pSide">Side</label>
          <select id="pSide" required>
            <option value="LONG">LONG</option>
            <option value="SHORT">SHORT</option>
          </select>
        </div>
        <div class="field">
          <label for="pMode">SL Mode</label>
          <select id="pMode">
            <option value="single">Single (-2.0% SL)</option>
            <option value="split">Split (-4.8% SL)</option>
          </select>
        </div>
        <div class="field">
          <label for="pEntry">Entry Price</label>
          <input id="pEntry" type="number" step="any" min="0" required placeholder="0.00" class="mono" />
        </div>
        <div class="field">
          <label id="lblCapital" for="pCapital">Margin (USDT)</label>
          <input id="pCapital" type="number" step="any" min="1" required value="100" class="mono" />
        </div>
        <div class="field">
          <label for="pLev">Leverage (Crypto)</label>
          <input id="pLev" type="number" step="1" min="1" max="50" value="5" class="mono" />
        </div>
        <div class="field">
          <label for="pRsi">Entry RSI</label>
          <input id="pRsi" type="number" step="0.01" min="0" max="100" placeholder="32.50" class="mono" />
        </div>
        <div class="field">
          <button type="submit" class="btn btn-primary" style="width: 100%; padding: 8px;">+ Add Position</button>
        </div>
      </div>
    </form>

    <section id="tab-open" class="tab-panel">
      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Market</th>
                <th>Symbol</th>
                <th class="text-center">Side / Lev</th>
                <th class="text-right">Entry Price</th>
                <th class="text-right">Capital / Margin</th>
                <th class="text-right">Live Price</th>
                <th class="text-right">Unrealized P&amp;L</th>
                <th class="text-right">ROE / Return %</th>
                <th class="text-right">Target (±6.28%)</th>
                <th class="text-right">Hard SL / Avg</th>
                <th class="text-center">Status</th>
                <th class="text-center">Actions</th>
              </tr>
            </thead>
            <tbody id="openPositionsTbody"></tbody>
          </table>
        </div>
        <div id="openPositionsEmpty" class="empty-state hidden">No open positions yet. Add one above or click <strong>+ Long / + Short</strong> on the Crypto or Nifty 50 pages.</div>
      </div>
    </section>

    <section id="tab-closed" class="tab-panel hidden">
      <div class="card">
        <div class="table-wrap">
          <table>
            <thead>
              <tr>
                <th>Closed Time</th>
                <th>Market</th>
                <th>Symbol</th>
                <th class="text-center">Side / Lev</th>
                <th class="text-right">Entry</th>
                <th class="text-right">Exit Price</th>
                <th class="text-right">Capital</th>
                <th class="text-right">Realized P&amp;L</th>
                <th class="text-right">Return %</th>
                <th class="text-center">Del</th>
              </tr>
            </thead>
            <tbody id="closedPositionsTbody"></tbody>
          </table>
        </div>
        <div id="closedPositionsEmpty" class="empty-state hidden">No closed positions yet. Click <strong>Book / Close</strong> on any open position to lock in realized P&amp;L.</div>
      </div>
    </section>
  </main>

  <div id="toastWrap" class="toast-wrap"></div>

  <script>
    (() => {
      'use strict';

      const STORAGE_CRYPTO_TRADES = 'top10_futures_rsi_v3_trades';
      const STORAGE_NIFTY_TRADES  = 'nifty50_rsi_v4_trades';
      const STORAGE_CLOSED_TRADES = 'jarvis_portfolio_closed_v1';
      const STORAGE_FX_RATE       = 'jarvis_portfolio_fx_inr';

      const CRYPTO_LIST = [
        { coin: 'BTC',  pair: 'BTCUSDT',  name: 'Bitcoin' },
        { coin: 'ETH',  pair: 'ETHUSDT',  name: 'Ethereum' },
        { coin: 'SOL',  pair: 'SOLUSDT',  name: 'Solana' },
        { coin: 'XRP',  pair: 'XRPUSDT',  name: 'XRP' },
        { coin: 'BNB',  pair: 'BNBUSDT',  name: 'BNB' },
        { coin: 'DOGE', pair: 'DOGEUSDT', name: 'Dogecoin' },
        { coin: 'SUI',  pair: 'SUIUSDT',  name: 'Sui' },
        { coin: 'ADA',  pair: 'ADAUSDT',  name: 'Cardano' },
        { coin: 'AVAX', pair: 'AVAXUSDT', name: 'Avalanche' },
        { coin: 'LINK', pair: 'LINKUSDT', name: 'Chainlink' }
      ];

      const NIFTY_SYMBOLS = [
        'RELIANCE','HDFCBANK','TCS','BHARTIARTL','ICICIBANK','SBIN','INFY','BAJFINANCE','HINDUNILVR','ITC',
        'LT','HCLTECH','KOTAKBANK','SUNPHARMA','MARUTI','M&M','AXISBANK','ULTRACEMCO','NTPC','BAJAJFINSV',
        'TITAN','ADANIPORTS','ONGC','BEL','ADANIENT','POWERGRID','WIPRO','JSWSTEEL','TATAMOTORS','COALINDIA',
        'ASIANPAINT','BAJAJ-AUTO','NESTLEIND','TRENT','GRASIM','EICHERMOT','SBILIFE','TECHM','HINDALCO','TATASTEEL',
        'HDFCLIFE','SHRIRAMFIN','CIPLA','APOLLOHOSP','DRREDDY','TATACONSUM','HEROMOTOCO','INDUSINDBK','JIOFIN','BRITANNIA'
      ];

      const TV_SYM_MAP = { 'M&M': 'M_M', 'BAJAJ-AUTO': 'BAJAJ_AUTO' };

      const state = {
        cryptoTrades: [],
        niftyTrades: [],
        closedTrades: [],
        cryptoPrices: new Map(),
        niftyPrices: new Map(),
        fxRate: 84.5,
        activeTab: 'open',
        marketFilter: 'ALL'
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

      function fmtINR(v) {
        if (v === null || v === undefined || isNaN(v)) return '--';
        return (v < 0 ? '-₹' : '₹') + Math.abs(v).toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      }

      function fmtPct(v) {
        if (v === null || v === undefined || isNaN(v)) return '--';
        return (v > 0 ? '+' : '') + v.toFixed(2) + '%';
      }

      function showToast(msg, type = 'info') {
        const wrap = document.getElementById('toastWrap');
        const el = document.createElement('div');
        el.className = `toast ${type}`;
        el.textContent = msg;
        wrap.appendChild(el);
        setTimeout(() => el.remove(), 3400);
      }

      function loadAllStorage() {
        try {
          const c = JSON.parse(localStorage.getItem(STORAGE_CRYPTO_TRADES) || '[]');
          if (Array.isArray(c)) state.cryptoTrades = c;
          const n = JSON.parse(localStorage.getItem(STORAGE_NIFTY_TRADES) || '[]');
          if (Array.isArray(n)) state.niftyTrades = n;
          const cl = JSON.parse(localStorage.getItem(STORAGE_CLOSED_TRADES) || '[]');
          if (Array.isArray(cl)) state.closedTrades = cl;
          const fx = parseFloat(localStorage.getItem(STORAGE_FX_RATE));
          if (!isNaN(fx) && fx >= 50 && fx <= 150) {
            state.fxRate = fx;
            document.getElementById('fxRateInput').value = fx;
          }
        } catch {}
      }

      function saveCryptoTrades() {
        try { localStorage.setItem(STORAGE_CRYPTO_TRADES, JSON.stringify(state.cryptoTrades)); } catch {}
      }
      function saveNiftyTrades() {
        try { localStorage.setItem(STORAGE_NIFTY_TRADES, JSON.stringify(state.niftyTrades)); } catch {}
      }
      function saveClosedTrades() {
        try { localStorage.setItem(STORAGE_CLOSED_TRADES, JSON.stringify(state.closedTrades)); } catch {}
      }

      async function fetchLiveCryptoPrices() {
        try {
          const res = await fetch('/api/state', { cache: 'no-store' });
          if (!res.ok) return;
          const data = await res.json();
          if (Array.isArray(data.coins)) {
            data.coins.forEach(c => {
              if (c.coin && typeof c.price === 'number') {
                state.cryptoPrices.set(c.coin, c.price);
              }
            });
          }
        } catch {}
      }

      async function fetchLiveNiftyPrices() {
        if (state.niftyTrades.length === 0 && document.getElementById('pMarket').value !== 'NIFTY') return;
        try {
          const tickers = NIFTY_SYMBOLS.map(s => `NSE:${TV_SYM_MAP[s] || s}`);
          const res = await fetch('https://scanner.tradingview.com/india/scan', {
            method: 'POST',
            headers: { 'Content-Type': 'text/plain;charset=UTF-8' },
            body: JSON.stringify({ symbols: { tickers, query: { types: [] } }, columns: ['close'] })
          });
          if (!res.ok) return;
          const json = await res.json();
          if (Array.isArray(json.data)) {
            json.data.forEach(row => {
              const raw = String(row.s || '').replace('NSE:', '');
              const sym = raw === 'M_M' ? 'M&M' : raw === 'BAJAJ_AUTO' ? 'BAJAJ-AUTO' : raw;
              if (Array.isArray(row.d) && typeof row.d[0] === 'number') {
                state.niftyPrices.set(sym, row.d[0]);
              }
            });
          }
        } catch {}
      }

      function buildUnifiedPositions() {
        const rows = [];

        state.cryptoTrades.forEach(t => {
          const live = state.cryptoPrices.get(t.coin) ?? t.entry;
          const posSize = t.margin * t.lev;
          const qty = t.entry > 0 ? posSize / t.entry : 0;
          const isLong = t.side === 'LONG';
          const isSplit = t.mode === 'split';
          const pnl = isLong ? (live - t.entry) * qty : (t.entry - live) * qty;
          const roe = t.margin > 0 ? (pnl / t.margin) * 100 : 0;
          const tp = isLong ? t.entry * 1.0628 : t.entry * 0.9372;
          const slPct = isSplit ? 0.048 : 0.02;
          const hardSl = isLong ? t.entry * (1 - slPct) : t.entry * (1 + slPct);

          let status = 'HOLD';
          let stCls = 'badge-neutral';
          if ((isLong && live >= tp) || (!isLong && live <= tp)) { status = 'TP HIT'; stCls = 'badge-long'; }
          else if ((isLong && live <= hardSl) || (!isLong && live >= hardSl)) { status = 'SL RISK'; stCls = 'badge-short'; }
          else if (pnl > 0) { status = 'IN PROFIT'; stCls = 'badge-be'; }

          rows.push({
            id: t.id,
            market: 'CRYPTO',
            symbol: t.coin + 'USDT',
            rawSym: t.coin,
            side: t.side,
            lev: t.lev,
            mode: t.mode || 'single',
            entry: t.entry,
            capital: t.margin,
            live,
            pnl,
            roe,
            tp,
            slOrAvg: hardSl,
            status,
            stCls
          });
        });

        state.niftyTrades.forEach(t => {
          const live = state.niftyPrices.get(t.symbol) ?? t.entry;
          const invested = t.entry * t.qty;
          const isLong = t.side === 'LONG';
          const pnl = isLong ? (live - t.entry) * t.qty : (t.entry - live) * t.qty;
          const roe = invested > 0 ? (pnl / invested) * 100 : 0;
          const tp = isLong ? t.entry * 1.0628 : t.entry * 0.9372;
          const avg = isLong ? t.entry * 0.9686 : t.entry * 1.0314;

          let status = 'HOLD';
          let stCls = 'badge-neutral';
          if ((isLong && live >= tp) || (!isLong && live <= tp)) { status = 'TP HIT'; stCls = 'badge-long'; }
          else if ((isLong && live <= avg) || (!isLong && live >= avg)) { status = 'AVG ZONE'; stCls = 'badge-wait'; }
          else if (pnl > 0) { status = 'IN PROFIT'; stCls = 'badge-be'; }

          rows.push({
            id: t.id,
            market: 'NIFTY',
            symbol: t.symbol,
            rawSym: t.symbol,
            side: t.side,
            lev: 1,
            qty: t.qty,
            entry: t.entry,
            capital: invested,
            live,
            pnl,
            roe,
            tp,
            slOrAvg: avg,
            status,
            stCls
          });
        });

        return rows;
      }

      function renderAll() {
        const allOpen = buildUnifiedPositions();
        const cryptoOpen = allOpen.filter(r => r.market === 'CRYPTO');
        const niftyOpen  = allOpen.filter(r => r.market === 'NIFTY');

        const cCap = cryptoOpen.reduce((acc, r) => acc + r.capital, 0);
        const cPnl = cryptoOpen.reduce((acc, r) => acc + r.pnl, 0);
        const cRoe = cCap > 0 ? (cPnl / cCap) * 100 : 0;

        const nCap = niftyOpen.reduce((acc, r) => acc + r.capital, 0);
        const nPnl = niftyOpen.reduce((acc, r) => acc + r.pnl, 0);
        const nRoe = nCap > 0 ? (nPnl / nCap) * 100 : 0;

        const cCapInr = cCap * state.fxRate;
        const cPnlInr = cPnl * state.fxRate;
        const totalCapInr = cCapInr + nCap;
        const totalOpenPnlInr = cPnlInr + nPnl;

        let realizedInr = 0;
        let wins = 0;
        let losses = 0;
        state.closedTrades.forEach(ct => {
          const pnlInr = ct.market === 'CRYPTO' ? ct.pnl * state.fxRate : ct.pnl;
          realizedInr += pnlInr;
          if (ct.pnl >= 0) wins++;
          else losses++;
        });
        const totalClosed = wins + losses;
        const winRate = totalClosed > 0 ? (wins / totalClosed) * 100 : 0;

        document.getElementById('kpiCryptoCap').textContent = fmtUSDT(cCap);
        const cSub = document.getElementById('kpiCryptoPnl');
        cSub.textContent = `Unrealized: ${fmtUSDT(cPnl)} (${fmtPct(cRoe)})`;
        cSub.className = `kpi-sub mono ${cPnl >= 0 ? 'text-green' : 'text-red'}`;

        document.getElementById('kpiNiftyCap').textContent = fmtINR(nCap);
        const nSub = document.getElementById('kpiNiftyPnl');
        nSub.textContent = `Unrealized: ${fmtINR(nPnl)} (${fmtPct(nRoe)})`;
        nSub.className = `kpi-sub mono ${nPnl >= 0 ? 'text-green' : 'text-red'}`;

        document.getElementById('kpiTotalInr').textContent = fmtINR(totalCapInr + totalOpenPnlInr);
        const uSub = document.getElementById('kpiTotalUnrealizedInr');
        uSub.textContent = `Open P&L: ${fmtINR(totalOpenPnlInr)}`;
        uSub.className = `kpi-sub mono ${totalOpenPnlInr >= 0 ? 'text-green' : 'text-red'}`;

        const rEl = document.getElementById('kpiRealizedInr');
        rEl.textContent = fmtINR(realizedInr);
        rEl.className = `kpi-val mono ${realizedInr > 0 ? 'text-green' : realizedInr < 0 ? 'text-red' : ''}`;
        document.getElementById('kpiClosedCount').textContent = `${totalClosed} closed trades`;

        document.getElementById('kpiWinRate').textContent = `${winRate.toFixed(1)}%`;
        document.getElementById('kpiWinLossSplit').textContent = `${wins} Wins / ${losses} Losses`;

        document.getElementById('kpiOpenCount').textContent = allOpen.length;
        const slRiskCount = allOpen.filter(r => r.status === 'SL RISK').length;
        const riskEl = document.getElementById('kpiRiskAlert');
        riskEl.textContent = slRiskCount > 0 ? `⚠ ${slRiskCount} near Hard SL` : 'All positions healthy';
        riskEl.className = `kpi-sub mono ${slRiskCount > 0 ? 'text-red' : 'text-green'}`;

        document.getElementById('allocTotalTag').textContent = `${fmtINR(totalCapInr)} Deployed`;
        const cPct = totalCapInr > 0 ? (cCapInr / totalCapInr) * 100 : 50;
        const nPct = totalCapInr > 0 ? (nCap / totalCapInr) * 100 : 50;
        document.getElementById('allocCryptoBar').style.width = `${cPct.toFixed(1)}%`;
        document.getElementById('allocNiftyBar').style.width = `${nPct.toFixed(1)}%`;
        document.getElementById('allocCryptoTxt').textContent = `${cPct.toFixed(1)}% (${fmtINR(cCapInr)})`;
        document.getElementById('allocNiftyTxt').textContent = `${nPct.toFixed(1)}% (${fmtINR(nCap)})`;

        const totalOpen = allOpen.length || 1;
        const inProfit = allOpen.filter(r => r.pnl > 0).length;
        const inLoss   = allOpen.filter(r => r.pnl < 0).length;
        const inHold   = allOpen.filter(r => r.pnl === 0).length;
        document.getElementById('barInProfit').style.width = `${(inProfit / totalOpen) * 100}%`;
        document.getElementById('barInHold').style.width   = `${(inHold / totalOpen) * 100}%`;
        document.getElementById('barInLoss').style.width   = `${(inLoss / totalOpen) * 100}%`;
        document.getElementById('cntInProfit').textContent = inProfit;
        document.getElementById('cntInHold').textContent   = inHold;
        document.getElementById('cntInLoss').textContent   = inLoss;

        document.getElementById('tabOpenBadge').textContent = allOpen.length;
        document.getElementById('tabClosedBadge').textContent = state.closedTrades.length;

        const filteredOpen = allOpen.filter(r => state.marketFilter === 'ALL' || r.market === state.marketFilter);
        const openBody = document.getElementById('openPositionsTbody');
        const openEmpty = document.getElementById('openPositionsEmpty');

        if (filteredOpen.length === 0) {
          openBody.innerHTML = '';
          openEmpty.classList.remove('hidden');
        } else {
          openEmpty.classList.add('hidden');
          openBody.innerHTML = filteredOpen.map(r => {
            const isCrypto = r.market === 'CRYPTO';
            const moneyFmt = isCrypto ? fmtUSDT : fmtINR;
            const pxPrefix = isCrypto ? '$' : '₹';
            const pnlCls = r.pnl > 0 ? 'text-green' : r.pnl < 0 ? 'text-red' : 'text-muted';
            const rowCls = r.pnl > 0 ? 'row-profit' : r.pnl < 0 ? 'row-loss' : '';
            return `
              <tr class="${rowCls}">
                <td><span class="badge ${isCrypto ? 'badge-crypto' : 'badge-nifty'}">${isCrypto ? 'CRYPTO' : 'NIFTY 50'}</span></td>
                <td><strong class="mono">${r.symbol}</strong></td>
                <td class="text-center"><span class="badge ${r.side === 'LONG' ? 'badge-long' : 'badge-short'} mono">${r.side} ${isCrypto ? r.lev + 'x' : r.qty + ' qty'}</span></td>
                <td class="text-right mono">${pxPrefix}${fmtPrice(r.entry)}</td>
                <td class="text-right mono">${moneyFmt(r.capital)}</td>
                <td class="text-right mono text-cyan" style="font-weight:700;">${pxPrefix}${fmtPrice(r.live)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${moneyFmt(r.pnl)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${fmtPct(r.roe)}</td>
                <td class="text-right mono text-green">${pxPrefix}${fmtPrice(r.tp)}</td>
                <td class="text-right mono text-red">${pxPrefix}${fmtPrice(r.slOrAvg)}</td>
                <td class="text-center"><span class="badge ${r.stCls}">${r.status}</span></td>
                <td class="text-center">
                  <button type="button" class="btn btn-long" style="padding: 3px 8px; font-size: 11px;" data-close-market="${r.market}" data-close-id="${r.id}">✓ Close</button>
                  <button type="button" class="btn btn-del" data-del-market="${r.market}" data-del-id="${r.id}">✕</button>
                </td>
              </tr>
            `;
          }).join('');
        }

        const filteredClosed = state.closedTrades.filter(r => state.marketFilter === 'ALL' || r.market === state.marketFilter);
        const closedBody = document.getElementById('closedPositionsTbody');
        const closedEmpty = document.getElementById('closedPositionsEmpty');
        if (filteredClosed.length === 0) {
          closedBody.innerHTML = '';
          closedEmpty.classList.remove('hidden');
        } else {
          closedEmpty.classList.add('hidden');
          closedBody.innerHTML = filteredClosed.map(r => {
            const isCrypto = r.market === 'CRYPTO';
            const moneyFmt = isCrypto ? fmtUSDT : fmtINR;
            const pxPrefix = isCrypto ? '$' : '₹';
            const pnlCls = r.pnl >= 0 ? 'text-green' : 'text-red';
            return `
              <tr>
                <td class="mono text-muted">${r.closedAt}</td>
                <td><span class="badge ${isCrypto ? 'badge-crypto' : 'badge-nifty'}">${isCrypto ? 'CRYPTO' : 'NIFTY 50'}</span></td>
                <td><strong class="mono">${r.symbol}</strong></td>
                <td class="text-center"><span class="badge ${r.side === 'LONG' ? 'badge-long' : 'badge-short'} mono">${r.side}</span></td>
                <td class="text-right mono">${pxPrefix}${fmtPrice(r.entry)}</td>
                <td class="text-right mono text-cyan">${pxPrefix}${fmtPrice(r.exitPrice)}</td>
                <td class="text-right mono">${moneyFmt(r.capital)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${moneyFmt(r.pnl)}</td>
                <td class="text-right mono ${pnlCls}" style="font-weight:700;">${fmtPct(r.roe)}</td>
                <td class="text-center"><button type="button" class="btn btn-del" data-del-closed="${r.id}">✕</button></td>
              </tr>
            `;
          }).join('');
        }
      }

      function populateSymbolDropdown() {
        const mkt = document.getElementById('pMarket').value;
        const symSel = document.getElementById('pSymbol');
        if (mkt === 'CRYPTO') {
          symSel.innerHTML = CRYPTO_LIST.map(c => `<option value="${c.coin}">${c.pair} — ${c.name}</option>`).join('');
          document.getElementById('lblCapital').textContent = 'Margin (USDT)';
          document.getElementById('pCapital').value = 100;
          document.getElementById('pLev').disabled = false;
          document.getElementById('pMode').disabled = false;
        } else {
          symSel.innerHTML = NIFTY_SYMBOLS.map(s => `<option value="${s}">${s} (NSE)</option>`).join('');
          document.getElementById('lblCapital').textContent = 'Quantity (Shares)';
          document.getElementById('pCapital').value = 10;
          document.getElementById('pLev').disabled = true;
          document.getElementById('pMode').disabled = true;
        }
        updateFormPreview();
      }

      function updateFormPreview() {
        const mkt = document.getElementById('pMarket').value;
        const entry = parseFloat(document.getElementById('pEntry').value) || 0;
        const cap = parseFloat(document.getElementById('pCapital').value) || 0;
        const lev = parseFloat(document.getElementById('pLev').value) || 1;
        if (mkt === 'CRYPTO') {
          document.getElementById('formPreviewTxt').textContent = `Pos Size: ${fmtUSDT(cap * lev)}`;
        } else {
          document.getElementById('formPreviewTxt').textContent = `Invested: ${fmtINR(entry * cap)}`;
        }
      }

      function exportFullCSV() {
        const openRows = buildUnifiedPositions();
        if (openRows.length === 0 && state.closedTrades.length === 0) {
          showToast('No portfolio trades to export yet.');
          return;
        }
        const headers = ['Status', 'Market', 'Symbol', 'Side', 'Entry', 'Live/Exit', 'Capital', 'PnL', 'Return %'];
        const lines = [headers.join(',')];
        openRows.forEach(r => {
          lines.push(['OPEN', r.market, r.symbol, r.side, r.entry, r.live, r.capital, r.pnl.toFixed(2), r.roe.toFixed(2) + '%'].join(','));
        });
        state.closedTrades.forEach(r => {
          lines.push(['CLOSED', r.market, r.symbol, r.side, r.entry, r.exitPrice, r.capital, r.pnl.toFixed(2), r.roe.toFixed(2) + '%'].join(','));
        });
        const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `jarvis-portfolio-${new Date().toISOString().slice(0, 10)}.csv`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(url);
        showToast('Full Portfolio CSV exported!', 'long');
      }

      function bindEvents() {
        document.querySelectorAll('.tab-btn').forEach(btn => {
          btn.addEventListener('click', () => {
            state.activeTab = btn.dataset.tab;
            document.querySelectorAll('.tab-panel').forEach(p => p.classList.add('hidden'));
            document.getElementById(`tab-${state.activeTab}`).classList.remove('hidden');
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === state.activeTab));
          });
        });

        document.getElementById('marketFilter').addEventListener('change', e => {
          state.marketFilter = e.target.value;
          renderAll();
        });

        document.getElementById('fxRateInput').addEventListener('input', e => {
          const v = parseFloat(e.target.value);
          if (!isNaN(v) && v >= 50 && v <= 150) {
            state.fxRate = v;
            try { localStorage.setItem(STORAGE_FX_RATE, String(v)); } catch {}
            renderAll();
          }
        });

        document.getElementById('pMarket').addEventListener('change', () => {
          populateSymbolDropdown();
          if (document.getElementById('pMarket').value === 'NIFTY') fetchLiveNiftyPrices();
        });

        document.getElementById('pEntry').addEventListener('input', updateFormPreview);
        document.getElementById('pCapital').addEventListener('input', updateFormPreview);
        document.getElementById('pLev').addEventListener('input', updateFormPreview);

        document.getElementById('fillLiveBtn').addEventListener('click', () => {
          const mkt = document.getElementById('pMarket').value;
          const sym = document.getElementById('pSymbol').value;
          const px = mkt === 'CRYPTO' ? state.cryptoPrices.get(sym) : state.niftyPrices.get(sym);
          if (px) {
            document.getElementById('pEntry').value = px;
            updateFormPreview();
          } else {
            showToast('Live price syncing — try again in 2 seconds.');
          }
        });

        document.getElementById('portfolioForm').addEventListener('submit', e => {
          e.preventDefault();
          const mkt = document.getElementById('pMarket').value;
          const sym = document.getElementById('pSymbol').value;
          const side = document.getElementById('pSide').value;
          const mode = document.getElementById('pMode').value;
          const entry = parseFloat(document.getElementById('pEntry').value);
          const cap = parseFloat(document.getElementById('pCapital').value);
          const lev = Math.min(50, Math.max(1, parseInt(document.getElementById('pLev').value, 10) || 1));
          const rsiVal = document.getElementById('pRsi').value;

          if (!sym || isNaN(entry) || entry <= 0 || isNaN(cap) || cap <= 0) {
            showToast('Enter valid Entry Price and Margin/Quantity.', 'error');
            return;
          }

          if (mkt === 'CRYPTO') {
            state.cryptoTrades.push({
              id: 'ft_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
              coin: sym, side, mode, entry, margin: cap, lev,
              rsi: rsiVal !== '' ? parseFloat(rsiVal) : null
            });
            saveCryptoTrades();
          } else {
            state.niftyTrades.push({
              id: 'n50_' + Date.now() + '_' + Math.random().toString(36).slice(2, 6),
              symbol: sym, side, entry, qty: Math.round(cap),
              rsi: rsiVal !== '' ? parseFloat(rsiVal) : null
            });
            saveNiftyTrades();
          }
          renderAll();
          showToast(`Added ${side} ${sym} to portfolio!`, side === 'LONG' ? 'long' : 'short');
        });

        document.addEventListener('click', e => {
          const closeBtn = e.target.closest('[data-close-id]');
          if (closeBtn) {
            const mkt = closeBtn.dataset.closeMarket;
            const id = closeBtn.dataset.closeId;
            const allOpen = buildUnifiedPositions();
            const pos = allOpen.find(x => x.market === mkt && x.id === id);
            if (!pos) return;

            state.closedTrades.unshift({
              id: 'cl_' + Date.now(),
              closedAt: new Date().toLocaleTimeString('en-IN', { hour12: false }) + ' IST',
              market: pos.market,
              symbol: pos.symbol,
              side: pos.side,
              entry: pos.entry,
              exitPrice: pos.live,
              capital: pos.capital,
              pnl: pos.pnl,
              roe: pos.roe
            });
            saveClosedTrades();

            if (mkt === 'CRYPTO') {
              state.cryptoTrades = state.cryptoTrades.filter(t => t.id !== id);
              saveCryptoTrades();
            } else {
              state.niftyTrades = state.niftyTrades.filter(t => t.id !== id);
              saveNiftyTrades();
            }
            renderAll();
            showToast(`Closed ${pos.symbol} (${fmtPct(pos.roe)})`, pos.pnl >= 0 ? 'long' : 'short');
            return;
          }

          const delBtn = e.target.closest('[data-del-id]');
          if (delBtn) {
            const mkt = delBtn.dataset.delMarket;
            const id = delBtn.dataset.delId;
            if (mkt === 'CRYPTO') {
              state.cryptoTrades = state.cryptoTrades.filter(t => t.id !== id);
              saveCryptoTrades();
            } else {
              state.niftyTrades = state.niftyTrades.filter(t => t.id !== id);
              saveNiftyTrades();
            }
            renderAll();
            showToast('Position removed.');
            return;
          }

          const delClosed = e.target.closest('[data-del-closed]');
          if (delClosed) {
            state.closedTrades = state.closedTrades.filter(t => t.id !== delClosed.dataset.delClosed);
            saveClosedTrades();
            renderAll();
          }
        });

        document.getElementById('exportAllCsvBtn').addEventListener('click', exportFullCSV);
      }

      (function initJarvisSimulation() {
        const canvas = document.getElementById('sciFiBgCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let w = 0, h = 0, dpr = Math.min(window.devicePixelRatio || 1, 2);
        let tick = 0;

        const particles = [];
        const streams = [];
        const PARTICLE_COUNT = 54;
        const STREAM_COUNT = 9;

        function resize() {
          w = window.innerWidth;
          h = window.innerHeight;
          canvas.width = Math.max(1, w) * dpr;
          canvas.height = Math.max(1, h) * dpr;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function initEntities() {
          particles.length = 0;
          for (let i = 0; i < PARTICLE_COUNT; i++) {
            particles.push({
              x: Math.random() * (w || 1440),
              y: Math.random() * (h || 900),
              vx: (Math.random() - 0.5) * 0.42,
              vy: (Math.random() - 0.5) * 0.42,
              size: 1 + Math.random() * 2,
              hue: i % 3 === 0 ? '16, 185, 129' : '56, 189, 248'
            });
          }

          streams.length = 0;
          for (let s = 0; s < STREAM_COUNT; s++) {
            streams.push({
              x: ((s + 0.5) / STREAM_COUNT) * (w || 1440),
              y: Math.random() * (h || 900),
              len: 45 + Math.random() * 95,
              speed: 1.1 + Math.random() * 1.9,
              alpha: 0.08 + Math.random() * 0.14
            });
          }
        }

        function drawArcReactor(cx, cy, radius, rot) {
          ctx.save();
          ctx.translate(cx, cy);

          const glow = ctx.createRadialGradient(0, 0, radius * 0.15, 0, 0, radius * 1.35);
          glow.addColorStop(0, 'rgba(56, 189, 248, 0.14)');
          glow.addColorStop(0.55, 'rgba(6, 182, 212, 0.05)');
          glow.addColorStop(1, 'rgba(6, 182, 212, 0)');
          ctx.fillStyle = glow;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 1.35, 0, Math.PI * 2);
          ctx.fill();

          ctx.save();
          ctx.rotate(rot * 0.65);
          const segs = 12;
          for (let i = 0; i < segs; i++) {
            const a0 = (i / segs) * Math.PI * 2 + 0.05;
            const a1 = ((i + 1) / segs) * Math.PI * 2 - 0.05;
            ctx.strokeStyle = i % 3 === 0 ? 'rgba(16, 185, 129, 0.28)' : 'rgba(103, 232, 249, 0.24)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(0, 0, radius, a0, a1);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(-rot * 1.25);
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.22)';
          ctx.lineWidth = 1.2;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.76, 0, Math.PI * 2);
          ctx.stroke();

          for (let k = 0; k < 24; k++) {
            const ang = (k / 24) * Math.PI * 2;
            const rIn = radius * (k % 2 === 0 ? 0.69 : 0.72);
            const rOut = radius * 0.76;
            ctx.strokeStyle = k % 6 === 0 ? 'rgba(110, 231, 183, 0.45)' : 'rgba(125, 211, 252, 0.26)';
            ctx.lineWidth = k % 6 === 0 ? 1.8 : 1;
            ctx.beginPath();
            ctx.moveTo(Math.cos(ang) * rIn, Math.sin(ang) * rIn);
            ctx.lineTo(Math.cos(ang) * rOut, Math.sin(ang) * rOut);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(rot * 2.4);
          const sweepGrad = ctx.createConicGradient(0, 0);
          sweepGrad.addColorStop(0, 'rgba(103, 232, 249, 0.28)');
          sweepGrad.addColorStop(0.14, 'rgba(16, 185, 129, 0.08)');
          sweepGrad.addColorStop(0.15, 'rgba(56, 189, 248, 0)');
          sweepGrad.addColorStop(1, 'rgba(56, 189, 248, 0)');
          ctx.fillStyle = sweepGrad;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.66, 0, Math.PI * 2);
          ctx.fill();
          ctx.restore();

          ctx.strokeStyle = 'rgba(125, 211, 252, 0.35)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.26, 0, Math.PI * 2);
          ctx.stroke();

          ctx.restore();
        }

        resize();
        initEntities();
        window.addEventListener('resize', () => {
          resize();
          initEntities();
        });

        function renderJarvis() {
          tick += 1;
          const rot = tick * 0.006;
          ctx.clearRect(0, 0, w, h);

          const step = 72;
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.04)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let x = 0; x < w; x += step) {
            ctx.moveTo(x, 0);
            ctx.lineTo(x, h);
          }
          for (let y = 0; y < h; y += step) {
            ctx.moveTo(0, y);
            ctx.lineTo(w, y);
          }
          ctx.stroke();

          for (let i = 0; i < streams.length; i++) {
            const st = streams[i];
            st.y += st.speed;
            if (st.y - st.len > h) {
              st.y = -20;
              st.x = ((i + 0.5) / STREAM_COUNT) * w + (Math.random() - 0.5) * 40;
            }
            const g = ctx.createLinearGradient(st.x, st.y - st.len, st.x, st.y);
            g.addColorStop(0, 'rgba(56, 189, 248, 0)');
            g.addColorStop(0.75, `rgba(56, 189, 248, ${st.alpha})`);
            g.addColorStop(1, 'rgba(110, 231, 183, 0.45)');
            ctx.strokeStyle = g;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(st.x, st.y - st.len);
            ctx.lineTo(st.x, st.y);
            ctx.stroke();
          }

          const mainR = Math.min(w, h) * 0.25;
          drawArcReactor(w * 0.5, h * 0.54, mainR, rot);
          drawArcReactor(w * 0.88, h * 0.22, Math.min(w, h) * 0.11, -rot * 1.4);

          for (let i = 0; i < particles.length; i++) {
            const p = particles[i];
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < 0) p.x = w;
            if (p.x > w) p.x = 0;
            if (p.y < 0) p.y = h;
            if (p.y > h) p.y = 0;

            for (let j = i + 1; j < particles.length; j++) {
              const q = particles[j];
              const d = Math.hypot(p.x - q.x, p.y - q.y);
              if (d < 155) {
                const a = (1 - d / 155) * 0.16;
                ctx.strokeStyle = `rgba(56, 189, 248, ${a.toFixed(3)})`;
                ctx.lineWidth = 0.75;
                ctx.beginPath();
                ctx.moveTo(p.x, p.y);
                ctx.lineTo(q.x, q.y);
                ctx.stroke();
              }
            }

            ctx.fillStyle = `rgba(${p.hue}, 0.65)`;
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
            ctx.fill();
          }

          const baseWaveY = h * 0.91;
          ctx.strokeStyle = 'rgba(103, 232, 249, 0.22)';
          ctx.lineWidth = 1.4;
          ctx.beginPath();
          for (let x = 20; x < w - 20; x += 8) {
            const env = Math.sin((x / w) * Math.PI);
            const y = baseWaveY + Math.sin(x * 0.022 + tick * 0.06) * 16 * env + Math.cos(x * 0.05 - tick * 0.04) * 5 * env;
            if (x === 20) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
          }
          ctx.stroke();

          requestAnimationFrame(renderJarvis);
        }

        requestAnimationFrame(renderJarvis);
      })();

      async function syncLoop() {
        loadAllStorage();
        await Promise.all([fetchLiveCryptoPrices(), fetchLiveNiftyPrices()]);
        document.getElementById('liveClock').textContent = new Date().toLocaleTimeString('en-IN', { hour12: false }) + ' IST';
        renderAll();
      }

      function init() {
        loadAllStorage();
        populateSymbolDropdown();
        bindEvents();
        renderAll();
        syncLoop();
        setInterval(syncLoop, 2500);
      }

      if (document.readyState === 'loading') {
        window.addEventListener('DOMContentLoaded', init);
      } else {
        init();
      }
    })();
  </script>
</body>
</html>"""

NIFTY_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Nifty 50 Live RSI Screener (5m – 1Y) — J.A.R.V.I.S. Render Terminal</title>
  <style>
    :root {
      --bg-main: #030914;
      --bg-header: rgba(5, 13, 27, 0.72);
      --bg-card: rgba(8, 18, 36, 0.34);
      --bg-card-hover: rgba(14, 116, 144, 0.22);
      --bg-input: rgba(6, 15, 30, 0.75);
      --border: rgba(56, 189, 248, 0.22);
      --border-light: rgba(103, 232, 249, 0.35);
      --text-main: #f1f5f9;
      --text-muted: #94a3b8;
      --text-dim: #64748b;
      --green: #10b981;
      --green-bg: rgba(16, 185, 129, 0.16);
      --green-border: rgba(16, 185, 129, 0.42);
      --red: #f43f5e;
      --red-bg: rgba(244, 63, 94, 0.16);
      --red-border: rgba(244, 63, 94, 0.42);
      --amber: #f59e0b;
      --amber-bg: rgba(245, 158, 11, 0.16);
      --amber-border: rgba(245, 158, 11, 0.38);
      --cyan: #06b6d4;
      --cyan-light: #67e8f9;
      --cyan-bg: rgba(6, 182, 212, 0.16);
      --cyan-border: rgba(6, 182, 212, 0.38);
      --purple-light: #d8b4fe;
      --purple-bg: rgba(168, 85, 247, 0.16);
      --purple-border: rgba(168, 85, 247, 0.42);
      --radius: 10px;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, Helvetica, Arial, sans-serif;
      --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { color: var(--text-main); font-family: var(--font-sans); font-size: 14px; line-height: 1.45; min-height: 100vh; -webkit-font-smoothing: antialiased; }
    ::-webkit-scrollbar { width: 7px; height: 7px; }
    ::-webkit-scrollbar-track { background: var(--bg-main); }
    ::-webkit-scrollbar-thumb { background: #334155; border-radius: 99px; }
    .mono { font-family: var(--font-mono); font-variant-numeric: tabular-nums; font-feature-settings: "tnum" 1; }
    .text-green { color: var(--green); } .text-red { color: var(--red); } .text-amber { color: var(--amber); }
    .text-cyan { color: var(--cyan-light); } .text-muted { color: var(--text-muted); } .text-dim { color: var(--text-dim); }
    .text-right { text-align: right; } .text-center { text-align: center; } .hidden { display: none !important; }
    @keyframes flashUp { 0% { background-color: rgba(16, 185, 129, 0.35); color: #6ee7b7; } 100% { background-color: transparent; } }
    @keyframes flashDown { 0% { background-color: rgba(244, 63, 94, 0.35); color: #fda4af; } 100% { background-color: transparent; } }
    .tick-up { animation: flashUp 0.7s ease-out; } .tick-down { animation: flashDown 0.7s ease-out; }
    @keyframes pulseDot { 0%, 100% { opacity: 1; transform: scale(1); } 50% { opacity: 0.4; transform: scale(0.85); } }
    .pulse-dot { animation: pulseDot 1.4s infinite ease-in-out; }
    @keyframes spin { from { transform: rotate(0deg); } to { transform: rotate(360deg); } }
    .spinning { display: inline-block; animation: spin 0.8s linear infinite; }
    html, body {
      background-color: #030914 !important;
      background-image:
        radial-gradient(circle at 50% 22%, rgba(0, 212, 255, 0.11), transparent 48%),
        radial-gradient(circle at 18% 82%, rgba(16, 185, 129, 0.08), transparent 45%),
        radial-gradient(circle at 85% 78%, rgba(56, 189, 248, 0.09), transparent 45%) !important;
    }

    #sciFiBgCanvas {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
      opacity: 0.92;
    }

    .sci-fi-vignette {
      position: fixed;
      inset: 0;
      z-index: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at 50% 35%, transparent 38%, rgba(2, 6, 15, 0.72) 100%),
        repeating-linear-gradient(
          0deg,
          rgba(56, 189, 248, 0.022) 0px,
          rgba(56, 189, 248, 0.022) 1px,
          transparent 1px,
          transparent 3px
        );
    }

    .header, .main, .toast-wrap {
      position: relative;
      z-index: 2;
    }

    .header {
      position: sticky;
      top: 0;
      z-index: 40;
      background: rgba(5, 13, 27, 0.62) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.24) !important;
      backdrop-filter: blur(14px) saturate(140%);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45), 0 0 24px rgba(6, 182, 212, 0.08);
    }

    .card, .kpi-card, .control-group, .live-bar, .tf-strip, .tg-panel {
      background: rgba(8, 18, 36, 0.34) !important;
      border-color: rgba(56, 189, 248, 0.22) !important;
      backdrop-filter: blur(10px) saturate(135%);
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.14),
        0 8px 28px rgba(2, 8, 20, 0.38);
    }

    .kpi-card:hover, .card:hover {
      border-color: rgba(103, 232, 249, 0.45) !important;
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.22),
        0 0 24px rgba(6, 182, 212, 0.14);
    }

    thead th {
      background: rgba(6, 15, 30, 0.72) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.25) !important;
      color: #7dd3fc !important;
    }

    tbody tr {
      background: rgba(8, 18, 36, 0.18);
      border-bottom: 1px solid rgba(56, 189, 248, 0.11) !important;
    }

    tbody tr:hover {
      background: rgba(14, 116, 144, 0.22) !important;
    }

    tbody tr.row-long {
      background: rgba(16, 185, 129, 0.11) !important;
    }

    tbody tr.row-short {
      background: rgba(244, 63, 94, 0.11) !important;
    }

    .brand-icon {
      position: relative;
      overflow: hidden;
      background: linear-gradient(135deg, rgba(6, 182, 212, 0.35), rgba(16, 185, 129, 0.28)) !important;
      border: 1px solid rgba(103, 232, 249, 0.75) !important;
      box-shadow: 0 0 22px rgba(6, 182, 212, 0.55), inset 0 0 12px rgba(103, 232, 249, 0.35) !important;
    }
    .container { max-width: 1640px; margin: 0 auto; padding: 0 16px; }
    .header-inner { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 0; }
    .brand { display: flex; align-items: center; gap: 10px; }
    .brand-icon { width: 38px; height: 38px; border-radius: 10px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
    .brand-title { font-size: 16px; font-weight: 700; display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
    .brand-sub { font-size: 11px; color: var(--text-muted); margin-top: 1px; }
    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 2px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid var(--border); }
    .pill-tf { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .pill-nse { background: var(--amber-bg); color: #fcd34d; border-color: var(--amber-border); font-size: 10px; }
    .live-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; border: 1px solid var(--border); border-radius: 999px; padding: 5px 12px; font-size: 12px; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--amber); flex-shrink: 0; }
    .status-dot.live { background: var(--green); box-shadow: 0 0 8px rgba(16, 185, 129, 0.75); }
    .status-dot.error { background: var(--red); }
    .live-sep { color: var(--border-light); user-select: none; }
    .controls-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 10px 0; border-top: 1px solid rgba(56, 189, 248, 0.16); }
    .tf-strip { display: inline-flex; flex-wrap: wrap; align-items: center; gap: 2px; border: 1px solid var(--border); border-radius: 8px; padding: 2px; max-width: 100%; }
    .control-group { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border); border-radius: 8px; padding: 4px 8px; font-size: 12px; }
    .control-group label { color: var(--text-muted); font-size: 11px; font-weight: 600; white-space: nowrap; }
    select, input[type="number"], input[type="text"] { background: var(--bg-input); color: var(--text-main); border: 1px solid var(--border-light); border-radius: 6px; padding: 5px 8px; font-size: 12px; outline: none; }
    .input-sm { width: 52px; text-align: center; padding: 3px 4px !important; font-weight: 700; }
    .btn { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 6px 12px; border-radius: 8px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid var(--border-light); background: rgba(15, 23, 42, 0.65); color: var(--text-main); transition: all 0.15s; white-space: nowrap; text-decoration: none; }
    .btn:hover { background: rgba(14, 116, 144, 0.35); border-color: #67e8f9; }
    .btn-tf { padding: 4px 8px; font-size: 11px; border: 1px solid transparent; background: transparent; color: var(--text-muted); border-radius: 6px; }
    .btn-tf.active { background: var(--green-bg); color: #34d399; border-color: var(--green-border); }
    .btn-sound.active { background: var(--green-bg); color: #34d399; border-color: var(--green-border); }
    .btn-tg.active { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .btn-primary { background: linear-gradient(135deg, #10b981, #06b6d4); color: #04131c; border: none; font-weight: 700; }
    .btn-long { padding: 4px 8px; font-size: 11px; background: var(--green-bg); color: #34d399; border-color: var(--green-border); border-radius: 6px; }
    .btn-short { padding: 4px 8px; font-size: 11px; background: var(--red-bg); color: #fb7185; border-color: var(--red-border); border-radius: 6px; }
    .btn-del { padding: 4px 7px; font-size: 11px; background: transparent; color: var(--text-muted); border-color: transparent; }
    .error-banner { margin-bottom: 14px; padding: 12px 16px; border-radius: var(--radius); background: rgba(244, 63, 94, 0.14); border: 1px solid var(--red-border); display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; font-size: 12px; color: #fecdd3; }
    .progress-bar-wrap { height: 3px; background: var(--border); width: 100%; overflow: hidden; }
    .progress-bar-fill { height: 100%; width: 0%; background: linear-gradient(90deg, #10b981, #06b6d4); transition: width 0.25s ease; }
    .main { padding: 16px 0 36px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(5, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    .kpi-card { border: 1px solid var(--border); border-radius: var(--radius); padding: 13px 14px; display: flex; flex-direction: column; justify-content: space-between; transition: all 0.15s; }
    .kpi-card.clickable { cursor: pointer; }
    .kpi-top { display: flex; align-items: center; justify-content: space-between; gap: 6px; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; color: var(--text-muted); }
    .kpi-val { font-size: 20px; font-weight: 700; margin-top: 6px; display: flex; align-items: baseline; justify-content: space-between; gap: 8px; }
    .kpi-sub { font-size: 11px; color: var(--text-muted); margin-top: 4px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    .tabs-row { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 10px; border-bottom: 1px solid var(--border); padding-bottom: 10px; margin-bottom: 14px; }
    .tabs-list { display: flex; flex-wrap: wrap; gap: 6px; }
    .tab-btn { display: inline-flex; align-items: center; gap: 8px; padding: 8px 14px; border-radius: 8px; font-size: 13px; font-weight: 600; background: transparent; color: var(--text-muted); border: 1px solid transparent; cursor: pointer; }
    .tab-btn.active { background: rgba(14, 116, 144, 0.28); color: #fff; border-color: var(--border-light); }
    .tab-badge { padding: 1px 7px; border-radius: 99px; font-size: 11px; font-weight: 700; background: rgba(6, 182, 212, 0.24); color: var(--cyan-light); }
    .filter-row { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; }
    .card { border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
    .table-wrap { overflow-x: auto; width: 100%; }
    table { width: 100%; border-collapse: collapse; white-space: nowrap; font-size: 13px; }
    thead th { position: sticky; top: 0; z-index: 10; font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: 0.04em; padding: 11px 12px; user-select: none; }
    thead th.sortable { cursor: pointer; }
    tbody td { padding: 10px 12px; vertical-align: middle; }
    .badge { display: inline-block; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid transparent; }
    .badge-long { background: var(--green-bg); color: var(--green); border-color: var(--green-border); }
    .badge-short { background: var(--red-bg); color: var(--red); border-color: var(--red-border); }
    .badge-wait { background: var(--amber-bg); color: var(--amber); border-color: var(--amber-border); }
    .badge-neutral { background: rgba(30, 41, 59, 0.65); color: #cbd5e1; border-color: #334155; }
    .sector-tag { display: inline-block; padding: 2px 7px; border-radius: 5px; font-size: 10px; font-weight: 600; background: rgba(56, 189, 248, 0.1); color: var(--cyan-light); border: 1px solid rgba(56, 189, 248, 0.22); }
    .rsi-cell { display: flex; align-items: center; gap: 8px; min-width: 115px; }
    .rsi-bar-bg { flex: 1; height: 6px; background: rgba(30, 41, 59, 0.75); border-radius: 99px; overflow: hidden; }
    .rsi-bar-fill { height: 100%; border-radius: 99px; transition: width 0.3s ease; }
    .action-btns { display: inline-flex; gap: 5px; }
    .signals-grid { display: grid; grid-template-columns: repeat(2, minmax(0, 1fr)); gap: 16px; }
    .signal-header { padding: 12px 16px; display: flex; align-items: center; justify-content: space-between; border-bottom: 1px solid var(--border); }
    .signal-header.long-hd { background: rgba(16, 185, 129, 0.11); border-bottom-color: var(--green-border); }
    .signal-header.short-hd { background: rgba(244, 63, 94, 0.11); border-bottom-color: var(--red-border); }
    .empty-state { padding: 36px 20px; text-align: center; color: var(--text-muted); font-size: 13px; }
    .summary-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin-bottom: 14px; }
    .form-card { padding: 16px; margin-bottom: 14px; }
    .form-header { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 8px; margin-bottom: 12px; }
    .form-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 10px; align-items: end; }
    .field label { display: block; font-size: 11px; color: var(--text-muted); margin-bottom: 4px; font-weight: 600; }
    .field input, .field select { width: 100%; padding: 7px 9px; }
    .toast-wrap { position: fixed; bottom: 16px; right: 16px; z-index: 60; display: flex; flex-direction: column; gap: 8px; max-width: 340px; pointer-events: none; }
    .toast { pointer-events: auto; padding: 10px 14px; border-radius: 10px; font-size: 12px; font-weight: 600; background: rgba(8, 18, 36, 0.88); backdrop-filter: blur(10px); border: 1px solid var(--border-light); color: var(--text-main); }
    .toast.long { border-color: var(--green-border); color: #6ee7b7; }
    .toast.short { border-color: var(--red-border); color: #fda4af; }
    .toast.error { border-color: var(--red-border); color: #fecdd3; }
  </style>
</head>
<body>
  <canvas id="sciFiBgCanvas"></canvas>
  <div class="sci-fi-vignette"></div>
  <header class="header">
    <div class="container">
      <div class="header-inner">
        <div class="brand">
          <div class="brand-icon" title="J.A.R.V.I.S. Nifty 50 Quantum Scanner">
            <svg width="26" height="26" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg"><polygon points="18,2 32,10 32,26 18,34 4,26 4,10" fill="#04131c" stroke="#67e8f9" stroke-width="1.5"/><circle cx="18" cy="18" r="10.5" stroke="#34d399" stroke-width="1.2" stroke-dasharray="4 3"/><path d="M9 21L14 15L18.5 19.5L27 11" stroke="#67e8f9" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="27" cy="11" r="2.5" fill="#10b981"/></svg>
          </div>
          <div>
            <div class="brand-title">
              <span>Nifty 50 Live RSI Screener</span>
              <span id="headerTfBadge" class="pill pill-tf mono">15M</span>
              <span class="pill pill-nse mono">J.A.R.V.I.S. HUD</span>
              <a href="/" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--cyan-border); color: var(--cyan-light);">🪙 Crypto Futures</a>
              <a href="/portfolio" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--purple-border); color: var(--purple-light);">💼 Live Portfolio →</a>
            </div>
            <div class="brand-sub">5m to 1Y Timeframes • Dual Render TG Bot Connected • Wilder's 14 RSI • Multi-Source NSE</div>
          </div>
        </div>
        <div class="live-bar">
          <span id="liveDot" class="status-dot pulse-dot"></span>
          <strong id="liveFeedName" class="mono">INITIALIZING...</strong>
          <span class="live-sep">|</span>
          <span class="text-muted">Loaded: <strong id="liveLoadedCount" class="mono text-cyan">0/50</strong></span>
          <span class="live-sep">|</span>
          <span class="text-muted">Scans: <strong id="liveScanCount" class="mono text-cyan">0</strong></span>
          <span class="live-sep">|</span>
          <span id="liveTimestamp" class="mono text-muted">--:--:-- IST</span>
          <button id="refreshBtn" type="button" class="btn" style="padding: 3px 9px; font-size: 11px;"><span id="refreshSpin">↻</span> Refresh</button>
        </div>
      </div>
      <div class="controls-bar">
        <div class="control-group">
          <label for="sourceSelect">Data Source:</label>
          <select id="sourceSelect">
            <option value="auto" selected>Auto (TV + Yahoo RSI)</option>
            <option value="tv_direct">TradingView NSE Direct</option>
            <option value="yahoo_allorigins">Yahoo Finance (AllOrigins)</option>
            <option value="yahoo_codetabs">Yahoo Finance (CodeTabs)</option>
          </select>
        </div>
        <div class="tf-strip">
          <button type="button" data-tf="5m" class="btn btn-tf">5m</button>
          <button type="button" data-tf="15m" class="btn btn-tf active">15m</button>
          <button type="button" data-tf="30m" class="btn btn-tf">30m</button>
          <button type="button" data-tf="1h" class="btn btn-tf">1h</button>
          <button type="button" data-tf="2h" class="btn btn-tf">2h</button>
          <button type="button" data-tf="4h" class="btn btn-tf">4h</button>
          <button type="button" data-tf="1d" class="btn btn-tf">1D</button>
          <button type="button" data-tf="1w" class="btn btn-tf">1W</button>
          <button type="button" data-tf="1mo" class="btn btn-tf">1M</button>
          <button type="button" data-tf="1y" class="btn btn-tf">1Y</button>
        </div>
        <div class="control-group">
          <label for="intervalSelect">Auto Sync:</label>
          <select id="intervalSelect">
            <option value="5000">Every 5s</option>
            <option value="10000" selected>Every 10s</option>
            <option value="30000">Every 30s</option>
            <option value="60000">Every 60s</option>
            <option value="0">Manual</option>
          </select>
        </div>
        <div class="control-group"><label for="longThInput" class="text-green">Long RSI &lt;</label><input id="longThInput" type="number" min="10" max="55" value="35" class="input-sm mono text-green" /></div>
        <div class="control-group"><label for="shortThInput" class="text-red">Short RSI &gt;</label><input id="shortThInput" type="number" min="45" max="90" value="70" class="input-sm mono text-red" /></div>
        <button id="soundToggleBtn" type="button" class="btn btn-sound"><span id="soundIcon">🔇</span><span id="soundLabel">Sound: OFF</span></button>
        <button id="tgToggleBtn" type="button" class="btn btn-tg active"><span>✈️</span><span id="tgLabel">Render TG Alert: ON</span></button>
        <button id="tgTestBtn" type="button" class="btn">🧪 Test TG</button>
      </div>
    </div>
    <div class="progress-bar-wrap"><div id="loadProgressBar" class="progress-bar-fill"></div></div>
  </header>
  <main class="main container">
    <div id="networkErrorBanner" class="error-banner hidden">
      <div><strong>Unable to reach live NSE feeds right now.</strong><span id="networkErrorDetail"></span></div>
      <button id="testConnBtn" type="button" class="btn btn-primary" style="padding: 6px 12px;">Retry Live Scan</button>
    </div>
    <section class="kpi-grid">
      <div class="kpi-card"><div class="kpi-top"><span>NSE Status &amp; Sync</span><span id="kpiTfTag" class="mono text-cyan">15M</span></div><div id="kpiSyncTime" class="kpi-val mono" style="font-size: 16px;">--:--:-- IST</div><div id="kpiFeedDetail" class="kpi-sub">Connecting...</div></div>
      <div id="kpiLongCard" class="kpi-card clickable"><div class="kpi-top"><span>Long Signals (&lt; <span class="long-th-txt">35</span>)</span><span class="text-green">View →</span></div><div class="kpi-val mono"><span id="kpiLongCount" class="text-green">0</span><span class="text-dim" style="font-size: 12px;">/ 50</span></div><div class="kpi-sub">Oversold dip setups</div></div>
      <div id="kpiShortCard" class="kpi-card clickable"><div class="kpi-top"><span>Short Signals (&gt; <span class="short-th-txt">70</span>)</span><span class="text-red">View →</span></div><div class="kpi-val mono"><span id="kpiShortCount" class="text-red">0</span><span class="text-dim" style="font-size: 12px;">/ 50</span></div><div class="kpi-sub">Overbought reversal setups</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Oversold</span><span class="text-green mono">Lowest RSI</span></div><div class="kpi-val"><span id="kpiOversoldSym" style="font-size: 16px;">--</span><span id="kpiOversoldRsi" class="mono text-green">--</span></div><div id="kpiOversoldPrice" class="kpi-sub mono">LTP: --</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Overbought</span><span class="text-red mono">Highest RSI</span></div><div class="kpi-val"><span id="kpiOverboughtSym" style="font-size: 16px;">--</span><span id="kpiOverboughtRsi" class="mono text-red">--</span></div><div id="kpiOverboughtPrice" class="kpi-sub mono">LTP: --</div></div>
    </section>
    <div class="tabs-row">
      <div class="tabs-list">
        <button type="button" data-tab="screener" class="tab-btn active">Nifty 50 Screener</button>
        <button type="button" data-tab="signals" class="tab-btn"><span>Active Signals (LONG &amp; SHORT)</span><span id="tabSignalsBadge" class="tab-badge mono">0</span></button>
        <button type="button" data-tab="tradelog" class="tab-btn"><span>Equity Trade Log</span><span id="tabTradesBadge" class="tab-badge mono">0</span></button>
      </div>
      <div class="filter-row">
        <input id="searchInput" type="text" placeholder="Search symbol / company..." style="width: 185px;" />
        <select id="sectorFilter"><option value="ALL">All Sectors</option></select>
        <select id="signalFilter"><option value="ALL">All Signals</option><option value="LONG">LONG Only</option><option value="SHORT">SHORT Only</option><option value="WAIT">WAIT Only</option></select>
      </div>
    </div>
    <section id="tab-screener" class="tab-panel"><div class="card"><div class="table-wrap"><table><thead><tr><th class="sortable" data-sort="sr"># <span class="sort-ind"></span></th><th class="sortable" data-sort="symbol">Stock / Company <span class="sort-ind"></span></th><th class="sortable" data-sort="sector">Sector <span class="sort-ind"></span></th><th class="sortable text-right" data-sort="price">Live LTP (₹) <span class="sort-ind"></span></th><th class="sortable text-right" data-sort="prevClose">Prev Close <span class="sort-ind"></span></th><th class="sortable text-right" data-sort="changePct">Day Chg % <span class="sort-ind"></span></th><th class="sortable" data-sort="rsi">14-Period RSI <span class="sort-ind"></span></th><th class="sortable text-center" data-sort="signal">Signal <span class="sort-ind"></span></th><th class="sortable text-center" data-sort="rsiRank">RSI Rank <span class="sort-ind"></span></th><th class="sortable text-right" data-sort="target">Target +6.28% (2π) <span class="sort-ind"></span></th><th class="sortable text-right" data-sort="nextAvg">Next Avg -3.14% (π) <span class="sort-ind"></span></th><th class="text-center">Quick Trade</th></tr></thead><tbody id="screenerTbody"><tr><td colspan="12" class="empty-state">Fetching live Nifty 50 prices &amp; RSI...</td></tr></tbody></table></div></div></section>
    <section id="tab-signals" class="tab-panel hidden"><div class="signals-grid"><div class="card"><div class="signal-header long-hd"><div><strong class="text-green">LONG Signals (RSI &lt; <span class="long-th-txt">35</span>)</strong></div><span id="longSigCount" class="badge badge-long mono">0</span></div><div id="longSigEmpty" class="empty-state hidden">No Nifty 50 stocks below Long RSI threshold.</div><div id="longSigWrap" class="table-wrap"><table><thead><tr><th>Stock</th><th class="text-right">RSI</th><th class="text-right">LTP (₹)</th><th class="text-right">TP (+6.28%)</th><th class="text-right">Avg (-3.14%)</th><th class="text-center">Action</th></tr></thead><tbody id="longSigTbody"></tbody></table></div></div><div class="card"><div class="signal-header short-hd"><div><strong class="text-red">SHORT Signals (RSI &gt; <span class="short-th-txt">70</span>)</strong></div><span id="shortSigCount" class="badge badge-short mono">0</span></div><div id="shortSigEmpty" class="empty-state hidden">No Nifty 50 stocks above Short RSI threshold.</div><div id="shortSigWrap" class="table-wrap"><table><thead><tr><th>Stock</th><th class="text-right">RSI</th><th class="text-right">LTP (₹)</th><th class="text-right">TP (-6.28%)</th><th class="text-right">Avg (+3.14%)</th><th class="text-center">Action</th></tr></thead><tbody id="shortSigTbody"></tbody></table></div></div></div></section>
    <section id="tab-tradelog" class="tab-panel hidden"><div class="summary-grid"><div class="kpi-card"><div class="kpi-top"><span>Total Capital Deployed</span></div><div id="sumCapital" class="kpi-val mono">₹0.00</div></div><div class="kpi-card"><div class="kpi-top"><span>Unrealized P&amp;L</span></div><div id="sumPnl" class="kpi-val mono">₹0.00</div></div><div class="kpi-card" style="flex-direction: row; align-items: center; justify-content: space-between;"><div><div class="kpi-top"><span>Overall Return %</span></div><div id="sumRoe" class="kpi-val mono">0.00%</div></div><button id="exportCsvBtn" type="button" class="btn btn-long" style="padding: 7px 12px; font-size: 12px;">⬇ Export CSV</button></div></div><form id="tradeForm" class="card form-card"><div class="form-header"><strong>Add NSE Equity / Intraday Trade</strong><div style="display: flex; align-items: center; gap: 10px;"><span id="posSizePreview" class="mono text-muted" style="font-size: 12px;">Value: ₹0.00</span><button id="autoFillBtn" type="button" class="btn" style="padding: 4px 9px; font-size: 11px;">Fill Live LTP</button></div></div><div class="form-grid"><div class="field"><label for="fSym">Stock</label><select id="fSym" required></select></div><div class="field"><label for="fSide">Side</label><select id="fSide" required><option value="LONG">LONG</option><option value="SHORT">SHORT</option></select></div><div class="field"><label for="fEntry">Entry Price (₹)</label><input id="fEntry" type="number" step="any" min="0" required placeholder="0.00" class="mono" /></div><div class="field"><label for="fQty">Quantity (Shares)</label><input id="fQty" type="number" step="1" min="1" required value="10" class="mono" /></div><div class="field"><label for="fRsi">Entry RSI</label><input id="fRsi" type="number" step="0.01" min="0" max="100" placeholder="32.50" class="mono" /></div><div class="field"><button type="submit" class="btn btn-primary" style="width: 100%; padding: 8px;">+ Add Trade</button></div></div></form><div class="card"><div class="table-wrap"><table><thead><tr><th>Stock</th><th class="text-center">Side</th><th class="text-right">Qty</th><th class="text-right">Entry (₹)</th><th class="text-right">Invested</th><th class="text-right">Live LTP</th><th class="text-right">Unrealized P&amp;L</th><th class="text-right">Return %</th><th class="text-right">TP (±6.28%)</th><th class="text-right">Avg (±3.14%)</th><th class="text-center">Status</th><th class="text-center">Del</th></tr></thead><tbody id="tradeLogTbody"></tbody></table></div><div id="tradeLogEmpty" class="empty-state hidden">No equity trades logged yet.</div></div></section>
  </main>
  <div id="toastWrap" class="toast-wrap"></div>
  <script>
    (() => {
      'use strict';
      const STOCKS = [
        { sr: 1, symbol: 'RELIANCE', tv: 'RELIANCE', name: 'Reliance Industries', sector: 'Energy' },
        { sr: 2, symbol: 'HDFCBANK', tv: 'HDFCBANK', name: 'HDFC Bank', sector: 'Banking' },
        { sr: 3, symbol: 'TCS', tv: 'TCS', name: 'Tata Consultancy Svcs', sector: 'IT' },
        { sr: 4, symbol: 'BHARTIARTL', tv: 'BHARTIARTL', name: 'Bharti Airtel', sector: 'Telecom' },
        { sr: 5, symbol: 'ICICIBANK', tv: 'ICICIBANK', name: 'ICICI Bank', sector: 'Banking' },
        { sr: 6, symbol: 'SBIN', tv: 'SBIN', name: 'State Bank of India', sector: 'Banking' },
        { sr: 7, symbol: 'INFY', tv: 'INFY', name: 'Infosys', sector: 'IT' },
        { sr: 8, symbol: 'BAJFINANCE', tv: 'BAJFINANCE', name: 'Bajaj Finance', sector: 'Finance' },
        { sr: 9, symbol: 'HINDUNILVR', tv: 'HINDUNILVR', name: 'Hindustan Unilever', sector: 'FMCG' },
        { sr: 10, symbol: 'ITC', tv: 'ITC', name: 'ITC Ltd', sector: 'FMCG' },
        { sr: 11, symbol: 'LT', tv: 'LT', name: 'Larsen & Toubro', sector: 'Infra' },
        { sr: 12, symbol: 'HCLTECH', tv: 'HCLTECH', name: 'HCL Technologies', sector: 'IT' },
        { sr: 13, symbol: 'KOTAKBANK', tv: 'KOTAKBANK', name: 'Kotak Mahindra Bank', sector: 'Banking' },
        { sr: 14, symbol: 'SUNPHARMA', tv: 'SUNPHARMA', name: 'Sun Pharma', sector: 'Pharma' },
        { sr: 15, symbol: 'MARUTI', tv: 'MARUTI', name: 'Maruti Suzuki', sector: 'Auto' },
        { sr: 16, symbol: 'M&M', tv: 'M_M', name: 'Mahindra & Mahindra', sector: 'Auto' },
        { sr: 17, symbol: 'AXISBANK', tv: 'AXISBANK', name: 'Axis Bank', sector: 'Banking' },
        { sr: 18, symbol: 'ULTRACEMCO', tv: 'ULTRACEMCO', name: 'UltraTech Cement', sector: 'Materials' },
        { sr: 19, symbol: 'NTPC', tv: 'NTPC', name: 'NTPC Ltd', sector: 'Energy' },
        { sr: 20, symbol: 'BAJAJFINSV', tv: 'BAJAJFINSV', name: 'Bajaj Finserv', sector: 'Finance' },
        { sr: 21, symbol: 'TITAN', tv: 'TITAN', name: 'Titan Company', sector: 'Consumer' },
        { sr: 22, symbol: 'ADANIPORTS', tv: 'ADANIPORTS', name: 'Adani Ports & SEZ', sector: 'Infra' },
        { sr: 23, symbol: 'ONGC', tv: 'ONGC', name: 'ONGC', sector: 'Energy' },
        { sr: 24, symbol: 'BEL', tv: 'BEL', name: 'Bharat Electronics', sector: 'Defence' },
        { sr: 25, symbol: 'ADANIENT', tv: 'ADANIENT', name: 'Adani Enterprises', sector: 'Metals' },
        { sr: 26, symbol: 'POWERGRID', tv: 'POWERGRID', name: 'Power Grid Corp', sector: 'Energy' },
        { sr: 27, symbol: 'WIPRO', tv: 'WIPRO', name: 'Wipro', sector: 'IT' },
        { sr: 28, symbol: 'JSWSTEEL', tv: 'JSWSTEEL', name: 'JSW Steel', sector: 'Metals' },
        { sr: 29, symbol: 'TATAMOTORS', tv: 'TATAMOTORS', name: 'Tata Motors', sector: 'Auto' },
        { sr: 30, symbol: 'COALINDIA', tv: 'COALINDIA', name: 'Coal India', sector: 'Energy' },
        { sr: 31, symbol: 'ASIANPAINT', tv: 'ASIANPAINT', name: 'Asian Paints', sector: 'Consumer' },
        { sr: 32, symbol: 'BAJAJ-AUTO', tv: 'BAJAJ_AUTO', name: 'Bajaj Auto', sector: 'Auto' },
        { sr: 33, symbol: 'NESTLEIND', tv: 'NESTLEIND', name: 'Nestle India', sector: 'FMCG' },
        { sr: 34, symbol: 'TRENT', tv: 'TRENT', name: 'Trent Ltd', sector: 'Consumer' },
        { sr: 35, symbol: 'GRASIM', tv: 'GRASIM', name: 'Grasim Industries', sector: 'Materials' },
        { sr: 36, symbol: 'EICHERMOT', tv: 'EICHERMOT', name: 'Eicher Motors', sector: 'Auto' },
        { sr: 37, symbol: 'SBILIFE', tv: 'SBILIFE', name: 'SBI Life Insurance', sector: 'Finance' },
        { sr: 38, symbol: 'TECHM', tv: 'TECHM', name: 'Tech Mahindra', sector: 'IT' },
        { sr: 39, symbol: 'HINDALCO', tv: 'HINDALCO', name: 'Hindalco Industries', sector: 'Metals' },
        { sr: 40, symbol: 'TATASTEEL', tv: 'TATASTEEL', name: 'Tata Steel', sector: 'Metals' },
        { sr: 41, symbol: 'HDFCLIFE', tv: 'HDFCLIFE', name: 'HDFC Life Insurance', sector: 'Finance' },
        { sr: 42, symbol: 'SHRIRAMFIN', tv: 'SHRIRAMFIN', name: 'Shriram Finance', sector: 'Finance' },
        { sr: 43, symbol: 'CIPLA', tv: 'CIPLA', name: 'Cipla', sector: 'Pharma' },
        { sr: 44, symbol: 'APOLLOHOSP', tv: 'APOLLOHOSP', name: 'Apollo Hospitals', sector: 'Pharma' },
        { sr: 45, symbol: 'DRREDDY', tv: 'DRREDDY', name: "Dr. Reddy's Labs", sector: 'Pharma' },
        { sr: 46, symbol: 'TATACONSUM', tv: 'TATACONSUM', name: 'Tata Consumer Products', sector: 'FMCG' },
        { sr: 47, symbol: 'HEROMOTOCO', tv: 'HEROMOTOCO', name: 'Hero MotoCorp', sector: 'Auto' },
        { sr: 48, symbol: 'INDUSINDBK', tv: 'INDUSINDBK', name: 'IndusInd Bank', sector: 'Banking' },
        { sr: 49, symbol: 'JIOFIN', tv: 'JIOFIN', name: 'Jio Financial Services', sector: 'Finance' },
        { sr: 50, symbol: 'BRITANNIA', tv: 'BRITANNIA', name: 'Britannia Industries', sector: 'FMCG' }
      ];
      const TV_TO_SYM = new Map(STOCKS.map(s => [`NSE:${s.tv}`, s.symbol]));
      const TIMEFRAMES = [
        { key: '5m', label: '5m', tvCol: 'RSI|5', yahoo: { interval: '5m', range: '5d' } },
        { key: '15m', label: '15m', tvCol: 'RSI|15', yahoo: { interval: '15m', range: '5d' } },
        { key: '30m', label: '30m', tvCol: 'RSI|30', yahoo: { interval: '30m', range: '10d' } },
        { key: '1h', label: '1h', tvCol: 'RSI|60', yahoo: { interval: '60m', range: '1mo' } },
        { key: '2h', label: '2h', tvCol: 'RSI|120', yahoo: { interval: '60m', range: '2mo' } },
        { key: '4h', label: '4h', tvCol: 'RSI|240', yahoo: { interval: '60m', range: '3mo' } },
        { key: '1d', label: '1D', tvCol: 'RSI', yahoo: { interval: '1d', range: '6mo' } },
        { key: '1w', label: '1W', tvCol: 'RSI|1W', yahoo: { interval: '1wk', range: '2y' } },
        { key: '1mo', label: '1M', tvCol: 'RSI|1M', yahoo: { interval: '1mo', range: '5y' } },
        { key: '1y', label: '1Y', tvCol: null, yahoo: { interval: '1mo', range: '10y' } }
      ];
      const TF_MAP = new Map(TIMEFRAMES.map(t => [t.key, t]));
      function createEmptyRsiMap() { return { '5m': null, '15m': null, '30m': null, '1h': null, '2h': null, '4h': null, '1d': null, '1w': null, '1mo': null, '1y': null }; }
      const RSI_PERIOD = 14, STORAGE_TRADES = 'nifty50_rsi_v4_trades';
      const state = { timeframe: '15m', longTh: 35, shortTh: 70, tgEnabled: true, searchQuery: '', sectorFilter: 'ALL', signalFilter: 'ALL', scans: 0, lastUpdateDate: null, sessionId: 0, isScanning: false, stocks: new Map(), trades: [] };
      STOCKS.forEach(s => state.stocks.set(s.symbol, { ...s, price: null, prevClose: null, changePct: null, rsiMap: createEmptyRsiMap(), rsi: null, rsiRank: null, loaded: false }));
      const tfDisplayLabel = k => (TF_MAP.get(k)?.label || k).toUpperCase();
      const fmtPrice = v => (v == null || isNaN(v)) ? '--' : v.toLocaleString('en-IN', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
      const fmtPct = v => (v == null || isNaN(v)) ? '--' : (v > 0 ? '+' : '') + v.toFixed(2) + '%';
      const fmtIST = d => d ? new Intl.DateTimeFormat('en-IN', { timeZone: 'Asia/Kolkata', hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: false }).format(d) + ' IST' : '--:--:-- IST';
      const getSignal = rsi => (rsi == null || isNaN(rsi)) ? { label: 'LOAD', cls: 'badge-neutral' } : rsi < state.longTh ? { label: 'LONG', cls: 'badge-long' } : rsi > state.shortTh ? { label: 'SHORT', cls: 'badge-short' } : { label: 'WAIT', cls: 'badge-wait' };
      function fetchWithTimeout(url, opts = {}, ms = 8000) { const c = new AbortController(); const t = setTimeout(() => c.abort(), ms); return fetch(url, { ...opts, signal: c.signal }).finally(() => clearTimeout(t)); }
      function showToast(msg, type = 'info') { const w = document.getElementById('toastWrap'); const el = document.createElement('div'); el.className = `toast ${type}`; el.textContent = msg; w.appendChild(el); setTimeout(() => el.remove(), 3500); }
      async function triggerRenderTelegramSignal(s, side) { if (!state.tgEnabled) return; try { await fetch('/api/nifty-alert', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ symbol: s.symbol, name: s.name, sector: s.sector, side, timeframe: tfDisplayLabel(state.timeframe), price: s.price, changePct: s.changePct, rsi: s.rsi }) }); } catch {} }
      function checkRsiAlerts(s, oldRsi, init) { if (init || s.rsi == null || oldRsi == null) return; if (oldRsi >= state.longTh && s.rsi < state.longTh) { showToast(`LONG: ${s.symbol} RSI ${s.rsi.toFixed(2)}`, 'long'); triggerRenderTelegramSignal(s, 'LONG'); } else if (oldRsi <= state.shortTh && s.rsi > state.shortTh) { showToast(`SHORT: ${s.symbol} RSI ${s.rsi.toFixed(2)}`, 'short'); triggerRenderTelegramSignal(s, 'SHORT'); } }
      async function fetchTradingViewDirect(sid, isInitial) { const tvCols = TIMEFRAMES.filter(t => t.tvCol).map(t => t.tvCol); const res = await fetchWithTimeout('https://scanner.tradingview.com/india/scan', { method: 'POST', headers: { 'Content-Type': 'text/plain;charset=UTF-8' }, body: JSON.stringify({ symbols: { tickers: STOCKS.map(s => `NSE:${s.tv}`), query: { types: [] } }, columns: ['name', 'close', 'change', 'change_abs', ...tvCols] }) }, 9000); if (!res.ok) throw new Error('TV HTTP ' + res.status); const json = await res.json(); if (sid !== state.sessionId || !json?.data?.length) throw new Error('Empty TV'); json.data.forEach(row => { const sym = TV_TO_SYM.get(row.s); const s = state.stocks.get(sym); const d = row.d; if (!s || !Array.isArray(d)) return; const oldRsi = s.rsi; s.price = typeof d[1] === 'number' ? d[1] : null; s.changePct = typeof d[2] === 'number' ? d[2] : null; s.prevClose = (s.price != null && typeof d[3] === 'number') ? s.price - d[3] : null; const m = createEmptyRsiMap(); TIMEFRAMES.forEach((tf, i) => { if (tf.tvCol && typeof d[4 + i] === 'number') m[tf.key] = Math.round(d[4 + i] * 100) / 100; }); s.rsiMap = m; s.rsi = s.rsiMap[state.timeframe]; s.loaded = s.price != null; checkRsiAlerts(s, oldRsi, isInitial); }); }
      async function runScan(isInitial = false) { if (state.isScanning && !isInitial) return; const sid = isInitial ? ++state.sessionId : state.sessionId; state.isScanning = true; try { await fetchTradingViewDirect(sid, isInitial); state.scans++; state.lastUpdateDate = new Date(); document.getElementById('liveDot').className = 'status-dot live pulse-dot'; document.getElementById('liveFeedName').textContent = `LIVE • TV NSE ${tfDisplayLabel(state.timeframe)}`; renderAll(); } catch (e) { document.getElementById('liveDot').className = 'status-dot error'; document.getElementById('liveFeedName').textContent = 'RECONNECTING...'; } finally { state.isScanning = false; } }
      function renderAll() { const loaded = Array.from(state.stocks.values()).filter(s => s.loaded && s.rsi != null); loaded.slice().sort((a, b) => a.rsi - b.rsi).forEach((s, i) => s.rsiRank = i + 1); const longs = loaded.filter(s => s.rsi < state.longTh); const shorts = loaded.filter(s => s.rsi > state.shortTh); document.getElementById('liveLoadedCount').textContent = `${loaded.length}/50`; document.getElementById('liveScanCount').textContent = state.scans; const ist = fmtIST(state.lastUpdateDate); document.getElementById('liveTimestamp').textContent = ist; document.getElementById('kpiSyncTime').textContent = ist; document.getElementById('kpiLongCount').textContent = longs.length; document.getElementById('kpiShortCount').textContent = shorts.length; document.getElementById('tabSignalsBadge').textContent = longs.length + shorts.length; document.getElementById('tabTradesBadge').textContent = state.trades.length; if (loaded.length) { const srt = loaded.slice().sort((a, b) => a.rsi - b.rsi); document.getElementById('kpiOversoldSym').textContent = srt[0].symbol; document.getElementById('kpiOversoldRsi').textContent = srt[0].rsi.toFixed(2); document.getElementById('kpiOversoldPrice').textContent = `LTP: ₹${fmtPrice(srt[0].price)}`; const hi = srt[srt.length - 1]; document.getElementById('kpiOverboughtSym').textContent = hi.symbol; document.getElementById('kpiOverboughtRsi').textContent = hi.rsi.toFixed(2); document.getElementById('kpiOverboughtPrice').textContent = `LTP: ₹${fmtPrice(hi.price)}`; } const q = state.searchQuery.trim().toLowerCase(); let list = Array.from(state.stocks.values()).filter(s => (!q || s.symbol.toLowerCase().includes(q) || s.name.toLowerCase().includes(q)) && (state.sectorFilter === 'ALL' || s.sector === state.sectorFilter) && (state.signalFilter === 'ALL' || getSignal(s.rsi).label === state.signalFilter)); list.sort((a, b) => (a.rsi ?? 999) - (b.rsi ?? 999)); document.getElementById('screenerTbody').innerHTML = list.map(s => { const sig = getSignal(s.rsi); return `<tr class="${sig.label === 'LONG' ? 'row-long' : sig.label === 'SHORT' ? 'row-short' : ''}"><td class="mono text-dim">#${s.sr}</td><td><strong class="mono">${s.symbol}</strong><div class="text-muted" style="font-size:11px;">${s.name}</div></td><td><span class="sector-tag">${s.sector}</span></td><td class="text-right mono" style="font-weight:700;">${fmtPrice(s.price)}</td><td class="text-right mono text-muted">${fmtPrice(s.prevClose)}</td><td class="text-right mono ${s.changePct >= 0 ? 'text-green' : 'text-red'}">${fmtPct(s.changePct)}</td><td class="mono">${s.rsi != null ? s.rsi.toFixed(2) : '--'}</td><td class="text-center"><span class="badge ${sig.cls}">${sig.label}</span></td><td class="text-center mono">${s.rsiRank ? '#' + s.rsiRank : '--'}</td><td class="text-right mono text-green">${fmtPrice(s.price ? s.price * 1.0628 : null)}</td><td class="text-right mono text-amber">${fmtPrice(s.price ? s.price * 0.9686 : null)}</td><td class="text-center"><button type="button" class="btn btn-long" data-quick-sym="${s.symbol}" data-quick-side="LONG">+ Long</button> <button type="button" class="btn btn-short" data-quick-sym="${s.symbol}" data-quick-side="SHORT">+ Short</button></td></tr>`; }).join(''); }
      (function initJarvisSimulation() {
        const canvas = document.getElementById('sciFiBgCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let w = 0, h = 0, dpr = Math.min(window.devicePixelRatio || 1, 2);
        let tick = 0;

        const particles = [];
        const streams = [];
        const PARTICLE_COUNT = 54;
        const STREAM_COUNT = 9;

        function resize() {
          w = window.innerWidth;
          h = window.innerHeight;
          canvas.width = Math.max(1, w) * dpr;
          canvas.height = Math.max(1, h) * dpr;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function initEntities() {
          particles.length = 0;
          for (let i = 0; i < PARTICLE_COUNT; i++) {
            particles.push({
              x: Math.random() * (w || 1440),
              y: Math.random() * (h || 900),
              vx: (Math.random() - 0.5) * 0.42,
              vy: (Math.random() - 0.5) * 0.42,
              size: 1 + Math.random() * 2,
              hue: i % 3 === 0 ? '16, 185, 129' : '56, 189, 248'
            });
          }

          streams.length = 0;
          for (let s = 0; s < STREAM_COUNT; s++) {
            streams.push({
              x: ((s + 0.5) / STREAM_COUNT) * (w || 1440),
              y: Math.random() * (h || 900),
              len: 45 + Math.random() * 95,
              speed: 1.1 + Math.random() * 1.9,
              alpha: 0.08 + Math.random() * 0.14
            });
          }
        }

        function drawArcReactor(cx, cy, radius, rot) {
          ctx.save();
          ctx.translate(cx, cy);

          const glow = ctx.createRadialGradient(0, 0, radius * 0.15, 0, 0, radius * 1.35);
          glow.addColorStop(0, 'rgba(56, 189, 248, 0.14)');
          glow.addColorStop(0.55, 'rgba(6, 182, 212, 0.05)');
          glow.addColorStop(1, 'rgba(6, 182, 212, 0)');
          ctx.fillStyle = glow;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 1.35, 0, Math.PI * 2);
          ctx.fill();

          ctx.save();
          ctx.rotate(rot * 0.65);
          const segs = 12;
          for (let i = 0; i < segs; i++) {
            const a0 = (i / segs) * Math.PI * 2 + 0.05;
            const a1 = ((i + 1) / segs) * Math.PI * 2 - 0.05;
            ctx.strokeStyle = i % 3 === 0 ? 'rgba(16, 185, 129, 0.28)' : 'rgba(103, 232, 249, 0.24)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(0, 0, radius, a0, a1);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(-rot * 1.25);
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.22)';
          ctx.lineWidth = 1.2;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.76, 0, Math.PI * 2);
          ctx.stroke();

          for (let k = 0; k < 24; k++) {
            const ang = (k / 24) * Math.PI * 2;
            const rIn = radius * (k % 2 === 0 ? 0.69 : 0.72);
            const rOut = radius * 0.76;
            ctx.strokeStyle = k % 6 === 0 ? 'rgba(110, 231, 183, 0.45)' : 'rgba(125, 211, 252, 0.26)';
            ctx.lineWidth = k % 6 === 0 ? 1.8 : 1;
            ctx.beginPath();
            ctx.moveTo(Math.cos(ang) * rIn, Math.sin(ang) * rIn);
            ctx.lineTo(Math.cos(ang) * rOut, Math.sin(ang) * rOut);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(rot * 2.4);
          const sweepGrad = ctx.createConicGradient(0, 0);
          sweepGrad.addColorStop(0, 'rgba(103, 232, 249, 0.28)');
          sweepGrad.addColorStop(0.14, 'rgba(16, 185, 129, 0.08)');
          sweepGrad.addColorStop(0.15, 'rgba(56, 189, 248, 0)');
          sweepGrad.addColorStop(1, 'rgba(56, 189, 248, 0)');
          ctx.fillStyle = sweepGrad;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.66, 0, Math.PI * 2);
          ctx.fill();
          ctx.restore();

          ctx.strokeStyle = 'rgba(125, 211, 252, 0.35)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.26, 0, Math.PI * 2);
          ctx.stroke();

          ctx.restore();
        }

        resize();
        initEntities();
        window.addEventListener('resize', () => {
          resize();
          initEntities();
        });

        function renderJarvis() {
          tick += 1;
          const rot = tick * 0.006;
          ctx.clearRect(0, 0, w, h);

          const step = 72;
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.04)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let x = 0; x < w; x += step) {
            ctx.moveTo(x, 0);
            ctx.lineTo(x, h);
          }
          for (let y = 0; y < h; y += step) {
            ctx.moveTo(0, y);
            ctx.lineTo(w, y);
          }
          ctx.stroke();

          for (let i = 0; i < streams.length; i++) {
            const st = streams[i];
            st.y += st.speed;
            if (st.y - st.len > h) {
              st.y = -20;
              st.x = ((i + 0.5) / STREAM_COUNT) * w + (Math.random() - 0.5) * 40;
            }
            const g = ctx.createLinearGradient(st.x, st.y - st.len, st.x, st.y);
            g.addColorStop(0, 'rgba(56, 189, 248, 0)');
            g.addColorStop(0.75, `rgba(56, 189, 248, ${st.alpha})`);
            g.addColorStop(1, 'rgba(110, 231, 183, 0.45)');
            ctx.strokeStyle = g;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(st.x, st.y - st.len);
            ctx.lineTo(st.x, st.y);
            ctx.stroke();
          }

          const mainR = Math.min(w, h) * 0.25;
          drawArcReactor(w * 0.5, h * 0.54, mainR, rot);
          drawArcReactor(w * 0.88, h * 0.22, Math.min(w, h) * 0.11, -rot * 1.4);

          for (let i = 0; i < particles.length; i++) {
            const p = particles[i];
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < 0) p.x = w;
            if (p.x > w) p.x = 0;
            if (p.y < 0) p.y = h;
            if (p.y > h) p.y = 0;

            for (let j = i + 1; j < particles.length; j++) {
              const q = particles[j];
              const d = Math.hypot(p.x - q.x, p.y - q.y);
              if (d < 155) {
                const a = (1 - d / 155) * 0.16;
                ctx.strokeStyle = `rgba(56, 189, 248, ${a.toFixed(3)})`;
                ctx.lineWidth = 0.75;
                ctx.beginPath();
                ctx.moveTo(p.x, p.y);
                ctx.lineTo(q.x, q.y);
                ctx.stroke();
              }
            }

            ctx.fillStyle = `rgba(${p.hue}, 0.65)`;
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
            ctx.fill();
          }

          const baseWaveY = h * 0.91;
          ctx.strokeStyle = 'rgba(103, 232, 249, 0.22)';
          ctx.lineWidth = 1.4;
          ctx.beginPath();
          for (let x = 20; x < w - 20; x += 8) {
            const env = Math.sin((x / w) * Math.PI);
            const y = baseWaveY + Math.sin(x * 0.022 + tick * 0.06) * 16 * env + Math.cos(x * 0.05 - tick * 0.04) * 5 * env;
            if (x === 20) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
          }
          ctx.stroke();

          requestAnimationFrame(renderJarvis);
        }

        requestAnimationFrame(renderJarvis);
      })();
      function init() { try { state.trades = JSON.parse(localStorage.getItem(STORAGE_TRADES) || '[]'); } catch {} document.getElementById('fSym').innerHTML = STOCKS.map(s => `<option value="${s.symbol}">${s.symbol}</option>`).join(''); Array.from(new Set(STOCKS.map(s => s.sector))).sort().forEach(sec => { const o = document.createElement('option'); o.value = sec; o.textContent = sec; document.getElementById('sectorFilter').appendChild(o); }); document.querySelectorAll('.btn-tf').forEach(b => b.addEventListener('click', () => { state.timeframe = b.dataset.tf; document.querySelectorAll('.btn-tf').forEach(x => x.classList.toggle('active', x.dataset.tf === state.timeframe)); document.getElementById('headerTfBadge').textContent = tfDisplayLabel(state.timeframe); state.stocks.forEach(s => s.rsi = s.rsiMap[state.timeframe]); renderAll(); })); document.getElementById('tgTestBtn').addEventListener('click', async () => { const r = await fetch('/api/nifty-alert', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ test: true, timeframe: tfDisplayLabel(state.timeframe), long_th: state.longTh, short_th: state.shortTh }) }); const d = await r.json(); showToast(d.ok ? 'Test sent to Render TG Bots!' : 'Error: ' + d.error, d.ok ? 'long' : 'error'); }); document.addEventListener('click', e => { const b = e.target.closest('[data-quick-sym]'); if (b) { const s = state.stocks.get(b.dataset.quickSym); state.trades.push({ id: 'n50_' + Date.now(), symbol: b.dataset.quickSym, side: b.dataset.quickSide, entry: s?.price || 0, qty: 10, rsi: s?.rsi }); localStorage.setItem(STORAGE_TRADES, JSON.stringify(state.trades)); renderAll(); showToast(`Added ${b.dataset.quickSide} ${b.dataset.quickSym} to Portfolio!`, b.dataset.quickSide === 'LONG' ? 'long' : 'short'); } }); runScan(true); setInterval(() => runScan(false), 10000); }
      if (document.readyState === 'loading') window.addEventListener('DOMContentLoaded', init); else init();
    })();
  </script>
</body>
</html>"""

DASHBOARD_HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>Top 10 Crypto Scalping &amp; Futures Terminal (VWAP + 9/21 EMA + 14-RSI) — J.A.R.V.I.S.</title>
  <style>
    :root {
      --bg-main: #030914; --bg-header: rgba(5, 13, 27, 0.72); --bg-card: rgba(8, 18, 36, 0.34);
      --border: rgba(56, 189, 248, 0.22); --border-light: rgba(103, 232, 249, 0.35);
      --text-main: #f1f5f9; --text-muted: #94a3b8; --text-dim: #64748b;
      --green: #10b981; --green-bg: rgba(16, 185, 129, 0.16); --green-border: rgba(16, 185, 129, 0.42);
      --red: #f43f5e; --red-bg: rgba(244, 63, 94, 0.16); --red-border: rgba(244, 63, 94, 0.42);
      --amber: #f59e0b; --amber-bg: rgba(245, 158, 11, 0.16); --amber-border: rgba(245, 158, 11, 0.38);
      --cyan: #06b6d4; --cyan-light: #67e8f9; --cyan-bg: rgba(6, 182, 212, 0.16); --cyan-border: rgba(6, 182, 212, 0.38);
      --purple-light: #d8b4fe; --purple-bg: rgba(168, 85, 247, 0.16); --purple-border: rgba(168, 85, 247, 0.42);
      --radius: 10px;
      --font-sans: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Inter, Helvetica, Arial, sans-serif;
      --font-mono: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", "Courier New", monospace;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; }
    html, body { color: var(--text-main); font-family: var(--font-sans); font-size: 14px; line-height: 1.45; min-height: 100vh; }
    .mono { font-family: var(--font-mono); font-variant-numeric: tabular-nums; }
    .text-green { color: var(--green); } .text-red { color: var(--red); } .text-amber { color: var(--amber); }
    .text-cyan { color: var(--cyan-light); } .text-purple { color: var(--purple-light); } .text-muted { color: var(--text-muted); } .text-dim { color: var(--text-dim); }
    .text-right { text-align: right; } .text-center { text-align: center; } .hidden { display: none !important; }
    @keyframes pulseDot { 0%, 100% { opacity: 1; } 50% { opacity: 0.4; } }
    .pulse-dot { animation: pulseDot 1.4s infinite ease-in-out; }
    html, body {
      background-color: #030914 !important;
      background-image:
        radial-gradient(circle at 50% 22%, rgba(0, 212, 255, 0.11), transparent 48%),
        radial-gradient(circle at 18% 82%, rgba(16, 185, 129, 0.08), transparent 45%),
        radial-gradient(circle at 85% 78%, rgba(56, 189, 248, 0.09), transparent 45%) !important;
    }

    #sciFiBgCanvas {
      position: fixed;
      inset: 0;
      width: 100vw;
      height: 100vh;
      z-index: 0;
      pointer-events: none;
      opacity: 0.92;
    }

    .sci-fi-vignette {
      position: fixed;
      inset: 0;
      z-index: 0;
      pointer-events: none;
      background:
        radial-gradient(circle at 50% 35%, transparent 38%, rgba(2, 6, 15, 0.72) 100%),
        repeating-linear-gradient(
          0deg,
          rgba(56, 189, 248, 0.022) 0px,
          rgba(56, 189, 248, 0.022) 1px,
          transparent 1px,
          transparent 3px
        );
    }

    .header, .main, .toast-wrap {
      position: relative;
      z-index: 2;
    }

    .header {
      position: sticky;
      top: 0;
      z-index: 40;
      background: rgba(5, 13, 27, 0.62) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.24) !important;
      backdrop-filter: blur(14px) saturate(140%);
      box-shadow: 0 8px 32px rgba(0, 0, 0, 0.45), 0 0 24px rgba(6, 182, 212, 0.08);
    }

    .card, .kpi-card, .control-group, .live-bar, .tf-strip, .tg-panel {
      background: rgba(8, 18, 36, 0.34) !important;
      border-color: rgba(56, 189, 248, 0.22) !important;
      backdrop-filter: blur(10px) saturate(135%);
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.14),
        0 8px 28px rgba(2, 8, 20, 0.38);
    }

    .kpi-card:hover, .card:hover {
      border-color: rgba(103, 232, 249, 0.45) !important;
      box-shadow:
        inset 0 1px 0 rgba(125, 211, 252, 0.22),
        0 0 24px rgba(6, 182, 212, 0.14);
    }

    thead th {
      background: rgba(6, 15, 30, 0.72) !important;
      border-bottom: 1px solid rgba(56, 189, 248, 0.25) !important;
      color: #7dd3fc !important;
    }

    tbody tr {
      background: rgba(8, 18, 36, 0.18);
      border-bottom: 1px solid rgba(56, 189, 248, 0.11) !important;
      transition: background-color 0.15s;
    }

    tbody tr:hover {
      background: rgba(14, 116, 144, 0.22) !important;
    }

    tbody tr.row-long {
      background: rgba(16, 185, 129, 0.13) !important;
    }

    tbody tr.row-short {
      background: rgba(244, 63, 94, 0.13) !important;
    }

    .brand-icon {
      position: relative;
      overflow: hidden;
      background: linear-gradient(135deg, rgba(6, 182, 212, 0.35), rgba(16, 185, 129, 0.28)) !important;
      border: 1px solid rgba(103, 232, 249, 0.75) !important;
      box-shadow: 0 0 22px rgba(6, 182, 212, 0.55), inset 0 0 12px rgba(103, 232, 249, 0.35) !important;
    }
    .container { max-width: 1780px; margin: 0 auto; padding: 0 16px; }
    .header-inner { display: flex; flex-wrap: wrap; align-items: center; justify-content: space-between; gap: 12px; padding: 12px 0; }
    .brand { display: flex; align-items: center; gap: 10px; }
    .brand-icon { width: 38px; height: 38px; border-radius: 10px; display: flex; align-items: center; justify-content: center; flex-shrink: 0; }
    .brand-title { font-size: 16px; font-weight: 700; display: flex; align-items: center; flex-wrap: wrap; gap: 6px; }
    .brand-sub { font-size: 11px; color: var(--text-muted); margin-top: 1px; }
    .pill { display: inline-flex; align-items: center; gap: 6px; padding: 2px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid var(--border); }
    .pill-tf { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .pill-cloud { background: var(--green-bg); color: #6ee7b7; border-color: var(--green-border); font-size: 10px; }
    .live-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; border: 1px solid var(--border); border-radius: 999px; padding: 5px 12px; font-size: 12px; }
    .status-dot { width: 8px; height: 8px; border-radius: 50%; background: var(--green); box-shadow: 0 0 8px rgba(16, 185, 129, 0.75); }
    .live-sep { color: var(--border-light); }
    .controls-bar { display: flex; flex-wrap: wrap; align-items: center; gap: 8px; padding: 10px 0; border-top: 1px solid rgba(56, 189, 248, 0.16); }
    .control-group { display: inline-flex; align-items: center; gap: 6px; border: 1px solid var(--border); border-radius: 8px; padding: 4px 8px; font-size: 12px; }
    input[type="number"] { background: rgba(6, 15, 30, 0.75); color: var(--text-main); border: 1px solid var(--border-light); border-radius: 6px; padding: 3px 4px; width: 52px; text-align: center; font-weight: 700; }
    .btn { display: inline-flex; align-items: center; justify-content: center; gap: 6px; padding: 6px 12px; border-radius: 8px; font-size: 12px; font-weight: 600; cursor: pointer; border: 1px solid var(--border-light); background: rgba(15, 23, 42, 0.65); color: var(--text-main); text-decoration: none; transition: all 0.15s; }
    .btn:hover { background: rgba(14, 116, 144, 0.35); border-color: #67e8f9; }
    .btn-tf { padding: 5px 10px; border: 1px solid transparent; background: transparent; color: var(--text-muted); border-radius: 6px; }
    .btn-tf.active { background: var(--green-bg); color: #34d399; border-color: var(--green-border); }
    .btn-vwap.active { background: var(--amber-bg); color: #fcd34d; border-color: var(--amber-border); }
    .btn-funding.active { background: var(--purple-bg); color: var(--purple-light); border-color: var(--purple-border); }
    .btn-scalp.active { background: var(--green-bg); color: #6ee7b7; border-color: var(--green-border); }
    .btn-tg.active { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .btn-long { padding: 4px 8px; font-size: 11px; background: var(--green-bg); color: #34d399; border-color: var(--green-border); border-radius: 6px; }
    .btn-short { padding: 4px 8px; font-size: 11px; background: var(--red-bg); color: #fb7185; border-color: var(--red-border); border-radius: 6px; }
    .main { padding: 16px 0 36px; }
    .kpi-grid { display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 12px; margin-bottom: 16px; }
    @media (max-width: 1320px) { .kpi-grid { grid-template-columns: repeat(3, minmax(0, 1fr)); } }
    @media (max-width: 680px) { .kpi-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
    .kpi-card { border: 1px solid var(--border); border-radius: var(--radius); padding: 13px 14px; display: flex; flex-direction: column; justify-content: space-between; }
    .kpi-top { display: flex; align-items: center; justify-content: space-between; font-size: 11px; font-weight: 600; text-transform: uppercase; color: var(--text-muted); }
    .kpi-val { font-size: 20px; font-weight: 700; margin-top: 6px; display: flex; align-items: baseline; justify-content: space-between; }
    .kpi-sub { font-size: 11px; color: var(--text-muted); margin-top: 4px; }
    .card { border: 1px solid var(--border); border-radius: var(--radius); overflow: hidden; }
    .table-wrap { overflow-x: auto; width: 100%; }
    table { width: 100%; border-collapse: collapse; white-space: nowrap; font-size: 13px; }
    thead th { padding: 11px 12px; font-size: 11px; font-weight: 600; text-transform: uppercase; }
    tbody td { padding: 10px 12px; vertical-align: middle; }
    .badge { display: inline-block; padding: 3px 8px; border-radius: 6px; font-size: 11px; font-weight: 700; border: 1px solid transparent; }
    .badge-long { background: var(--green-bg); color: var(--green); border-color: var(--green-border); }
    .badge-short { background: var(--red-bg); color: var(--red); border-color: var(--red-border); }
    .badge-wait { background: var(--amber-bg); color: var(--amber); border-color: var(--amber-border); }
    .badge-cyan { background: var(--cyan-bg); color: var(--cyan-light); border-color: var(--cyan-border); }
    .badge-neutral { background: rgba(30, 41, 59, 0.65); color: #cbd5e1; border-color: #334155; }
    .toast-wrap { position: fixed; bottom: 16px; right: 16px; z-index: 60; display: flex; flex-direction: column; gap: 8px; }
    .toast { padding: 10px 14px; border-radius: 10px; font-size: 12px; font-weight: 600; background: rgba(8, 18, 36, 0.9); border: 1px solid var(--border-light); color: var(--text-main); }
  </style>
</head>
<body>
  <canvas id="sciFiBgCanvas"></canvas>
  <div class="sci-fi-vignette"></div>
  <header class="header">
    <div class="container">
      <div class="header-inner">
        <div class="brand">
          <div class="brand-icon" title="J.A.R.V.I.S. Quantum Scalping & Futures Terminal">
            <svg width="26" height="26" viewBox="0 0 36 36" fill="none" xmlns="http://www.w3.org/2000/svg"><polygon points="18,2 32,10 32,26 18,34 4,26 4,10" fill="#04131c" stroke="#67e8f9" stroke-width="1.5"/><circle cx="18" cy="18" r="10.5" stroke="#34d399" stroke-width="1.2" stroke-dasharray="4 3"/><path d="M9 21L14 15L18.5 19.5L27 11" stroke="#67e8f9" stroke-width="2.4" stroke-linecap="round" stroke-linejoin="round"/><circle cx="27" cy="11" r="2.5" fill="#10b981"/></svg>
          </div>
          <div>
            <div class="brand-title">
              <span>Top 10 Crypto Scalping &amp; Futures Terminal (VWAP + 9/21 EMA + 14-RSI)</span>
              <span id="headerTfBadge" class="pill pill-tf mono">15M</span>
              <span class="pill pill-cloud mono">J.A.R.V.I.S. • 2S RENDER</span>
              <a href="/nifty" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--amber-border); color: #fcd34d;">🇮🇳 Nifty 50 NSE</a>
              <a href="/portfolio" class="btn" style="padding: 3px 10px; font-size: 11px; border-color: var(--purple-border); color: var(--purple-light);">💼 Live Portfolio →</a>
            </div>
            <div class="brand-sub">Har 2 Second Live Scan • Session VWAP (UTC) + 9/21 EMA Pullback Engine + Wilder's 14 RSI + 8h Funding + 24/7 TG Bot</div>
          </div>
        </div>
        <div class="live-bar">
          <span class="status-dot pulse-dot"></span>
          <strong id="liveFeedName" class="mono">CONNECTING...</strong>
          <span class="live-sep">|</span>
          <span class="text-muted">2s Scans: <strong id="liveScanCount" class="mono text-cyan">0</strong></span>
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
        <button id="vwapFilterToggleBtn" type="button" class="btn btn-vwap active"><span id="vwapFilterLabel">⚡ VWAP Filter: ON</span></button>
        <button id="fundingFilterToggleBtn" type="button" class="btn btn-funding active"><span id="fundingFilterLabel">💰 Funding Filter: ON</span></button>
        <button id="scalpAlertToggleBtn" type="button" class="btn btn-scalp active"><span id="scalpAlertLabel">🎯 9/21 EMA Scalp TG: ON</span></button>
        <button id="tgTestBtn" type="button" class="btn btn-tg active"><span id="tgStatusLabel">✈️ 24/7 TG Bot: Test</span></button>
      </div>
    </div>
  </header>
  <main class="main container">
    <section class="kpi-grid">
      <div class="kpi-card"><div class="kpi-top"><span>Render 2s Sync</span><span id="kpiTfTag" class="mono text-cyan">15m</span></div><div id="kpiSyncTime" class="kpi-val mono" style="font-size: 16px;">--:--:-- IST</div><div id="kpiFeedDetail" class="kpi-sub">Loading...</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Scalping Pullbacks</span><span class="text-cyan">9/21 EMA + VWAP</span></div><div class="kpi-val mono"><span id="kpiScalpCount" class="text-cyan">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div id="kpiScalpSub" class="kpi-sub">Scanning pullback zone...</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>RSI Long Signals</span><span class="text-green">LIVE</span></div><div class="kpi-val mono"><span id="kpiLongCount" class="text-green">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div class="kpi-sub">Oversold + VWAP + Funding</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>RSI Short Signals</span><span class="text-red">LIVE</span></div><div class="kpi-val mono"><span id="kpiShortCount" class="text-red">0</span><span class="text-dim" style="font-size: 12px;">/ 10</span></div><div class="kpi-sub">Overbought + VWAP + Funding</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Oversold</span><span class="text-green mono">Lowest RSI</span></div><div class="kpi-val"><span id="kpiOversoldCoin" style="font-size: 16px;">--</span><span id="kpiOversoldRsi" class="mono text-green">--</span></div><div id="kpiOversoldPrice" class="kpi-sub mono">Mark: --</div></div>
      <div class="kpi-card"><div class="kpi-top"><span>Most Overbought</span><span class="text-red mono">Highest RSI</span></div><div class="kpi-val"><span id="kpiOverboughtCoin" style="font-size: 16px;">--</span><span id="kpiOverboughtRsi" class="mono text-red">--</span></div><div id="kpiOverboughtPrice" class="kpi-sub mono">Mark: --</div></div>
    </section>
    <div class="card"><div class="table-wrap"><table><thead><tr><th>Rank</th><th>Pair</th><th class="text-right">Mark Price</th><th class="text-right">Session VWAP (UTC)</th><th class="text-right">9 / 21 EMA</th><th class="text-right">14-RSI</th><th class="text-center">Scalp Setup (EMA+VWAP)</th><th class="text-center">RSI Signal</th><th class="text-right">8h Funding</th><th class="text-right">Target ±6.28%</th><th class="text-right">Hard SL ±2.0%</th><th class="text-center">Portfolio Trade</th></tr></thead><tbody id="screenerTbody"><tr><td colspan="12" style="padding:28px;text-align:center;">Connecting to 2-second Render feed...</td></tr></tbody></table></div></div>
  </main>
  <div id="toastWrap" class="toast-wrap"></div>
  <script>
    (() => {
      'use strict';
      const STORAGE_TRADES = 'top10_futures_rsi_v3_trades';
      let cfg = { timeframe: '15m', long_th: 35, short_th: 70, vwap_filter: true, funding_filter: true, scalp_alerts: true };
      let coins = [];
      const fmtP = v => v == null ? '--' : v.toLocaleString('en-US', { minimumFractionDigits: Math.abs(v) < 1 ? 4 : 2, maximumFractionDigits: Math.abs(v) < 1 ? 4 : 2 });
      const fmtPct = v => v == null ? '--' : (v > 0 ? '+' : '') + v.toFixed(2) + '%';
      function toast(m) { const w = document.getElementById('toastWrap'); const e = document.createElement('div'); e.className = 'toast'; e.textContent = m; w.appendChild(e); setTimeout(() => e.remove(), 3000); }
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
            document.getElementById('headerTfBadge').textContent = cfg.timeframe.toUpperCase();
            document.getElementById('kpiTfTag').textContent = cfg.timeframe;
            document.querySelectorAll('.btn-tf').forEach(b => b.classList.toggle('active', b.dataset.tf === cfg.timeframe));
            document.getElementById('vwapFilterLabel').textContent = `⚡ VWAP Filter: ${cfg.vwap_filter ? 'ON' : 'OFF'}`;
            document.getElementById('fundingFilterLabel').textContent = `💰 Funding Filter: ${cfg.funding_filter ? 'ON' : 'OFF'}`;
            document.getElementById('scalpAlertLabel').textContent = `🎯 9/21 EMA Scalp TG: ${cfg.scalp_alerts ? 'ON' : 'OFF'}`;
          }
          if (Array.isArray(d.coins)) {
            coins = d.coins;
            const valid = coins.filter(c => c.rsi != null);
            const scalpHits = valid.filter(c => c.scalpCode === 'SCALP_LONG' || c.scalpCode === 'SCALP_SHORT');
            document.getElementById('kpiScalpCount').textContent = scalpHits.length;
            document.getElementById('kpiScalpSub').textContent = scalpHits.length
              ? scalpHits.map(x => `${x.coin} (${x.scalpCode === 'SCALP_LONG' ? 'LONG' : 'SHORT'})`).join(', ')
              : 'Waiting for 21 EMA / VWAP pullback';
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
            const priority = { SCALP_LONG: 0, SCALP_SHORT: 0, LONG: 1, SHORT: 1, BULL_TREND: 2, BEAR_TREND: 2, CHOP: 3, WAIT: 4 };
            const sorted = coins.slice().sort((a, b) => {
              const pa = priority[a.scalpCode] ?? 4;
              const pb = priority[b.scalpCode] ?? 4;
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
                : c.scalpCode === 'BULL_TREND' || c.scalpCode === 'BEAR_TREND' ? 'badge-cyan'
                : 'badge-neutral';
              const rowCls = (c.signal === 'LONG' || c.scalpCode === 'SCALP_LONG') ? 'row-long'
                : (c.signal === 'SHORT' || c.scalpCode === 'SCALP_SHORT') ? 'row-short'
                : '';
              const emaBull = c.ema9 != null && c.ema21 != null && c.ema9 >= c.ema21;
              return `<tr class="${rowCls}">
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
                <td class="text-center"><span class="badge ${cls}">${c.signal}</span></td>
                <td class="text-right mono">${c.funding != null ? (c.funding * 100).toFixed(4) + '%' : '—'}</td>
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
      document.getElementById('vwapFilterToggleBtn').addEventListener('click', () => pushCfg({ vwap_filter: !cfg.vwap_filter }));
      document.getElementById('fundingFilterToggleBtn').addEventListener('click', () => pushCfg({ funding_filter: !cfg.funding_filter }));
      document.getElementById('scalpAlertToggleBtn').addEventListener('click', () => pushCfg({ scalp_alerts: !cfg.scalp_alerts }));
      document.getElementById('tgTestBtn').addEventListener('click', async () => { const r = await fetch('/api/test-tg', { method: 'POST' }); const d = await r.json(); toast(d.ok ? 'Test sent to Render TG Bots!' : 'Error: ' + d.error); });
      document.addEventListener('click', e => {
        const b = e.target.closest('[data-add-c]');
        if (b) {
          const c = coins.find(x => x.coin === b.dataset.addC);
          if (!c) return;
          const trades = JSON.parse(localStorage.getItem(STORAGE_TRADES) || '[]');
          trades.push({ id: 'ft_' + Date.now(), coin: c.coin, side: b.dataset.addS, mode: 'single', entry: c.price, margin: 100, lev: 5, rsi: c.rsi });
          localStorage.setItem(STORAGE_TRADES, JSON.stringify(trades));
          toast(`Added ${b.dataset.addS} ${c.pair} ($100 • 5x) to Portfolio!`);
        }
      });
      (function initJarvisSimulation() {
        const canvas = document.getElementById('sciFiBgCanvas');
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        let w = 0, h = 0, dpr = Math.min(window.devicePixelRatio || 1, 2);
        let tick = 0;

        const particles = [];
        const streams = [];
        const PARTICLE_COUNT = 54;
        const STREAM_COUNT = 9;

        function resize() {
          w = window.innerWidth;
          h = window.innerHeight;
          canvas.width = Math.max(1, w) * dpr;
          canvas.height = Math.max(1, h) * dpr;
          ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        }

        function initEntities() {
          particles.length = 0;
          for (let i = 0; i < PARTICLE_COUNT; i++) {
            particles.push({
              x: Math.random() * (w || 1440),
              y: Math.random() * (h || 900),
              vx: (Math.random() - 0.5) * 0.42,
              vy: (Math.random() - 0.5) * 0.42,
              size: 1 + Math.random() * 2,
              hue: i % 3 === 0 ? '16, 185, 129' : '56, 189, 248'
            });
          }

          streams.length = 0;
          for (let s = 0; s < STREAM_COUNT; s++) {
            streams.push({
              x: ((s + 0.5) / STREAM_COUNT) * (w || 1440),
              y: Math.random() * (h || 900),
              len: 45 + Math.random() * 95,
              speed: 1.1 + Math.random() * 1.9,
              alpha: 0.08 + Math.random() * 0.14
            });
          }
        }

        function drawArcReactor(cx, cy, radius, rot) {
          ctx.save();
          ctx.translate(cx, cy);

          const glow = ctx.createRadialGradient(0, 0, radius * 0.15, 0, 0, radius * 1.35);
          glow.addColorStop(0, 'rgba(56, 189, 248, 0.14)');
          glow.addColorStop(0.55, 'rgba(6, 182, 212, 0.05)');
          glow.addColorStop(1, 'rgba(6, 182, 212, 0)');
          ctx.fillStyle = glow;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 1.35, 0, Math.PI * 2);
          ctx.fill();

          ctx.save();
          ctx.rotate(rot * 0.65);
          const segs = 12;
          for (let i = 0; i < segs; i++) {
            const a0 = (i / segs) * Math.PI * 2 + 0.05;
            const a1 = ((i + 1) / segs) * Math.PI * 2 - 0.05;
            ctx.strokeStyle = i % 3 === 0 ? 'rgba(16, 185, 129, 0.28)' : 'rgba(103, 232, 249, 0.24)';
            ctx.lineWidth = 2;
            ctx.beginPath();
            ctx.arc(0, 0, radius, a0, a1);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(-rot * 1.25);
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.22)';
          ctx.lineWidth = 1.2;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.76, 0, Math.PI * 2);
          ctx.stroke();

          for (let k = 0; k < 24; k++) {
            const ang = (k / 24) * Math.PI * 2;
            const rIn = radius * (k % 2 === 0 ? 0.69 : 0.72);
            const rOut = radius * 0.76;
            ctx.strokeStyle = k % 6 === 0 ? 'rgba(110, 231, 183, 0.45)' : 'rgba(125, 211, 252, 0.26)';
            ctx.lineWidth = k % 6 === 0 ? 1.8 : 1;
            ctx.beginPath();
            ctx.moveTo(Math.cos(ang) * rIn, Math.sin(ang) * rIn);
            ctx.lineTo(Math.cos(ang) * rOut, Math.sin(ang) * rOut);
            ctx.stroke();
          }
          ctx.restore();

          ctx.save();
          ctx.rotate(rot * 2.4);
          const sweepGrad = ctx.createConicGradient(0, 0);
          sweepGrad.addColorStop(0, 'rgba(103, 232, 249, 0.28)');
          sweepGrad.addColorStop(0.14, 'rgba(16, 185, 129, 0.08)');
          sweepGrad.addColorStop(0.15, 'rgba(56, 189, 248, 0)');
          sweepGrad.addColorStop(1, 'rgba(56, 189, 248, 0)');
          ctx.fillStyle = sweepGrad;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.66, 0, Math.PI * 2);
          ctx.fill();
          ctx.restore();

          ctx.strokeStyle = 'rgba(125, 211, 252, 0.35)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          ctx.arc(0, 0, radius * 0.26, 0, Math.PI * 2);
          ctx.stroke();

          ctx.restore();
        }

        resize();
        initEntities();
        window.addEventListener('resize', () => {
          resize();
          initEntities();
        });

        function renderJarvis() {
          tick += 1;
          const rot = tick * 0.006;
          ctx.clearRect(0, 0, w, h);

          const step = 72;
          ctx.strokeStyle = 'rgba(56, 189, 248, 0.04)';
          ctx.lineWidth = 1;
          ctx.beginPath();
          for (let x = 0; x < w; x += step) {
            ctx.moveTo(x, 0);
            ctx.lineTo(x, h);
          }
          for (let y = 0; y < h; y += step) {
            ctx.moveTo(0, y);
            ctx.lineTo(w, y);
          }
          ctx.stroke();

          for (let i = 0; i < streams.length; i++) {
            const st = streams[i];
            st.y += st.speed;
            if (st.y - st.len > h) {
              st.y = -20;
              st.x = ((i + 0.5) / STREAM_COUNT) * w + (Math.random() - 0.5) * 40;
            }
            const g = ctx.createLinearGradient(st.x, st.y - st.len, st.x, st.y);
            g.addColorStop(0, 'rgba(56, 189, 248, 0)');
            g.addColorStop(0.75, `rgba(56, 189, 248, ${st.alpha})`);
            g.addColorStop(1, 'rgba(110, 231, 183, 0.45)');
            ctx.strokeStyle = g;
            ctx.lineWidth = 1.2;
            ctx.beginPath();
            ctx.moveTo(st.x, st.y - st.len);
            ctx.lineTo(st.x, st.y);
            ctx.stroke();
          }

          const mainR = Math.min(w, h) * 0.25;
          drawArcReactor(w * 0.5, h * 0.54, mainR, rot);
          drawArcReactor(w * 0.88, h * 0.22, Math.min(w, h) * 0.11, -rot * 1.4);

          for (let i = 0; i < particles.length; i++) {
            const p = particles[i];
            p.x += p.vx;
            p.y += p.vy;
            if (p.x < 0) p.x = w;
            if (p.x > w) p.x = 0;
            if (p.y < 0) p.y = h;
            if (p.y > h) p.y = 0;

            for (let j = i + 1; j < particles.length; j++) {
              const q = particles[j];
              const d = Math.hypot(p.x - q.x, p.y - q.y);
              if (d < 155) {
                const a = (1 - d / 155) * 0.16;
                ctx.strokeStyle = `rgba(56, 189, 248, ${a.toFixed(3)})`;
                ctx.lineWidth = 0.75;
                ctx.beginPath();
                ctx.moveTo(p.x, p.y);
                ctx.lineTo(q.x, q.y);
                ctx.stroke();
              }
            }

            ctx.fillStyle = `rgba(${p.hue}, 0.65)`;
            ctx.beginPath();
            ctx.arc(p.x, p.y, p.size, 0, Math.PI * 2);
            ctx.fill();
          }

          const baseWaveY = h * 0.91;
          ctx.strokeStyle = 'rgba(103, 232, 249, 0.22)';
          ctx.lineWidth = 1.4;
          ctx.beginPath();
          for (let x = 20; x < w - 20; x += 8) {
            const env = Math.sin((x / w) * Math.PI);
            const y = baseWaveY + Math.sin(x * 0.022 + tick * 0.06) * 16 * env + Math.cos(x * 0.05 - tick * 0.04) * 5 * env;
            if (x === 20) ctx.moveTo(x, y);
            else ctx.lineTo(x, y);
          }
          ctx.stroke();

          requestAnimationFrame(renderJarvis);
        }

        requestAnimationFrame(renderJarvis);
      })();
      poll();
      setInterval(poll, 1500);
    })();
  </script>
</body>
</html>"""


# =============================================================================
# 8. HTTP SERVER ROUTES ("/", "/nifty", "/portfolio", "/api/state", "/api/nifty-alert")
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

    def _send_html(self, html_str: str):
        html_bytes = html_str.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.send_header("Content-Length", str(len(html_bytes)))
        self.end_headers()
        self.wfile.write(html_bytes)

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

        if path in ("/nifty", "/nifty50", "/nifty-50"):
            self._send_html(NIFTY_HTML)
            return

        if path in ("/portfolio", "/pnl"):
            self._send_html(PORTFOLIO_HTML)
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

        self._send_html(DASHBOARD_HTML)

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
                if "scalp_alerts" in body:
                    CONFIG["scalp_alerts"] = bool(body["scalp_alerts"])
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
                f"✅ <b>J.A.R.V.I.S. 2-Second Live Scalping Terminal Test OK!</b>\n"
                f"TF: <b>{cfg['timeframe'].upper()}</b> | Long &lt; <b>{cfg['long_th']}</b> | Short &gt; <b>{cfg['short_th']}</b>\n"
                f"VWAP: <b>{'ON' if cfg['vwap_filter'] else 'OFF'}</b> | "
                f"Funding: <b>{'ON' if cfg['funding_filter'] else 'OFF'}</b> | "
                f"9/21 EMA Scalp: <b>{'ON' if cfg['scalp_alerts'] else 'OFF'}</b>\n"
                f"⏰ {now_ist_str()}"
            )
            self._send_json({"ok": ok, "error": None if ok else err})
            return

        if path == "/api/nifty-alert":
            if body.get("test"):
                tf_lbl = str(body.get("timeframe", "15M"))
                l_th = body.get("long_th", 35)
                s_th = body.get("short_th", 70)
                ok, err = send_telegram(
                    f"✅ <b>Nifty 50 Page Connected to J.A.R.V.I.S. TG Bot!</b>\n"
                    f"Timeframe: <b>{tf_lbl}</b> | Long &lt; <b>{l_th}</b> | Short &gt; <b>{s_th}</b>\n"
                    f"⏰ {now_ist_str()}"
                )
                self._send_json({"ok": ok, "error": None if ok else err})
                return

            sym = str(body.get("symbol", "")).strip()
            side = str(body.get("side", "")).strip().upper()
            tf_lbl = str(body.get("timeframe", "15M")).strip()
            if not sym or side not in ("LONG", "SHORT"):
                self._send_json({"ok": False, "error": "Invalid Nifty alert payload"}, status=400)
                return

            with STATE_LOCK:
                cooldown_sec = max(1, CONFIG["cooldown_min"]) * 60
                key = (sym, side, tf_lbl)
                now_ts = time.time()
                if now_ts - NIFTY_COOLDOWNS.get(key, 0) < cooldown_sec:
                    self._send_json({"ok": False, "cooldown": True})
                    return
                NIFTY_COOLDOWNS[key] = now_ts

            price = float(body.get("price") or 0)
            chg = float(body.get("changePct") or 0)
            rsi = float(body.get("rsi") or 0)
            name = str(body.get("name", sym))
            sector = str(body.get("sector", "NSE"))
            is_long = side == "LONG"
            icon = "🟢" if is_long else "🔴"
            tp = price * 1.0628 if is_long else price * 0.9372
            avg = price * 0.9686 if is_long else price * 1.0314

            msg = (
                f"{icon} <b>NIFTY 50 {side}: {sym}</b> ({tf_lbl})\n"
                f"━━━━━━━━━━━━━━━━━━\n"
                f"🏢 <b>{name}</b> ({sector})\n"
                f"📊 <b>RSI (14):</b> {rsi:.2f}\n"
                f"💵 <b>Live LTP:</b> ₹{fmt_price(price)} ({fmt_pct(chg)})\n"
                f"🎯 <b>Target (2π):</b> ₹{fmt_price(tp)}\n"
                f"🔁 <b>Next Avg (π):</b> ₹{fmt_price(avg)}\n"
                f"⏰ <b>Time:</b> {now_ist_str()}"
            )
            ok, err = send_telegram(msg)
            if ok:
                with STATE_LOCK:
                    SHARED_STATE["alerts_sent"] += 1
                    SHARED_STATE["alert_history"].insert(0, {
                        "time": datetime.now(IST).strftime("%d %b %H:%M:%S IST"),
                        "pair": f"{sym} (NSE)",
                        "side": side,
                        "tf": tf_lbl,
                        "price": price,
                        "rsi": rsi,
                        "vwap": None,
                        "funding": None,
                    })
                    SHARED_STATE["alert_history"] = SHARED_STATE["alert_history"][:30]
                print(f"[{now_ist_str()}] >>> NIFTY 50 ALERT SENT: {side} {sym} (RSI {rsi:.2f})")
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
            f"✅ <b>J.A.R.V.I.S. 3-Page Live Scalping Terminal Started!</b>\n"
            f"Crypto (VWAP+9/21 EMA+RSI): <b>/</b> | Nifty 50: <b>/nifty</b> | Portfolio: <b>/portfolio</b>\n"
            f"⏰ {now_ist_str()}"
        )

    server = ThreadingHTTPServer(("0.0.0.0", PORT), WebTerminalHandler)
    print(f"[{now_ist_str()}] J.A.R.V.I.S. Terminal (/, /nifty, /portfolio) listening on port {PORT}")
    server.serve_forever()


if __name__ == "__main__":
    main()
