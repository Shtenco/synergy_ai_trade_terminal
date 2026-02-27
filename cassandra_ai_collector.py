#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║         CASSANDRA · AI TRAINING DATA COLLECTOR                              ║
║         Lite-Invest API  |  MOEX / FORTS                                    ║
║         © 2024-2026 E.Koshtenko                                             ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  ИСТОЧНИКИ ДАННЫХ:                                                           ║
║  1. /hubs/tick        → тики + направление + объём                          ║
║     ↳ ChangePrice     → % изменения за день                                 ║
║  2. /hubs/cluster     → объём на каждом уровне цены (footprint chart)       ║
║  3. /Quotes/{id}/0    → стакан L2 (bid/ask по уровням)                      ║
║  4. /api/Positions/close → закрытые позиции = МЕТКИ (labels)                ║
║  5. /hubs/finrez      → equity curve (PnL, просадка, комиссия)              ║
║                                                                              ║
║  ВЫЧИСЛЯЕМЫЕ ПРИЗНАКИ (features):                                            ║
║  ─ OHLCV свечи (1m/5m/15m/1h) из тиков                                     ║
║  ─ Cumulative Delta = buy_vol − sell_vol                                    ║
║  ─ POC (Point of Control) из кластеров                                      ║
║  ─ Volume Imbalance по уровням (buy% vs sell%)                              ║
║  ─ Bid/Ask Pressure из стакана                                               ║
║  ─ Spread dynamics                                                           ║
║  ─ Tick direction (up/dn runs)                                              ║
║  ─ RSI, EMA, VWAP, ATR                                                      ║
║  ─ Liquidity gap (пустые уровни)                                            ║
║                                                                              ║
║  ВЫХОДНЫЕ ФОРМАТЫ:                                                           ║
║  • CSV   — pandas/sklearn                                                   ║
║  • Parquet — columnar, быстрое чтение                                       ║
║  • NPZ   — numpy arrays для нейросетей                                      ║
║  • JSONL — построчный JSON для LLM fine-tuning                              ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""

import asyncio
import json
import csv
import math
import time
import signal
import logging
import argparse
import threading
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Optional, Tuple, Any
from pathlib import Path

import requests
import numpy as np

# ─────────────────────────────────────────────────────────────────────────────
# ЗАВИСИМОСТИ  (pip install signalrcore pandas pyarrow numpy requests)
# ─────────────────────────────────────────────────────────────────────────────
try:
    from signalrcore.hub_connection_builder import HubConnectionBuilder
    SIGNALR_OK = True
except ImportError:
    SIGNALR_OK = False
    print("[WARN] signalrcore не установлен. Только REST polling.")
    print("       pip install signalrcore")

try:
    import pandas as pd
    PANDAS_OK = True
except ImportError:
    PANDAS_OK = False
    print("[WARN] pandas не установлен. Только CSV.")
    print("       pip install pandas pyarrow")

# ════════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ════════════════════════════════════════════════════════════════════════════════

BASE_URL  = "https://api-cassandra.lite-invest.com"
USERNAME  = "Hasanov777@ya.ru"
PASSWORD  = "Hasanov777!"
TIMEOUT   = 30

# Тикеры для сбора данных (можно задать через аргументы)
DEFAULT_TICKERS = ["SBER", "GAZP", "LKOH", "YNDX", "ROSN"]

# Таймфреймы для свечей (в секундах)
TIMEFRAMES = {
    "1m":  60,
    "5m":  300,
    "15m": 900,
    "1h":  3600,
}

# Глубина скользящего окна для признаков
WINDOW_SIZES = [5, 10, 20, 50, 100, 200]

# Директория для сохранения данных
DATA_DIR = Path("ai_training_data")

# Интервал снапшота стакана (секунды)
ORDERBOOK_INTERVAL = 2.0

# Интервал загрузки закрытых позиций (секунды)
POSITIONS_INTERVAL = 30.0

# ════════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ════════════════════════════════════════════════════════════════════════════════

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%H:%M:%S'
)
log = logging.getLogger('AI-COLLECTOR')

# ════════════════════════════════════════════════════════════════════════════════
# СТРУКТУРЫ ДАННЫХ
# ════════════════════════════════════════════════════════════════════════════════

@dataclass
class Tick:
    """Один тик из /hubs/tick"""
    ts:        int            # unix ms
    price:     float          # нормализованная цена / 100
    volume:    float          # объём
    side:      int            # 0=buy, 1=sell
    ticker:    str
    security_id: str

@dataclass
class Candle:
    """OHLCV свеча + расширенные поля"""
    tf:          str
    ts_open:     int           # unix ms начала свечи
    ts_close:    int           # unix ms закрытия свечи
    ticker:      str
    security_id: str
    open:        float
    high:        float
    low:         float
    close:       float
    volume:      float
    buy_vol:     float         # объём покупок
    sell_vol:    float         # объём продаж
    cum_delta:   float         # buy_vol - sell_vol
    tick_count:  int           # число тиков
    buy_ticks:   int           # тики вверх
    sell_ticks:  int           # тики вниз
    vwap:        float         # VWAP свечи
    # Метки (заполняются post-factum)
    label_1bar:  Optional[float] = None   # return след. свечи
    label_5bar:  Optional[float] = None   # return через 5 свечей
    label_tp:    Optional[int]   = None   # 1 если TP достигнут
    label_sl:    Optional[int]   = None   # 1 если SL достигнут

@dataclass
class ClusterLevel:
    """Один ценовой уровень кластера"""
    price:    float
    ask_vol:  float   # объём продаж на уровне (a)
    bid_vol:  float   # объём покупок на уровне (b)
    delta:    float   # bid_vol - ask_vol (net buying pressure)
    imbalance: float  # (bid_vol - ask_vol) / (bid_vol + ask_vol)

@dataclass
class ClusterBar:
    """Полный кластерный бар (footprint)"""
    ts:          str
    ticker:      str
    security_id: str
    timeframe:   str
    open:        float
    close:       float
    high:        float
    low:         float
    levels:      List[ClusterLevel]
    poc_price:   float   # Point of Control — уровень с макс объёмом
    poc_vol:     float
    total_vol:   float
    buy_vol:     float
    sell_vol:    float
    cum_delta:   float
    max_imbalance: float  # максимальный дисбаланс в баре
    is_bullish:  bool    # cl >= op

@dataclass
class OrderBookSnapshot:
    """Снапшот стакана L2"""
    ts:          int
    ticker:      str
    security_id: str
    bid:         float   # лучший bid
    ask:         float   # лучший ask
    spread:      float   # спред
    depth:       int     # глубина стакана
    bid_vol_top5: float  # суммарный bid объём top-5 уровней
    ask_vol_top5: float  # суммарный ask объём top-5 уровней
    imbalance:   float   # (bid_vol - ask_vol) / (bid_vol + ask_vol)
    # полная глубина
    bids: List[Tuple[float, float]] = field(default_factory=list)  # [(price, vol)]
    asks: List[Tuple[float, float]] = field(default_factory=list)

@dataclass
class ClosedPosition:
    """Закрытая позиция = supervised label"""
    position_id:   str
    security_id:   str
    security_name: str
    open_time:     str
    close_time:    str
    side:          int     # 0=Long, 1=Short
    avg_entry:     float
    avg_exit:      float
    pnl_points:    float
    pnl_pct:       float
    outcome:       int     # 1=win, 0=loss, -1=breakeven
    holding_mins:  float

# ════════════════════════════════════════════════════════════════════════════════
# API CLIENT
# ════════════════════════════════════════════════════════════════════════════════

class CassandraAPIClient:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json", "Content-Type": "application/json"})
        self._token: Optional[str] = None
        self._token_exp: float = 0.0

    def _is_expired(self) -> bool:
        return time.time() > self._token_exp - 60

    def authenticate(self) -> bool:
        try:
            r = self.session.post(
                f"{BASE_URL}/api/Authentication/login",
                json={"email": USERNAME, "password": PASSWORD},
                timeout=TIMEOUT
            )
            r.raise_for_status()
            data = r.json()
            tok = data.get("accessToken")
            if not tok:
                log.error("No accessToken in auth response")
                return False
            self._token = tok
            # Decode JWT exp
            try:
                import base64
                payload = json.loads(base64.b64decode(tok.split('.')[1] + '=='))
                self._token_exp = float(payload.get('exp', time.time() + 3600))
            except Exception:
                self._token_exp = time.time() + 3600
            self.session.headers["Authorization"] = f"Bearer {tok}"
            log.info(f"Authenticated. Token valid until {datetime.fromtimestamp(self._token_exp).strftime('%H:%M:%S')}")
            return True
        except Exception as e:
            log.error(f"Auth failed: {e}")
            return False

    def ensure_token(self):
        if not self._token or self._is_expired():
            self.authenticate()

    def get(self, path: str, **kwargs) -> Optional[Any]:
        self.ensure_token()
        try:
            r = self.session.get(f"{BASE_URL}{path}", timeout=TIMEOUT, **kwargs)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            log.warning(f"GET {path}: {e}")
            return None

    @property
    def token(self) -> Optional[str]:
        self.ensure_token()
        return self._token

    # ── Endpoints ──────────────────────────────────────────────────────────

    def get_boards(self) -> List[Dict]:
        return self.get("/Security/boards") or []

    def get_securities(self, board: int, page: int = 1, size: int = 100) -> List[Dict]:
        data = self.get(f"/Security/{board}?pageNumber={page}&pageSize={size}&favorite=0")
        return data if isinstance(data, list) else (data or {}).get("data", []) or []

    def find_security(self, board: int, ticker: str) -> Optional[Dict]:
        data = self.get(f"/Security/find/{board}/{ticker}")
        if isinstance(data, list):
            for item in data:
                if item.get('shortName', '').upper() == ticker.upper():
                    return item
        return None

    def get_quotes(self, security_id: str) -> Optional[Dict]:
        """Стакан L2 — /Quotes/{secId}/0"""
        return self.get(f"/Quotes/{security_id}/0")

    def get_closed_positions(self) -> List[Dict]:
        data = self.get(f"/api/Positions/close/{USERNAME}")
        return data if isinstance(data, list) else []

    def get_transaction_history(self, ttype: int = 5) -> List[Dict]:
        """ttype=5 = Commission"""
        data = self.get(f"/api/account/TransactionHistory/{ttype}")
        return data if isinstance(data, list) else []


# ════════════════════════════════════════════════════════════════════════════════
# CANDLE BUILDER — строит свечи из тиков в реальном времени
# ════════════════════════════════════════════════════════════════════════════════

class CandleBuilder:
    def __init__(self, ticker: str, security_id: str):
        self.ticker = ticker
        self.security_id = security_id
        # Текущие незакрытые свечи по таймфреймам
        self._current: Dict[str, Optional[Candle]] = {tf: None for tf in TIMEFRAMES}
        # Накопленные закрытые свечи
        self.closed: Dict[str, List[Candle]] = {tf: [] for tf in TIMEFRAMES}
        self._lock = threading.Lock()

    def _bucket_ts(self, tick_ts_ms: int, tf_sec: int) -> int:
        """Начало свечи для данного тика"""
        ts_sec = tick_ts_ms // 1000
        return (ts_sec // tf_sec) * tf_sec * 1000

    def add_tick(self, tick: Tick) -> List[Candle]:
        """Добавить тик, вернуть список закрытых свечей (если появились)"""
        newly_closed = []
        with self._lock:
            for tf_name, tf_sec in TIMEFRAMES.items():
                bucket = self._bucket_ts(tick.ts, tf_sec)
                cur = self._current[tf_name]

                if cur is None:
                    # Первая свеча
                    cur = self._new_candle(tf_name, bucket, tick)
                    self._current[tf_name] = cur
                elif bucket > cur.ts_open:
                    # Новый бар — закрываем текущий
                    cur.ts_close = cur.ts_open + tf_sec * 1000 - 1
                    self.closed[tf_name].append(cur)
                    newly_closed.append(cur)
                    cur = self._new_candle(tf_name, bucket, tick)
                    self._current[tf_name] = cur
                else:
                    # Обновляем текущую свечу
                    self._update_candle(cur, tick)

        return newly_closed

    def _new_candle(self, tf: str, ts: int, tick: Tick) -> Candle:
        buy_v  = tick.volume if tick.side == 0 else 0.0
        sell_v = tick.volume if tick.side == 1 else 0.0
        return Candle(
            tf=tf, ts_open=ts, ts_close=0,
            ticker=self.ticker, security_id=self.security_id,
            open=tick.price, high=tick.price, low=tick.price, close=tick.price,
            volume=tick.volume, buy_vol=buy_v, sell_vol=sell_v,
            cum_delta=buy_v - sell_v, tick_count=1,
            buy_ticks=1 if tick.side == 0 else 0,
            sell_ticks=1 if tick.side == 1 else 0,
            vwap=tick.price,
        )

    def _update_candle(self, c: Candle, tick: Tick):
        c.high  = max(c.high, tick.price)
        c.low   = min(c.low, tick.price)
        c.close = tick.price
        c.volume += tick.volume
        if tick.side == 0:
            c.buy_vol  += tick.volume
            c.buy_ticks += 1
        else:
            c.sell_vol  += tick.volume
            c.sell_ticks += 1
        c.cum_delta = c.buy_vol - c.sell_vol
        c.tick_count += 1
        # VWAP = price * volume / total_volume
        # Упрощённо: running average
        c.vwap = (c.vwap * (c.tick_count - 1) + tick.price) / c.tick_count

    def get_last_candles(self, tf: str, n: int = 200) -> List[Candle]:
        with self._lock:
            all_c = self.closed[tf][-n:]
            cur = self._current.get(tf)
            if cur:
                all_c = all_c + [cur]
            return all_c


# ════════════════════════════════════════════════════════════════════════════════
# FEATURE ENGINEER — вычисляет признаки из свечей
# ════════════════════════════════════════════════════════════════════════════════

class FeatureEngineer:
    """
    Вычисляет все признаки из набора свечей.
    Результат — словарь feature_name → float для последней свечи.
    """

    @staticmethod
    def compute(candles: List[Candle], ob: Optional[OrderBookSnapshot] = None) -> Dict[str, float]:
        if len(candles) < 2:
            return {}

        feat: Dict[str, float] = {}
        closes = np.array([c.close for c in candles], dtype=float)
        highs  = np.array([c.high  for c in candles], dtype=float)
        lows   = np.array([c.low   for c in candles], dtype=float)
        vols   = np.array([c.volume for c in candles], dtype=float)
        buy_v  = np.array([c.buy_vol  for c in candles], dtype=float)
        sell_v = np.array([c.sell_vol for c in candles], dtype=float)
        deltas = np.array([c.cum_delta for c in candles], dtype=float)
        vwaps  = np.array([c.vwap for c in candles], dtype=float)

        last = candles[-1]
        last_idx = len(closes) - 1

        # ── 1. БАЗОВЫЕ ЦЕНОВЫЕ ПРИЗНАКИ ────────────────────────────────────
        feat['close']       = last.close
        feat['open']        = last.open
        feat['high']        = last.high
        feat['low']         = last.low
        feat['body_size']   = abs(last.close - last.open)
        feat['upper_wick']  = last.high - max(last.close, last.open)
        feat['lower_wick']  = min(last.close, last.open) - last.low
        feat['candle_range']= last.high - last.low
        feat['is_bullish']  = 1.0 if last.close >= last.open else 0.0
        feat['gap']         = last.open - candles[-2].close if len(candles) >= 2 else 0.0

        # ── 2. ОБЪЁМ И ДЕЛЬТА ───────────────────────────────────────────────
        feat['volume']      = last.volume
        feat['buy_vol']     = last.buy_vol
        feat['sell_vol']    = last.sell_vol
        feat['cum_delta']   = last.cum_delta
        feat['delta_pct']   = last.cum_delta / max(last.volume, 1e-9)
        feat['buy_ratio']   = last.buy_vol / max(last.volume, 1e-9)
        feat['tick_count']  = float(last.tick_count)
        feat['vwap']        = last.vwap
        feat['close_vs_vwap'] = last.close - last.vwap

        # ── 3. EMA ──────────────────────────────────────────────────────────
        for p in [5, 10, 20, 50, 100, 200]:
            if len(closes) >= p:
                ema = FeatureEngineer._ema(closes, p)
                feat[f'ema_{p}'] = ema[-1]
                feat[f'close_vs_ema_{p}'] = closes[-1] - ema[-1]
                feat[f'ema_{p}_slope'] = (ema[-1] - ema[-2]) if len(ema) >= 2 else 0.0

        # ── 4. RSI ──────────────────────────────────────────────────────────
        for p in [7, 14, 21]:
            if len(closes) >= p + 1:
                feat[f'rsi_{p}'] = FeatureEngineer._rsi(closes, p)

        # ── 5. ATR (Average True Range) ─────────────────────────────────────
        for p in [7, 14]:
            if len(closes) >= p + 1:
                feat[f'atr_{p}'] = FeatureEngineer._atr(highs, lows, closes, p)
                feat[f'atr_{p}_pct'] = feat[f'atr_{p}'] / max(closes[-1], 1e-9)

        # ── 6. СКОЛЬЗЯЩИЕ ОБЪЁМЫ ────────────────────────────────────────────
        for p in [5, 10, 20]:
            if len(vols) >= p:
                feat[f'vol_ma_{p}']    = float(np.mean(vols[-p:]))
                feat[f'vol_ratio_{p}'] = float(vols[-1] / max(np.mean(vols[-p:]), 1e-9))
                feat[f'delta_ma_{p}']  = float(np.mean(deltas[-p:]))
                feat[f'buy_ratio_ma_{p}'] = float(np.mean(buy_v[-p:] / (vols[-p:] + 1e-9)))

        # ── 7. КУМУЛЯТИВНАЯ ДЕЛЬТА ──────────────────────────────────────────
        for p in [5, 10, 20]:
            if len(deltas) >= p:
                feat[f'cum_delta_{p}'] = float(np.sum(deltas[-p:]))
        feat['delta_trend'] = float(np.sign(np.sum(deltas[-5:]))) if len(deltas) >= 5 else 0.0

        # ── 8. MOMENTUM / ROC ───────────────────────────────────────────────
        for p in [1, 3, 5, 10, 20]:
            if len(closes) >= p + 1:
                feat[f'roc_{p}'] = float((closes[-1] / closes[-(p+1)] - 1) * 100)

        # ── 9. BOLLINGER BANDS (20, 2σ) ──────────────────────────────────────
        if len(closes) >= 20:
            bb_mid = float(np.mean(closes[-20:]))
            bb_std = float(np.std(closes[-20:]))
            feat['bb_mid']   = bb_mid
            feat['bb_upper'] = bb_mid + 2 * bb_std
            feat['bb_lower'] = bb_mid - 2 * bb_std
            feat['bb_width'] = 4 * bb_std / max(bb_mid, 1e-9)
            feat['bb_pos']   = (closes[-1] - feat['bb_lower']) / max(feat['bb_upper'] - feat['bb_lower'], 1e-9)

        # ── 10. VOLATILITY (std / ATR normalised) ───────────────────────────
        for p in [10, 20]:
            if len(closes) >= p:
                feat[f'volatility_{p}'] = float(np.std(closes[-p:]) / max(closes[-1], 1e-9))

        # ── 11. MAX/MIN LOOKBACK ─────────────────────────────────────────────
        for p in [5, 10, 20, 50]:
            if len(closes) >= p:
                feat[f'high_{p}'] = float(np.max(highs[-p:]))
                feat[f'low_{p}']  = float(np.min(lows[-p:]))
                feat[f'dist_to_high_{p}'] = float(closes[-1] / max(np.max(highs[-p:]), 1e-9) - 1)
                feat[f'dist_to_low_{p}']  = float(closes[-1] / max(np.min(lows[-p:]), 1e-9) - 1)

        # ── 12. VWAP DEVIATION ───────────────────────────────────────────────
        if len(vwaps) >= 10:
            feat['vwap_dev_10'] = float((closes[-1] - np.mean(vwaps[-10:])) / max(closes[-1], 1e-9))

        # ── 13. ORDER BOOK FEATURES ──────────────────────────────────────────
        if ob is not None:
            feat['ob_bid']        = ob.bid
            feat['ob_ask']        = ob.ask
            feat['ob_spread']     = ob.spread
            feat['ob_spread_pct'] = ob.spread / max(ob.bid, 1e-9)
            feat['ob_imbalance']  = ob.imbalance
            feat['ob_bid_vol5']   = ob.bid_vol_top5
            feat['ob_ask_vol5']   = ob.ask_vol_top5
            feat['ob_depth']      = float(ob.depth)
            feat['close_vs_mid']  = closes[-1] - (ob.bid + ob.ask) / 2 if ob.bid > 0 and ob.ask > 0 else 0.0

        # ── 14. TIME FEATURES ────────────────────────────────────────────────
        dt = datetime.fromtimestamp(last.ts_open / 1000)
        feat['hour']        = float(dt.hour)
        feat['minute']      = float(dt.minute)
        feat['dow']         = float(dt.weekday())   # 0=Mon, 4=Fri
        feat['hour_sin']    = math.sin(2 * math.pi * dt.hour / 24)
        feat['hour_cos']    = math.cos(2 * math.pi * dt.hour / 24)
        feat['is_open']     = 1.0 if 10 <= dt.hour < 18 else 0.0  # MOEX основная сессия
        feat['is_close']    = 1.0 if 17 <= dt.hour <= 18 else 0.0  # Вечерняя торговля

        # ── 15. CANDLE PATTERN FEATURES ──────────────────────────────────────
        if len(candles) >= 3:
            c1, c2, c3 = candles[-3], candles[-2], candles[-1]
            feat['consec_bull'] = float(sum(1 for c in [c1,c2,c3] if c.close >= c.open))
            feat['consec_bear'] = float(sum(1 for c in [c1,c2,c3] if c.close < c.open))

        return feat

    # ── Технические индикаторы ─────────────────────────────────────────────

    @staticmethod
    def _ema(prices: np.ndarray, period: int) -> np.ndarray:
        k = 2 / (period + 1)
        ema = np.empty_like(prices)
        ema[0] = prices[0]
        for i in range(1, len(prices)):
            ema[i] = prices[i] * k + ema[i-1] * (1 - k)
        return ema

    @staticmethod
    def _rsi(prices: np.ndarray, period: int) -> float:
        delta = np.diff(prices[-(period+1):])
        gain  = np.where(delta > 0, delta, 0.0)
        loss  = np.where(delta < 0, -delta, 0.0)
        avg_g = np.mean(gain)
        avg_l = np.mean(loss)
        if avg_l == 0:
            return 100.0
        rs = avg_g / avg_l
        return float(100 - 100 / (1 + rs))

    @staticmethod
    def _atr(highs: np.ndarray, lows: np.ndarray, closes: np.ndarray, period: int) -> float:
        n = min(len(highs), period + 1)
        trs = []
        for i in range(1, n):
            tr = max(
                highs[-n+i] - lows[-n+i],
                abs(highs[-n+i] - closes[-n+i-1]),
                abs(lows[-n+i] - closes[-n+i-1])
            )
            trs.append(tr)
        return float(np.mean(trs)) if trs else 0.0


# ════════════════════════════════════════════════════════════════════════════════
# ORDER BOOK PROCESSOR — парсит /Quotes и строит снапшот
# ════════════════════════════════════════════════════════════════════════════════

class OrderBookProcessor:
    def __init__(self):
        self.snapshots: Dict[str, deque] = defaultdict(lambda: deque(maxlen=500))

    def parse(self, security_id: str, ticker: str, raw: Dict) -> Optional[OrderBookSnapshot]:
        """
        Парсит ответ /Quotes/{secId}/0
        Формат: { a: [{p, l:{v}}], b: [{p, l:{v}}], pl: last }
        """
        if not raw or not isinstance(raw, dict):
            return None

        asks = raw.get('a', [])
        bids = raw.get('b', [])

        def parse_levels(levels, normalize=True):
            result = []
            for lvl in levels:
                p = float(lvl.get('p', 0))
                l = lvl.get('l', {})
                v = float(l.get('v', 0)) if isinstance(l, dict) else 0.0
                if p > 0:
                    # Нормализация цены (делим на 100 если нужно)
                    pn = p / 100 if p > 10000 else p
                    result.append((pn, v))
            return result

        parsed_asks = parse_levels(asks)
        parsed_bids = parse_levels(bids)

        if not parsed_asks and not parsed_bids:
            return None

        best_ask = parsed_asks[0][0] if parsed_asks else 0.0
        best_bid = parsed_bids[0][0] if parsed_bids else 0.0
        spread   = best_ask - best_bid if best_ask > 0 and best_bid > 0 else 0.0

        bid_v5 = sum(v for _, v in parsed_bids[:5])
        ask_v5 = sum(v for _, v in parsed_asks[:5])
        total  = bid_v5 + ask_v5
        imbalance = (bid_v5 - ask_v5) / max(total, 1e-9)

        snap = OrderBookSnapshot(
            ts=int(time.time() * 1000),
            ticker=ticker,
            security_id=security_id,
            bid=best_bid,
            ask=best_ask,
            spread=spread,
            depth=max(len(parsed_asks), len(parsed_bids)),
            bid_vol_top5=bid_v5,
            ask_vol_top5=ask_v5,
            imbalance=imbalance,
            bids=parsed_bids,
            asks=parsed_asks,
        )
        self.snapshots[security_id].append(snap)
        return snap

    def get_latest(self, security_id: str) -> Optional[OrderBookSnapshot]:
        q = self.snapshots.get(security_id)
        return q[-1] if q else None


# ════════════════════════════════════════════════════════════════════════════════
# CLUSTER PROCESSOR — парсит кластеры из SignalR
# ════════════════════════════════════════════════════════════════════════════════

class ClusterProcessor:
    def __init__(self):
        self.bars: Dict[str, List[ClusterBar]] = defaultdict(list)

    def parse(self, raw_cluster: Dict) -> Optional[ClusterBar]:
        """
        Парсит кластерный бар из /hubs/cluster
        Формат: {i, f, t, s, e, l:[{p, a, b}], op, cl, hi, lo}
        """
        if not raw_cluster:
            return None

        sid      = str(raw_cluster.get('s', ''))
        ts       = raw_cluster.get('t', '')
        tf       = raw_cluster.get('e', '00:01:00')
        op       = float(raw_cluster.get('op', 0))
        cl_price = float(raw_cluster.get('cl', raw_cluster.get('\u0441l', 0)))  # Unicode bug fix
        hi       = float(raw_cluster.get('hi', 0))
        lo       = float(raw_cluster.get('lo', 0))
        levels_raw = raw_cluster.get('l', [])

        # Нормализация цен
        def norm_p(p: float) -> float:
            return p / 100 if p > 10000 else p

        op = norm_p(op); cl_price = norm_p(cl_price)
        hi = norm_p(hi); lo = norm_p(lo)

        levels = []
        total_vol = 0.0
        buy_vol = 0.0
        sell_vol = 0.0
        max_vol = 0.0
        poc_price = op
        max_imb = 0.0

        for lvl in levels_raw:
            p   = norm_p(float(lvl.get('p', 0)))
            a_v = float(lvl.get('a', 0))   # ask (sell) volume
            b_v = float(lvl.get('b', 0))   # bid (buy) volume
            tot = a_v + b_v
            delta = b_v - a_v
            imb = delta / max(tot, 1e-9)

            levels.append(ClusterLevel(
                price=p, ask_vol=a_v, bid_vol=b_v,
                delta=delta, imbalance=imb
            ))
            total_vol += tot
            buy_vol  += b_v
            sell_vol += a_v

            if tot > max_vol:
                max_vol = tot
                poc_price = p

            if abs(imb) > abs(max_imb):
                max_imb = imb

        bar = ClusterBar(
            ts=ts, ticker='', security_id=sid, timeframe=tf,
            open=op, close=cl_price, high=hi, low=lo,
            levels=levels, poc_price=poc_price, poc_vol=max_vol,
            total_vol=total_vol, buy_vol=buy_vol, sell_vol=sell_vol,
            cum_delta=buy_vol - sell_vol, max_imbalance=max_imb,
            is_bullish=(cl_price >= op)
        )
        self.bars[sid].append(bar)
        return bar

    def get_cluster_features(self, security_id: str) -> Dict[str, float]:
        """Признаки из последнего кластерного бара"""
        bars = self.bars.get(security_id, [])
        if not bars:
            return {}
        b = bars[-1]
        feat = {
            'cluster_poc':      b.poc_price,
            'cluster_poc_vol':  b.poc_vol,
            'cluster_total_vol': b.total_vol,
            'cluster_buy_vol':  b.buy_vol,
            'cluster_sell_vol': b.sell_vol,
            'cluster_delta':    b.cum_delta,
            'cluster_delta_pct': b.cum_delta / max(b.total_vol, 1e-9),
            'cluster_max_imb':  b.max_imbalance,
            'cluster_is_bull':  1.0 if b.is_bullish else 0.0,
            'cluster_range':    b.high - b.low,
            'poc_vs_close':     b.poc_price - b.close,
            'poc_vs_mid':       b.poc_price - (b.high + b.low) / 2,
        }
        # Добавляем уровни с максимальным дисбалансом
        if b.levels:
            sorted_by_imb = sorted(b.levels, key=lambda x: abs(x.imbalance), reverse=True)
            for i, lvl in enumerate(sorted_by_imb[:3]):
                feat[f'top_imb_{i}_price']     = lvl.price
                feat[f'top_imb_{i}_delta']     = lvl.delta
                feat[f'top_imb_{i}_imbalance'] = lvl.imbalance
        return feat


# ════════════════════════════════════════════════════════════════════════════════
# LABEL GENERATOR — генерирует метки из закрытых позиций
# ════════════════════════════════════════════════════════════════════════════════

class LabelGenerator:
    def __init__(self, client: CassandraAPIClient):
        self.client = client
        self.positions: List[ClosedPosition] = []

    def load(self):
        raw = self.client.get_closed_positions()
        positions = []
        for pos in raw:
            try:
                avg_entry = float(pos.get('averageEntry', 0))
                avg_exit  = float(pos.get('averageExit', 0))
                if avg_entry <= 0 or avg_exit <= 0:
                    continue
                # Нормализация
                if avg_entry > 10000:
                    avg_entry /= 100; avg_exit /= 100
                pnl_pts = avg_exit - avg_entry
                pnl_pct = pnl_pts / avg_entry * 100

                # Время удержания
                open_t  = pos.get('openTime', pos.get('opentime', ''))
                close_t = pos.get('closeTime', pos.get('closetime', ''))
                hold_mins = 0.0
                if open_t and close_t:
                    try:
                        t1 = datetime.fromisoformat(open_t.replace('Z', ''))
                        t2 = datetime.fromisoformat(close_t.replace('Z', ''))
                        hold_mins = (t2 - t1).total_seconds() / 60
                    except Exception:
                        pass

                outcome = 1 if pnl_pts > 0 else (0 if pnl_pts < 0 else -1)

                positions.append(ClosedPosition(
                    position_id  = str(pos.get('id', '')),
                    security_id  = str(pos.get('secId', pos.get('securityId', ''))),
                    security_name= str(pos.get('secName', pos.get('securityName', ''))),
                    open_time    = open_t,
                    close_time   = close_t,
                    side         = int(pos.get('side', 0)),
                    avg_entry    = avg_entry,
                    avg_exit     = avg_exit,
                    pnl_points   = pnl_pts,
                    pnl_pct      = pnl_pct,
                    outcome      = outcome,
                    holding_mins = hold_mins,
                ))
            except Exception as e:
                log.debug(f"Skipping position: {e}")

        self.positions = positions
        log.info(f"Loaded {len(positions)} closed positions. "
                 f"W={sum(1 for p in positions if p.outcome==1)} "
                 f"L={sum(1 for p in positions if p.outcome==0)}")
        return positions

    def label_candle(self, candle: Candle, future_candles: List[Candle],
                     tp_pct: float = 0.5, sl_pct: float = 0.5) -> Dict[str, float]:
        """Размечает одну свечу глядя вперёд N баров"""
        labels = {}
        if not future_candles:
            return labels

        ep = candle.close
        tp = ep * (1 + tp_pct / 100)
        sl = ep * (1 - sl_pct / 100)

        # N-bar forward returns
        for n in [1, 3, 5, 10]:
            if len(future_candles) >= n:
                labels[f'ret_{n}bar'] = float((future_candles[n-1].close / ep - 1) * 100)
                labels[f'dir_{n}bar'] = 1.0 if future_candles[n-1].close > ep else 0.0

        # TP/SL hit
        tp_hit = sl_hit = 0
        for fc in future_candles[:20]:
            if fc.high >= tp:
                tp_hit = 1
                break
            if fc.low <= sl:
                sl_hit = 1
                break
        labels['tp_hit']      = float(tp_hit)
        labels['sl_hit']      = float(sl_hit)
        labels['outcome_tpsl']= 1.0 if tp_hit and not sl_hit else (0.0 if sl_hit else -1.0)

        return labels


# ════════════════════════════════════════════════════════════════════════════════
# DATA WRITER — сохраняет данные в файлы
# ════════════════════════════════════════════════════════════════════════════════

class DataWriter:
    def __init__(self, base_dir: Path, ticker: str):
        self.base_dir = base_dir / ticker
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.ticker = ticker

        # CSV writers
        self._csv_files: Dict[str, Any] = {}
        self._csv_writers: Dict[str, Any] = {}

        # Buffer для batch записи
        self._buffers: Dict[str, List[Dict]] = defaultdict(list)
        self._lock = threading.Lock()

    def _get_writer(self, name: str, fieldnames: List[str]):
        if name not in self._csv_writers:
            path = self.base_dir / f"{name}.csv"
            is_new = not path.exists()
            f = open(path, 'a', newline='', encoding='utf-8')
            w = csv.DictWriter(f, fieldnames=fieldnames, extrasaction='ignore')
            if is_new:
                w.writeheader()
            self._csv_files[name] = f
            self._csv_writers[name] = w
        return self._csv_writers[name]

    def write_tick(self, tick: Tick):
        with self._lock:
            row = asdict(tick)
            self._buffers['ticks'].append(row)
            if len(self._buffers['ticks']) >= 500:
                self._flush_ticks()

    def _flush_ticks(self):
        if not self._buffers['ticks']:
            return
        fields = ['ts', 'price', 'volume', 'side', 'ticker', 'security_id']
        w = self._get_writer('ticks', fields)
        w.writerows(self._buffers['ticks'])
        self._csv_files['ticks'].flush()
        self._buffers['ticks'].clear()

    def write_candle_with_features(self, candle: Candle, features: Dict[str, float],
                                    labels: Dict[str, float] = None):
        row = asdict(candle)
        row.update(features)
        if labels:
            row.update(labels)
        row['ts_dt'] = datetime.fromtimestamp(candle.ts_open / 1000).isoformat()
        with self._lock:
            self._buffers[f'candles_{candle.tf}'].append(row)
            if len(self._buffers[f'candles_{candle.tf}']) >= 50:
                self._flush_candles(candle.tf)

    def _flush_candles(self, tf: str):
        buf = self._buffers.get(f'candles_{tf}', [])
        if not buf:
            return
        fields = list(buf[0].keys())
        w = self._get_writer(f'candles_{tf}', fields)
        w.writerows(buf)
        self._csv_files[f'candles_{tf}'].flush()
        buf.clear()

    def write_orderbook(self, snap: OrderBookSnapshot):
        row = {
            'ts': snap.ts,
            'ts_dt': datetime.fromtimestamp(snap.ts / 1000).isoformat(),
            'ticker': snap.ticker,
            'bid': snap.bid,
            'ask': snap.ask,
            'spread': snap.spread,
            'spread_pct': snap.spread / max(snap.bid, 1e-9),
            'depth': snap.depth,
            'bid_vol5': snap.bid_vol_top5,
            'ask_vol5': snap.ask_vol_top5,
            'imbalance': snap.imbalance,
        }
        # Добавляем топ-10 уровней стакана
        for i, (p, v) in enumerate(snap.bids[:10]):
            row[f'bid_p{i}'] = p; row[f'bid_v{i}'] = v
        for i, (p, v) in enumerate(snap.asks[:10]):
            row[f'ask_p{i}'] = p; row[f'ask_v{i}'] = v
        with self._lock:
            self._buffers['orderbook'].append(row)
            if len(self._buffers['orderbook']) >= 100:
                self._flush_generic('orderbook')

    def write_cluster(self, bar: ClusterBar):
        row = {
            'ts': bar.ts,
            'ticker': bar.ticker,
            'timeframe': bar.timeframe,
            'open': bar.open, 'close': bar.close,
            'high': bar.high, 'low': bar.low,
            'poc_price': bar.poc_price,
            'poc_vol': bar.poc_vol,
            'total_vol': bar.total_vol,
            'buy_vol': bar.buy_vol,
            'sell_vol': bar.sell_vol,
            'cum_delta': bar.cum_delta,
            'max_imbalance': bar.max_imbalance,
            'is_bullish': int(bar.is_bullish),
        }
        # Топ-5 уровней по объёму
        sorted_lvls = sorted(bar.levels, key=lambda x: x.bid_vol + x.ask_vol, reverse=True)
        for i, lvl in enumerate(sorted_lvls[:5]):
            row[f'lvl{i}_price']   = lvl.price
            row[f'lvl{i}_bid']     = lvl.bid_vol
            row[f'lvl{i}_ask']     = lvl.ask_vol
            row[f'lvl{i}_delta']   = lvl.delta
            row[f'lvl{i}_imb']     = lvl.imbalance
        with self._lock:
            self._buffers['clusters'].append(row)
            if len(self._buffers['clusters']) >= 50:
                self._flush_generic('clusters')

    def write_position(self, pos: ClosedPosition):
        with self._lock:
            self._buffers['positions'].append(asdict(pos))
            self._flush_generic('positions')

    def _flush_generic(self, name: str):
        buf = self._buffers.get(name, [])
        if not buf:
            return
        fields = list(buf[0].keys())
        w = self._get_writer(name, fields)
        w.writerows(buf)
        self._csv_files[name].flush()
        buf.clear()

    def flush_all(self):
        with self._lock:
            for tf in TIMEFRAMES:
                self._flush_candles(tf)
            self._flush_ticks()
            for name in ['orderbook', 'clusters', 'positions']:
                self._flush_generic(name)

    def to_parquet(self):
        """Конвертирует все CSV → Parquet (если pandas/pyarrow доступны)"""
        if not PANDAS_OK:
            return
        for csv_path in self.base_dir.glob("*.csv"):
            try:
                df = pd.read_csv(csv_path)
                pq_path = csv_path.with_suffix('.parquet')
                df.to_parquet(pq_path, index=False, compression='snappy')
                log.info(f"Parquet: {pq_path} ({len(df)} rows)")
            except Exception as e:
                log.warning(f"Parquet conversion failed {csv_path}: {e}")

    def to_npz(self, tf: str = '1m'):
        """Конвертирует свечи → numpy arrays для нейросетей"""
        csv_path = self.base_dir / f"candles_{tf}.csv"
        if not csv_path.exists():
            return
        try:
            if PANDAS_OK:
                df = pd.read_csv(csv_path)
            else:
                with open(csv_path) as f:
                    reader = csv.DictReader(f)
                    rows = list(reader)
                df = rows  # simple fallback

            if PANDAS_OK and len(df) > 10:
                # Числовые колонки
                num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
                X = df[num_cols].fillna(0).values

                # Метки
                label_cols = [c for c in num_cols if c.startswith(('ret_', 'dir_', 'tp_', 'sl_', 'outcome_'))]
                feature_cols = [c for c in num_cols if c not in label_cols]

                X_feat = df[feature_cols].fillna(0).values
                X_lab  = df[label_cols].fillna(0).values if label_cols else np.zeros((len(df), 1))

                npz_path = self.base_dir / f"candles_{tf}.npz"
                np.savez_compressed(
                    npz_path,
                    features=X_feat.astype(np.float32),
                    labels=X_lab.astype(np.float32),
                    feature_names=np.array(feature_cols),
                    label_names=np.array(label_cols) if label_cols else np.array(['none']),
                )
                log.info(f"NPZ: {npz_path} shape=({len(X_feat)}, {len(feature_cols)}) features, {len(label_cols)} labels")
        except Exception as e:
            log.warning(f"NPZ conversion failed: {e}")

    def to_jsonl(self, tf: str = '1m'):
        """
        Экспорт в JSONL для LLM fine-tuning.
        Формат: {"instruction": "...", "input": {features}, "output": "BUY/SELL/HOLD"}
        """
        csv_path = self.base_dir / f"candles_{tf}.csv"
        if not csv_path.exists() or not PANDAS_OK:
            return
        try:
            df = pd.read_csv(csv_path)
            if len(df) < 5:
                return
            jsonl_path = self.base_dir / f"candles_{tf}_llm.jsonl"
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for _, row in df.iterrows():
                    # Формируем контекст для LLM
                    direction = None
                    if 'dir_1bar' in row:
                        direction = "BUY" if row['dir_1bar'] == 1.0 else "SELL"
                    elif 'outcome_tpsl' in row:
                        direction = "WIN" if row['outcome_tpsl'] == 1.0 else "LOSS"
                    if direction is None:
                        continue

                    feat_subset = {k: round(float(v), 6) for k, v in row.items()
                                   if not k.startswith(('ret_', 'dir_', 'tp_', 'sl_', 'outcome_', 'ts', 'label'))
                                   and pd.notna(v)}

                    record = {
                        "instruction": (
                            f"Ты — торговый ИИ для MOEX. Тикер: {self.ticker}. "
                            f"Таймфрейм: {tf}. Проанализируй рыночные данные и предскажи направление."
                        ),
                        "input": feat_subset,
                        "output": direction,
                    }
                    f.write(json.dumps(record, ensure_ascii=False) + '\n')
            log.info(f"JSONL: {jsonl_path} ({sum(1 for _ in open(jsonl_path))} records)")
        except Exception as e:
            log.warning(f"JSONL conversion failed: {e}")

    def close(self):
        self.flush_all()
        for f in self._csv_files.values():
            try:
                f.close()
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
# SIGNALR COLLECTOR — подписывается на все хабы
# ════════════════════════════════════════════════════════════════════════════════

class SignalRCollector:
    def __init__(self, token: str, security_id: str, ticker: str,
                 on_tick, on_cluster, on_finrez):
        self.token = token
        self.security_id = security_id
        self.ticker = ticker
        self.on_tick = on_tick
        self.on_cluster = on_cluster
        self.on_finrez = on_finrez
        self.connections = []
        self._running = False

    def _make_conn(self, path: str):
        return (
            HubConnectionBuilder()
            .with_url(f"{BASE_URL}/hubs/{path}",
                      options={"access_token_factory": lambda: self.token,
                               "verify_ssl": True})
            .with_automatic_reconnect({
                "type": "interval",
                "keep_alive_interval": 10,
                "intervals": [1, 3, 5, 10, 15, 30]
            })
            .build()
        )

    def start(self):
        if not SIGNALR_OK:
            log.warning("SignalR не доступен. Только REST polling.")
            return

        self._running = True

        # /hubs/tick?securityId=...
        try:
            tick_conn = self._make_conn(f"tick?securityId={self.security_id}")
            tick_conn.on("Tick", self._handle_ticks)
            tick_conn.on("ChangePrice", self._handle_change_price)
            tick_conn.start()
            self.connections.append(tick_conn)
            log.info(f"Tick hub connected: {self.ticker}")
        except Exception as e:
            log.warning(f"Tick hub failed: {e}")

        # /hubs/cluster?securityId=... (если есть)
        try:
            cluster_conn = self._make_conn(f"cluster?securityId={self.security_id}")
            cluster_conn.on("Cluster", self._handle_cluster)
            cluster_conn.start()
            self.connections.append(cluster_conn)
            log.info(f"Cluster hub connected: {self.ticker}")
        except Exception as e:
            log.info(f"Cluster hub not available (normal): {e}")

        # /hubs/finrez
        try:
            finrez_conn = self._make_conn("finrez")
            finrez_conn.on("FinRez", self._handle_finrez)
            finrez_conn.start()
            self.connections.append(finrez_conn)
            log.info("FinRez hub connected")
        except Exception as e:
            log.warning(f"FinRez hub failed: {e}")

    def _handle_ticks(self, ticks_raw):
        """
        ticks_raw = [[{t, p, v, s}, ...]] или [{t, p, v, s}]
        """
        arr = ticks_raw if isinstance(ticks_raw, list) else [ticks_raw]
        if arr and isinstance(arr[0], list):
            arr = arr[0]
        for t in arr:
            if not isinstance(t, dict):
                continue
            ts = int(t.get('t', time.time() * 1000))
            p  = float(t.get('p', 0))
            # Нормализация
            if p > 10000:
                p /= 100
            v  = float(t.get('v', 1))
            s  = int(t.get('s', 0))
            tick = Tick(ts=ts, price=p, volume=v, side=s,
                        ticker=self.ticker, security_id=self.security_id)
            self.on_tick(tick)

    def _handle_change_price(self, data):
        if isinstance(data, dict) and 'p' in data:
            log.debug(f"ChangePrice {self.ticker}: {data['p']}%")

    def _handle_cluster(self, data):
        arr = data if isinstance(data, list) else [data]
        if arr and isinstance(arr[0], list):
            arr = arr[0]
        for c in arr:
            if isinstance(c, dict):
                self.on_cluster(c)

    def _handle_finrez(self, data):
        self.on_finrez(data)

    def stop(self):
        self._running = False
        for conn in self.connections:
            try:
                conn.stop()
            except Exception:
                pass


# ════════════════════════════════════════════════════════════════════════════════
# MAIN COLLECTOR
# ════════════════════════════════════════════════════════════════════════════════

class AIDataCollector:
    def __init__(self, tickers: List[str], output_dir: Path):
        self.tickers = [t.upper() for t in tickers]
        self.output_dir = output_dir
        output_dir.mkdir(parents=True, exist_ok=True)

        self.client  = CassandraAPIClient()
        self.ob_proc = OrderBookProcessor()
        self.cl_proc = ClusterProcessor()
        self.label_gen = LabelGenerator(self.client)

        # Per-ticker state
        self.builders: Dict[str, CandleBuilder] = {}
        self.writers:  Dict[str, DataWriter]    = {}
        self.security_map: Dict[str, Dict] = {}    # ticker → {id, name, ...}
        self.signalr:  Dict[str, SignalRCollector] = {}
        self.latest_ob: Dict[str, Optional[OrderBookSnapshot]] = {}

        self._running = False
        self._lock = threading.Lock()

        # Статистика
        self.stats = defaultdict(int)

    def setup(self):
        if not self.client.authenticate():
            raise RuntimeError("Authentication failed")

        # Загружаем инструменты
        log.info("Loading securities...")
        for board in [1, 0]:  # FORTS, STOCK
            secs = self.client.get_securities(board, size=200)
            for s in secs:
                tk = (s.get('shortName') or s.get('isin') or '').strip().upper()
                sid = str(s.get('securityId') or s.get('id') or '')
                if tk and sid:
                    self.security_map[tk] = {
                        'id': sid,
                        'name': s.get('fullName', tk),
                        'board': board,
                        'lot': s.get('lot', 1),
                        'pstep': s.get('priceStep', 0.01),
                        'decimals': s.get('decimals', 2),
                    }

        # Инициализируем per-ticker компоненты
        for ticker in self.tickers:
            info = self.security_map.get(ticker)
            if not info:
                # Попробуем найти
                for board in [1, 0]:
                    found = self.client.find_security(board, ticker)
                    if found:
                        info = {
                            'id': str(found.get('securityId', '')),
                            'name': found.get('fullName', ticker),
                            'board': board,
                            'lot': found.get('lot', 1),
                            'pstep': found.get('priceStep', 0.01),
                            'decimals': found.get('decimals', 2),
                        }
                        self.security_map[ticker] = info
                        break

            if not info or not info.get('id'):
                log.warning(f"Security not found: {ticker}")
                continue

            sid = info['id']
            self.builders[ticker] = CandleBuilder(ticker, sid)
            self.writers[ticker]  = DataWriter(self.output_dir, ticker)
            self.latest_ob[ticker] = None
            log.info(f"  {ticker} → id={sid} board={info['board']}")

        # Загружаем закрытые позиции
        log.info("Loading closed positions (labels)...")
        positions = self.label_gen.load()
        # Записываем позиции
        for pos in positions:
            # Определяем тикер по security_id
            for tk, info in self.security_map.items():
                if info['id'] == pos.security_id and tk in self.writers:
                    self.writers[tk].write_position(pos)

        log.info(f"Setup complete. Tracking: {list(self.builders.keys())}")

    def start(self):
        self._running = True

        # SignalR для каждого тикера
        for ticker, builder in self.builders.items():
            info = self.security_map[ticker]
            sid  = info['id']

            def make_callbacks(tk, bld):
                def on_tick(tick: Tick):
                    with self._lock:
                        closed = bld.add_tick(tick)
                        self.writers[tk].write_tick(tick)
                        self.stats[f'{tk}_ticks'] += 1
                        # Для каждой закрытой свечи — вычисляем признаки
                        for candle in closed:
                            all_c = bld.get_last_candles(candle.tf, 250)
                            ob = self.latest_ob.get(tk)
                            features = FeatureEngineer.compute(all_c, ob)
                            # Кластерные признаки
                            cluster_feat = self.cl_proc.get_cluster_features(sid)
                            features.update(cluster_feat)
                            # Метки (если есть следующие свечи в истории)
                            labels = {}
                            idx = len(all_c) - 1
                            future = all_c[idx+1:idx+21] if idx < len(all_c)-1 else []
                            if future:
                                labels = self.label_gen.label_candle(candle, future)
                            self.writers[tk].write_candle_with_features(candle, features, labels)
                            self.stats[f'{tk}_candles_{candle.tf}'] += 1

                def on_cluster(raw):
                    bar = self.cl_proc.parse(raw)
                    if bar:
                        bar.ticker = tk
                        self.writers[tk].write_cluster(bar)
                        self.stats[f'{tk}_clusters'] += 1

                def on_finrez(data):
                    pass  # finrez глобальный, не per-ticker

                return on_tick, on_cluster, on_finrez

            ot, oc, of = make_callbacks(ticker, builder)
            sr = SignalRCollector(self.client.token, sid, ticker, ot, oc, of)
            sr.start()
            self.signalr[ticker] = sr

        # Фоновые потоки
        threading.Thread(target=self._orderbook_loop, daemon=True).start()
        threading.Thread(target=self._positions_loop, daemon=True).start()
        threading.Thread(target=self._stats_loop, daemon=True).start()

        log.info("=" * 70)
        log.info("AI DATA COLLECTOR RUNNING")
        log.info(f"Output: {self.output_dir.absolute()}")
        log.info("Ctrl+C to stop and export")
        log.info("=" * 70)

        # REST polling если SignalR недоступен
        if not SIGNALR_OK:
            self._rest_polling_loop()

    def _orderbook_loop(self):
        """Периодически снимает снапшоты стакана"""
        while self._running:
            for ticker, builder in list(self.builders.items()):
                try:
                    info = self.security_map[ticker]
                    raw  = self.client.get_quotes(info['id'])
                    if raw:
                        snap = self.ob_proc.parse(info['id'], ticker, raw)
                        if snap:
                            self.latest_ob[ticker] = snap
                            self.writers[ticker].write_orderbook(snap)
                            self.stats[f'{ticker}_ob'] += 1
                except Exception as e:
                    log.debug(f"OB loop {ticker}: {e}")
            time.sleep(ORDERBOOK_INTERVAL)

    def _positions_loop(self):
        """Периодически обновляет закрытые позиции"""
        while self._running:
            time.sleep(POSITIONS_INTERVAL)
            try:
                new_positions = self.label_gen.load()
                for pos in new_positions:
                    for tk, info in self.security_map.items():
                        if info['id'] == pos.security_id and tk in self.writers:
                            self.writers[tk].write_position(pos)
            except Exception as e:
                log.debug(f"Positions loop: {e}")

    def _stats_loop(self):
        """Вывод статистики каждые 30 сек"""
        while self._running:
            time.sleep(30)
            log.info("─── STATS ───────────────────────────────────────────")
            for k, v in sorted(self.stats.items()):
                log.info(f"  {k}: {v}")
            # Flush writers
            for writer in self.writers.values():
                writer.flush_all()

    def _rest_polling_loop(self):
        """
        Fallback: REST polling если нет signalrcore.
        Опрашивает /Quotes каждые 0.5 сек и строит псевдо-тики.
        """
        log.warning("REST polling mode (install signalrcore for real-time ticks)")
        prev_prices: Dict[str, float] = {}

        while self._running:
            for ticker, builder in list(self.builders.items()):
                try:
                    info = self.security_map[ticker]
                    raw  = self.client.get_quotes(info['id'])
                    if not raw:
                        continue
                    snap = self.ob_proc.parse(info['id'], ticker, raw)
                    if not snap:
                        continue
                    self.latest_ob[ticker] = snap

                    mid = (snap.bid + snap.ask) / 2 if snap.bid and snap.ask else 0
                    if mid <= 0:
                        continue

                    prev = prev_prices.get(ticker, mid)
                    side = 0 if mid >= prev else 1   # простая эвристика
                    tick = Tick(
                        ts=int(time.time() * 1000),
                        price=mid, volume=1.0, side=side,
                        ticker=ticker, security_id=info['id']
                    )
                    with self._lock:
                        closed = builder.add_tick(tick)
                        self.writers[ticker].write_tick(tick)
                        self.writers[ticker].write_orderbook(snap)
                        for candle in closed:
                            all_c = builder.get_last_candles(candle.tf, 250)
                            features = FeatureEngineer.compute(all_c, snap)
                            cluster_feat = self.cl_proc.get_cluster_features(info['id'])
                            features.update(cluster_feat)
                            self.writers[ticker].write_candle_with_features(candle, features)
                            self.stats[f'{ticker}_candles_{candle.tf}'] += 1
                        self.stats[f'{ticker}_polls'] += 1

                    prev_prices[ticker] = mid
                except Exception as e:
                    log.debug(f"REST poll {ticker}: {e}")

            time.sleep(0.5)

    def stop(self):
        log.info("Stopping collector...")
        self._running = False

        # Остановить SignalR
        for sr in self.signalr.values():
            sr.stop()

        # Финальный flush
        log.info("Flushing all buffers...")
        for writer in self.writers.values():
            writer.flush_all()

        # Экспорт
        log.info("Exporting data...")
        for ticker, writer in self.writers.items():
            log.info(f"  {ticker}:")
            writer.to_parquet()
            writer.to_npz('1m')
            writer.to_npz('5m')
            writer.to_jsonl('1m')
            writer.close()

        # Итоговая статистика
        log.info("═" * 70)
        log.info("FINAL STATS:")
        for k, v in sorted(self.stats.items()):
            log.info(f"  {k}: {v}")
        log.info(f"Data saved to: {self.output_dir.absolute()}")
        log.info("═" * 70)


# ════════════════════════════════════════════════════════════════════════════════
# OFFLINE LABELER — размечает уже собранные данные
# ════════════════════════════════════════════════════════════════════════════════

def label_existing_data(data_dir: Path):
    """
    Загружает уже сохранённые CSV свечей и добавляет метки
    на основе N-bar forward returns.
    Используется post-hoc на собранных исторических данных.
    """
    if not PANDAS_OK:
        log.error("pandas required for offline labeling")
        return

    for csv_path in data_dir.rglob("candles_*.csv"):
        tf = csv_path.stem.replace("candles_", "")
        log.info(f"Labeling {csv_path}...")

        df = pd.read_csv(csv_path)
        if 'close' not in df.columns:
            continue

        closes = df['close'].values

        # N-bar forward returns
        for n in [1, 3, 5, 10]:
            col = f'ret_{n}bar'
            if col not in df.columns:
                df[col] = np.nan
                for i in range(len(closes) - n):
                    df.loc[i, col] = (closes[i+n] / closes[i] - 1) * 100
                df[f'dir_{n}bar'] = (df[col] > 0).astype(int)

        # TP/SL labels (0.5%/0.5%)
        tp_pct = 0.005
        sl_pct = 0.005
        tp_hits = [0] * len(closes)
        sl_hits = [0] * len(closes)

        for i in range(len(closes) - 1):
            ep = closes[i]
            tp = ep * (1 + tp_pct)
            sl = ep * (1 - sl_pct)
            for j in range(i+1, min(i+21, len(closes))):
                h = df['high'].values[j] if 'high' in df.columns else closes[j]
                l = df['low'].values[j]  if 'low'  in df.columns else closes[j]
                if h >= tp:
                    tp_hits[i] = 1; break
                if l <= sl:
                    sl_hits[i] = 1; break

        df['tp_hit'] = tp_hits
        df['sl_hit'] = sl_hits
        df['outcome'] = df.apply(
            lambda r: 1 if r['tp_hit'] == 1 else (0 if r['sl_hit'] == 1 else -1), axis=1
        )

        df.to_csv(csv_path, index=False)

        # Пересохраняем parquet + npz
        pq_path = csv_path.with_suffix('.parquet')
        df.to_parquet(pq_path, index=False, compression='snappy')

        num_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        label_cols = [c for c in num_cols if c.startswith(('ret_', 'dir_', 'tp_', 'sl_', 'outcome'))]
        feat_cols  = [c for c in num_cols if c not in label_cols and c not in ('ts_open', 'ts_close')]
        if feat_cols and label_cols:
            npz_path = csv_path.with_suffix('.npz')
            np.savez_compressed(
                npz_path,
                features=df[feat_cols].fillna(0).values.astype(np.float32),
                labels=df[label_cols].fillna(0).values.astype(np.float32),
                feature_names=np.array(feat_cols),
                label_names=np.array(label_cols),
            )
            log.info(f"  → {pq_path.name}: {len(df)} rows, {len(feat_cols)} features, {len(label_cols)} labels")


# ════════════════════════════════════════════════════════════════════════════════
# DATA SUMMARY — выводит что собрано
# ════════════════════════════════════════════════════════════════════════════════

def print_summary(data_dir: Path):
    print("\n" + "═" * 70)
    print(" СОБРАННЫЕ ДАННЫЕ — ОТЧЁТ")
    print("═" * 70)
    total_rows = 0
    for ticker_dir in sorted(data_dir.iterdir()):
        if not ticker_dir.is_dir():
            continue
        print(f"\n  📂 {ticker_dir.name}")
        for f in sorted(ticker_dir.iterdir()):
            if f.suffix == '.csv':
                try:
                    with open(f) as fh:
                        n = sum(1 for _ in fh) - 1
                    size = f.stat().st_size / 1024
                    print(f"    {f.name:<30} {n:>6} rows  {size:>7.1f} KB")
                    total_rows += n
                except Exception:
                    pass
            elif f.suffix == '.npz':
                try:
                    data = np.load(f, allow_pickle=True)
                    feat = data['features']
                    print(f"    {f.name:<30} shape={feat.shape}")
                except Exception:
                    pass
    print(f"\n  ВСЕГО строк: {total_rows:,}")
    print("═" * 70)


# ════════════════════════════════════════════════════════════════════════════════
# CLI
# ════════════════════════════════════════════════════════════════════════════════

def parse_args():
    p = argparse.ArgumentParser(
        description="Cassandra AI Training Data Collector",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
ПРИМЕРЫ:
  # Собирать данные по SBER и GAZP
  python cassandra_ai_collector.py --tickers SBER GAZP

  # Все фьючерсы FORTS
  python cassandra_ai_collector.py --tickers SBRF GAZPF LKOHF SNGSPF

  # Только разметить уже собранные данные
  python cassandra_ai_collector.py --label-only --data-dir ./ai_training_data

  # Показать что собрано
  python cassandra_ai_collector.py --summary
        """
    )
    p.add_argument('--tickers', nargs='+', default=DEFAULT_TICKERS,
                   help='Список тикеров')
    p.add_argument('--data-dir', type=Path, default=DATA_DIR,
                   help='Директория для данных')
    p.add_argument('--duration', type=int, default=0,
                   help='Длительность сбора в секундах (0 = бесконечно)')
    p.add_argument('--label-only', action='store_true',
                   help='Только разметить существующие данные')
    p.add_argument('--summary', action='store_true',
                   help='Показать сводку собранных данных')
    return p.parse_args()


def main():
    args = parse_args()

    if args.summary:
        print_summary(args.data_dir)
        return

    if args.label_only:
        label_existing_data(args.data_dir)
        return

    collector = AIDataCollector(args.tickers, args.data_dir)

    # Graceful shutdown
    def shutdown(sig, frame):
        log.info("Shutdown signal received...")
        collector.stop()
        import sys; sys.exit(0)

    signal.signal(signal.SIGINT,  shutdown)
    signal.signal(signal.SIGTERM, shutdown)

    try:
        collector.setup()
        collector.start()

        if args.duration > 0:
            log.info(f"Will stop after {args.duration} seconds")
            time.sleep(args.duration)
            collector.stop()
        else:
            # Бесконечный сбор
            while True:
                time.sleep(1)

    except KeyboardInterrupt:
        collector.stop()
    except Exception as e:
        log.error(f"Fatal error: {e}")
        import traceback; traceback.print_exc()
        collector.stop()


if __name__ == "__main__":
    main()
