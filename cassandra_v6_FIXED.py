#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cassandra Real Order Tester + API Server v5.0 FINAL
=====================================================
Полностью рабочая версия с РЕАЛЬНЫМ размещением ордеров.

Что делает:
  1. Авторизация
  2. Получение торгового счёта
  3. Поиск инструмента (SBER, GAZP и т.д.)
  4. Получение текущей цены из стакана
  5. Расчёт уровней TP/SL
  6. Размещение BUY ордера (лимитный по ask)
  7. Размещение TP/SL ордеров (StopLimit)
  8. Мониторинг позиции до срабатывания TP/SL/Timeout
  9. Закрытие позиции
  10. Очистка оставшихся ордеров
  11. Финальный отчёт

Автор: Evgeniy Koshtenko
Версия: 5.0 FINAL
"""

import requests
import json
import time
import logging
import sys
import traceback
import threading
import webbrowser
import os
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Tuple
from dataclasses import dataclass, field, asdict
from enum import Enum
from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
from flask_socketio import SocketIO, emit

# ════════════════════════════════════════════════════════════════════════════════
# ОТКЛЮЧИТЬ ЛИШНИЕ ЛОГИ
# ════════════════════════════════════════════════════════════════════════════════

logging.getLogger('werkzeug').setLevel(logging.ERROR)
logging.getLogger('werkzeug').propagate = False
logging.getLogger('flask.app').setLevel(logging.ERROR)
logging.getLogger('flask.app').propagate = False
logging.getLogger('flask_socketio').setLevel(logging.WARNING)
logging.getLogger('python-socketio').setLevel(logging.WARNING)

for handler in logging.root.handlers[:]:
    logging.root.removeHandler(handler)

# ════════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ════════════════════════════════════════════════════════════════════════════════

BASE_URL = "https://api-cassandra.lite-invest.com"
USERNAME = "Hasanov777@ya.ru"
PASSWORD = "Hasanov777!"

TIMEOUT = 30
REQUEST_DELAY = 0.4

DEFAULT_CONFIG = {
    "ticker": "SBER",
    "tp_percent": 0.5,
    "sl_percent": 0.5,
    "volume": 1,
    "poll_interval": 10,
    "max_monitor_minutes": 120,
    "board": 1,  # 0=StockMoex, 1=FortsMoex (фьючерсы), 2=Other
}

# Маппинг тикер -> поиск фьючерса на FORTS (board=1)
FUTURES_SEARCH_MAP = {
    "SBER": "SRH",    # SBRF фьючерс
    "GAZP": "GAZR",   # GAZR фьючерс
    "LKOH": "LKH",    # LKOH фьючерс
    "ROSN": "RNH",    # ROSN фьючерс
    "GMKN": "NGH",    # Норникель фьючерс
    "VTBR": "VBH",    # ВТБ фьючерс
    "Si":   "SiH",    # USD/RUB фьючерс
    "RTS":  "RIH",    # RTS фьючерс
    "BR":   "BRH",    # Brent фьючерс
    "GOLD": "GDH",    # Золото фьючерс
}

API_PORT = 5000
AUTO_OPEN_BROWSER = True

# ════════════════════════════════════════════════════════════════════════════════
# ПЕРЕЧИСЛЕНИЯ И СТРУКТУРЫ
# ════════════════════════════════════════════════════════════════════════════════

class TradeState(Enum):
    INIT = "INIT"
    BUYING = "BUYING"
    POSITION_OPEN = "POSITION_OPEN"
    TP_HIT = "TAKE_PROFIT_HIT"
    SL_HIT = "STOP_LOSS_HIT"
    CLOSING = "CLOSING"
    CLOSED = "CLOSED"
    ERROR = "ERROR"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"

class OrderSide(Enum):
    BUY = 0
    SELL = 1

@dataclass
class SecurityInfo:
    """Информация об инструменте"""
    security_id: str       # e.g. "SBER@TQBR"
    isin: str
    full_name: str
    board: int
    decimals: int
    lot: float
    price_step: float
    price_step_cost: float
    price_limit_high: int
    price_limit_low: int

@dataclass
class PriceSnapshot:
    """Снимок цены"""
    timestamp: str
    best_bid: float
    best_ask: float
    mid_price: float
    best_bid_raw: int
    best_ask_raw: int

@dataclass
class TradeLifecycle:
    ticker: str
    security_id: str = ""
    trade_account: str = ""
    lot_size: float = 0.0
    decimals: int = 2
    entry_order_id: str = ""
    entry_price: float = 0.0
    entry_price_raw: int = 0
    entry_time: str = ""
    volume: int = 0
    tp_price: float = 0.0
    tp_price_raw: int = 0
    sl_price: float = 0.0
    sl_price_raw: int = 0
    tp_order_id: str = ""
    sl_order_id: str = ""
    exit_price: float = 0.0
    exit_time: str = ""
    exit_reason: str = ""
    exit_order_id: str = ""
    pnl_points: float = 0.0
    pnl_rub: float = 0.0          # брутто P&L (без комиссии)
    pnl_percent: float = 0.0
    commission: float = 0.0       # FIX: комиссия брокера (обе стороны)
    pnl_net: float = 0.0          # FIX: нетто P&L после вычета комиссии
    duration_seconds: float = 0.0
    state: str = TradeState.INIT.value
    events: list = field(default_factory=list)
    price_history: list = field(default_factory=list)
    errors: list = field(default_factory=list)

# ════════════════════════════════════════════════════════════════════════════════
# ЛОГИРОВАНИЕ
# ════════════════════════════════════════════════════════════════════════════════

class TradeLogger:
    def __init__(self):
        self.session_id = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log = logging.getLogger('CassandraTrader')
        self.log.handlers.clear()
        self.log.setLevel(logging.DEBUG)
        self.log.propagate = False

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%H:%M:%S')
        ch.setFormatter(formatter)
        self.log.addHandler(ch)

        try:
            fh = logging.FileHandler(f'trading_session_{self.session_id}.log', encoding='utf-8')
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S'))
            self.log.addHandler(fh)
        except Exception:
            pass

    def banner(self, text: str):
        line = "=" * 90
        self.log.info(line)
        self.log.info(f" {text} ".center(90))
        self.log.info(line)

    def section(self, text: str):
        self.log.info("-" * 90)
        self.log.info(f"  {text}")
        self.log.info("-" * 90)

# ════════════════════════════════════════════════════════════════════════════════
# ОСНОВНОЙ ТЕСТЕР
# ════════════════════════════════════════════════════════════════════════════════

class CassandraRealOrderTester:
    """
    Полный цикл торговли: вход -> мониторинг -> выход.
    Использует проверенные эндпоинты Cassandra API.
    """

    def __init__(self, config: dict, update_callback=None):
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json",
            "Content-Type": "application/json"
        })

        self.config = {**DEFAULT_CONFIG, **config}
        self.update_callback = update_callback
        self.token: Optional[str] = None
        self.logger = TradeLogger()
        self.log = self.logger.log
        self.trade = TradeLifecycle(ticker=self.config['ticker'])
        self.sec_info: Optional[SecurityInfo] = None
        self.trade_account: str = ""
        self.should_stop = False
        # FIX TZ: храним время старта в UTC для корректного сравнения с API closeTime
        self.session_start_utc: datetime = datetime.utcnow()

    # ──────────────────────────────────────────────────────────────────────
    # Вспомогательные методы
    # ──────────────────────────────────────────────────────────────────────

    def _emit_update(self, event_type: str, data: Dict = None):
        if self.update_callback:
            try:
                self.update_callback(event_type, data or {})
            except Exception:
                pass

    def _add_event(self, event_type: str, description: str, data: Dict = None):
        event = {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "description": description,
            "data": data
        }
        self.trade.events.append(event)
        self.log.info(f"  [{event_type}] {description}")
        self._emit_update("event", event)

    def _set_state(self, state: TradeState):
        old = self.trade.state
        self.trade.state = state.value
        self.log.info(f"  STATE: {old} -> {state.value}")
        self._emit_update("state_changed", {"state": state.value, "old_state": old})

    def _request(self, method: str, endpoint: str, data: Optional[Dict] = None,
                 silent: bool = False) -> Tuple[Optional[int], Any, bool]:
        """HTTP-запрос с логированием"""
        url = f"{BASE_URL}{endpoint}"
        if not silent:
            self.log.debug(f"-> {method} {endpoint}")
            if data:
                self.log.debug(f"   Body: {json.dumps(data, ensure_ascii=False)}")

        try:
            kwargs = {"timeout": TIMEOUT}
            if data is not None:
                kwargs["json"] = data

            resp = getattr(self.session, method.lower())(url, **kwargs)

            try:
                body = resp.json()
            except Exception:
                body = resp.text or None

            if not silent:
                self.log.debug(f"<- {resp.status_code}")
                if isinstance(body, (dict, list)):
                    self.log.debug(f"   {json.dumps(body, ensure_ascii=False)[:500]}")

            return resp.status_code, body, resp.ok

        except Exception as e:
            if not silent:
                self.log.error(f"<- ERROR: {e}")
            return None, None, False

    def _raw_to_price(self, raw: int) -> float:
        """Конвертация нормализованной цены в обычную"""
        if self.sec_info:
            if self.sec_info.decimals == 0:
                return float(raw)
            return raw / (10 ** self.sec_info.decimals)
        return raw / 100

    def _price_to_raw(self, price: float) -> int:
        """Конвертация обычной цены в нормализованную"""
        if self.sec_info:
            if self.sec_info.decimals == 0:
                return int(round(price))
            return int(round(price * (10 ** self.sec_info.decimals)))
        return int(round(price * 100))

    def _align_price(self, raw_price: int) -> int:
        """Округление цены до шага цены инструмента"""
        if not self.sec_info or self.sec_info.price_step <= 0:
            return raw_price
        step_raw = self._price_to_raw(self.sec_info.price_step)
        if step_raw <= 0:
            return raw_price
        return round(raw_price / step_raw) * step_raw

    # ──────────────────────────────────────────────────────────────────────
    # 1. АВТОРИЗАЦИЯ
    # ──────────────────────────────────────────────────────────────────────

    def login(self) -> bool:
        """
        POST /api/Authentication/login
        {"email": "...", "password": "..."}
        """
        self.logger.section("АВТОРИЗАЦИЯ")

        status, body, ok = self._request(
            "POST", "/api/Authentication/login",
            data={"email": USERNAME, "password": PASSWORD}
        )

        if ok and isinstance(body, dict):
            token = body.get("accessToken")
            if token:
                self.token = token
                self.session.headers["Authorization"] = f"Bearer {token}"
                self._add_event("AUTH", "Авторизация успешна")
                self.log.info(f"  Token: {token[:30]}...")
                self._emit_update("login_success", {"token": token[:20] + "..."})
                return True

        if status == 401:
            self.log.error("  Неверный email или пароль!")
        else:
            self.log.error(f"  Ошибка авторизации: HTTP {status}")

        self._add_event("AUTH_ERROR", f"Ошибка авторизации: HTTP {status}")
        return False

    # ──────────────────────────────────────────────────────────────────────
    # 2. ПОЛУЧЕНИЕ СЧЁТА
    # ──────────────────────────────────────────────────────────────────────

    def get_account(self) -> bool:
        """
        GET /api/Account/TradeAccountNumbers
        Возвращает список строк (номера счетов)
        """
        self.logger.section("ПОЛУЧЕНИЕ ТОРГОВОГО СЧЁТА")

        status, body, ok = self._request("GET", "/api/Account/TradeAccountNumbers")

        if ok and isinstance(body, list) and body:
            self.trade_account = body[0]
            self.trade.trade_account = self.trade_account
            self._add_event("ACCOUNT", f"Торговый счёт: {self.trade_account}")
            self.log.info(f"  Все счета: {body}")
            self._emit_update("account_loaded", {"account": self.trade_account})
            return True

        self._add_event("ACCOUNT_ERROR", f"Не удалось получить счёт: HTTP {status}")
        return False

    # ──────────────────────────────────────────────────────────────────────
    # 3. ПОИСК ИНСТРУМЕНТА
    # ──────────────────────────────────────────────────────────────────────

    def find_security(self, ticker: str) -> bool:
        """
        Стратегия поиска:
          1) Если board=1 (FORTS), ищем фьючерс по маппингу FUTURES_SEARCH_MAP
          2) GET /Security/find/{board}/{ticker} — быстрый поиск
          3) Перебираем другие площадки если не нашли
        """
        self.logger.section(f"ПОИСК ИНСТРУМЕНТА: {ticker}")

        board = self.config.get('board', 1)

        # Список вариантов поиска: (board_id, search_query)
        search_variants = []

        # Для FORTS: используем маппинг тикер -> фьючерс
        if board == 1:
            upper_ticker = ticker.upper()
            futures_prefix = FUTURES_SEARCH_MAP.get(upper_ticker)
            if futures_prefix:
                search_variants.append((1, futures_prefix))
            search_variants.append((1, ticker))
        else:
            search_variants.append((board, ticker))

        # Добавляем другие площадки как fallback
        for b in [0, 1, 2]:
            if (b, ticker) not in search_variants:
                search_variants.append((b, ticker))

        for board_id, query in search_variants:
            status, body, ok = self._request("GET", f"/Security/find/{board_id}/{query}")
            if ok and isinstance(body, list) and body:
                sec = body[0]
                self.sec_info = SecurityInfo(
                    security_id=sec.get('securityId', ''),
                    isin=sec.get('isin', ticker),
                    full_name=sec.get('fullName', ticker),
                    board=sec.get('board', board_id),
                    decimals=sec.get('decimals', 0),
                    lot=sec.get('lot', 1.0),
                    price_step=sec.get('priceStep', 1.0),
                    price_step_cost=sec.get('priceStepCost', 1.0),
                    price_limit_high=sec.get('priceLimitHigh', 0),
                    price_limit_low=sec.get('priceLimitLow', 0),
                )
                self.trade.security_id = self.sec_info.security_id
                self.trade.lot_size = self.sec_info.lot
                self.trade.decimals = self.sec_info.decimals
                self._add_event("SECURITY_FOUND",
                    f"{self.sec_info.full_name} ({self.sec_info.security_id}) board={self.sec_info.board}", {
                    "security_id": self.sec_info.security_id,
                    "isin": self.sec_info.isin,
                    "lot": self.sec_info.lot,
                    "decimals": self.sec_info.decimals,
                    "price_step": self.sec_info.price_step,
                    "board": self.sec_info.board,
                })
                self._emit_update("security_found", {
                    "security_id": self.sec_info.security_id,
                    "name": self.sec_info.full_name,
                })
                return True
            time.sleep(REQUEST_DELAY)

        self._add_event("SEC_NOT_FOUND", f"Инструмент {ticker} не найден")
        return False

    # ──────────────────────────────────────────────────────────────────────
    # 4. ПОЛУЧЕНИЕ ТЕКУЩЕЙ ЦЕНЫ (СТАКАН)
    # ──────────────────────────────────────────────────────────────────────

    def get_price(self, silent: bool = False) -> Optional[PriceSnapshot]:
        """
        GET /Quotes/{securityId}/{scale}

        Стакан возвращает:
          - bb/ba: лучший bid/ask (нормализованные, верхнеуровневые)
          - b[]: массив bid-уровней, каждый {p: цена, l: {v: объём}}
          - a[]: массив ask-уровней, каждый {p: цена, l: {v: объём}}

        Пробуем scale = decimals, потом 0.01
        """
        if not self.sec_info:
            return None

        # Пробуем варианты scale: decimals, потом "0.01"
        scales = [str(self.sec_info.decimals)]
        if self.sec_info.decimals > 0:
            scales.append("0.01")
        for scale in scales:
            status, body, ok = self._request(
                "GET", f"/Quotes/{self.sec_info.security_id}/{scale}",
                silent=silent
            )
            if ok and isinstance(body, dict) and (body.get('bb', 0) > 0 or body.get('a', [])):
                break
        else:
            return None

        if not isinstance(body, dict):
            return None

        bb_raw = 0
        ba_raw = 0

        # Способ 1: Верхнеуровневые bb/ba
        bb_raw = body.get('bb', 0)
        ba_raw = body.get('ba', 0)

        # Способ 2: Из массивов a[]/b[] (формат из документации)
        if bb_raw == 0:
            bids = body.get('b', [])
            if bids and isinstance(bids, list):
                first_bid = bids[0]
                bb_raw = first_bid.get('p', 0)

        if ba_raw == 0:
            asks = body.get('a', [])
            if asks and isinstance(asks, list):
                # Первый ask = лучший (наименьший)
                first_ask = asks[0]
                ba_raw = first_ask.get('p', 0)

        if bb_raw == 0 or ba_raw == 0:
            if not silent:
                self.log.warning(f"  Цена не получена из стакана. Ответ: {json.dumps(body, ensure_ascii=False)[:300]}")
            return None

        bb = self._raw_to_price(bb_raw)
        ba = self._raw_to_price(ba_raw)
        mid = (bb + ba) / 2

        snap = PriceSnapshot(
            timestamp=datetime.now().isoformat(),
            best_bid=bb,
            best_ask=ba,
            mid_price=mid,
            best_bid_raw=bb_raw,
            best_ask_raw=ba_raw,
        )

        if not silent:
            self.log.info(f"  Bid: {bb:.2f} | Ask: {ba:.2f} | Mid: {mid:.2f} | Spread: {ba - bb:.2f}")
            self._emit_update("price_update", {
                "bid": bb, "ask": ba, "mid": mid, "spread": ba - bb
            })

        return snap

    # ──────────────────────────────────────────────────────────────────────
    # 5. ПОЗИЦИИ
    # ──────────────────────────────────────────────────────────────────────

    def get_positions(self) -> List[Dict]:
        """Получить открытые позиции через /api/Positions/open/{email}"""
        status, body, ok = self._request("GET", f"/api/Positions/open/{USERNAME}", silent=True)
        if ok and isinstance(body, list):
            return body

        # Fallback: /api/positions
        status, body, ok = self._request("GET", "/api/positions", silent=True)
        if ok and isinstance(body, list):
            return body
        return []

    def has_position(self, security_id: str) -> Optional[Dict]:
        for pos in self.get_positions():
            # /api/Positions/open returns 'secId', /api/positions returns 'securityId'
            pos_sec = pos.get('secId', pos.get('securityId', ''))
            if str(pos_sec) == str(security_id):
                return pos
        return None

    # ──────────────────────────────────────────────────────────────────────
    # 6. АКТИВНЫЕ ОРДЕРА
    # ──────────────────────────────────────────────────────────────────────

    def get_active_orders(self) -> List[Dict]:
        """GET /Order/{email}/Active — активные ордера"""
        status, body, ok = self._request("GET", f"/Order/{USERNAME}/Active", silent=True)
        if ok and isinstance(body, dict):
            return body.get("data", [])
        if ok and isinstance(body, list):
            return body
        return []

    def get_filled_orders(self) -> List[Dict]:
        """GET /Order/{email}/3 — исполненные ордера (state=3)"""
        status, body, ok = self._request("GET", f"/Order/{USERNAME}/3", silent=True)
        if ok and isinstance(body, dict):
            return body.get("data", [])
        return []

    # ──────────────────────────────────────────────────────────────────────
    # 7. РАЗМЕЩЕНИЕ ОРДЕРА
    # ──────────────────────────────────────────────────────────────────────

    def place_order(self, side: OrderSide, price_raw: int, volume: int) -> Optional[str]:
        """
        POST /Order — Выставить ордер (заявку) на биржу.

        OrderCreationRequest:
          required: email, securityId, side, volume
          optional: tradeAccount, price (int, нормализованная цена)
        """
        order_data = {
            "email": USERNAME,
            "tradeAccount": self.trade_account,
            "securityId": str(self.sec_info.security_id),
            "price": price_raw,
            "volume": volume,
            "side": side.value,
        }

        side_name = "BUY" if side == OrderSide.BUY else "SELL"
        price_human = self._raw_to_price(price_raw)
        self._add_event(
            f"ORDER_{side_name}_SUBMIT",
            f"{side_name} {volume} лот @ {price_human:.2f}",
            order_data
        )

        # POST /Order — проверенный эндпоинт
        self.log.info(f"  POST /Order: {side_name} {volume} @ {price_human:.2f}")
        status, body, ok = self._request("POST", "/Order", data=order_data)

        if ok and isinstance(body, dict) and body.get("data"):
            msg = body.get("message", "")
            self._add_event(
                f"ORDER_{side_name}_PLACED",
                f"Ордер отправлен: {msg}",
                {"message": msg}
            )
            self._emit_update("order_placed", {
                "side": side_name, "price": price_human, "message": msg
            })
            return msg or "ok"

        error_msg = body.get("message", str(body)) if isinstance(body, dict) else str(body)
        self._add_event(f"ORDER_{side_name}_ERROR", f"HTTP {status}: {error_msg}")
        return None

    # ──────────────────────────────────────────────────────────────────────
    # 8. STOP-LIMIT ОРДЕРА (TP / SL)
    # ──────────────────────────────────────────────────────────────────────

    def place_stop_limit(self, side: OrderSide, price_raw: int, volume: int) -> Optional[str]:
        """
        POST /OrderStopLimit/Set
        Размещает StopLimit ордер.
        price — нормализованная цена (int64)
        """
        data = {
            "tradeAccount": self.trade_account,
            "securityId": str(self.sec_info.security_id),
            "price": price_raw,
            "volume": volume,
            "side": side.value,
        }

        side_name = "BUY" if side == OrderSide.BUY else "SELL"
        price_human = self._raw_to_price(price_raw)
        self._add_event(
            f"STOP_LIMIT_{side_name}_SUBMIT",
            f"StopLimit {side_name} {volume} лот @ {price_human:.2f}",
            data
        )

        status, body, ok = self._request("POST", "/OrderStopLimit/Set", data=data)

        if ok and isinstance(body, dict) and body.get("data"):
            order_id = ""
            msg = body.get("message", "")
            if "Id=" in msg:
                order_id = msg.split("Id=")[-1].strip()

            self._add_event(
                f"STOP_LIMIT_{side_name}_PLACED",
                f"StopLimit размещён: {order_id}",
                {"order_id": order_id}
            )
            return order_id or "unknown"

        error_msg = body.get("message", str(body)) if isinstance(body, dict) else str(body)
        self._add_event(f"STOP_LIMIT_{side_name}_ERROR", f"Ошибка: {error_msg}")
        return None

    def cancel_stop_limit(self, order_id: str) -> bool:
        """DELETE /OrderStopLimit/DelById/{id}"""
        status, body, ok = self._request("DELETE", f"/OrderStopLimit/DelById/{order_id}")
        if ok and isinstance(body, dict) and body.get("data"):
            self._add_event("STOP_LIMIT_CANCELLED", f"StopLimit {order_id} отменён: {body.get('message', '')}")
            return True
        return False

    def cancel_stop_limit_by_price(self, price_raw: int) -> bool:
        """DELETE /OrderStopLimit/Del — удалить StopLimit по цене"""
        data = {
            "tradeAccount": self.trade_account,
            "securityId": self.sec_info.security_id,
            "price": price_raw,
        }
        status, body, ok = self._request("DELETE", "/OrderStopLimit/Del", data=data)
        if ok and isinstance(body, dict) and body.get("data"):
            self._add_event("STOP_LIMIT_CANCELLED_BY_PRICE", body.get("message", ""))
            return True
        return False

    def get_stop_limit_orders(self) -> List[Dict]:
        """GET /OrderStopLimit/GetByUser — все StopLimit ордера пользователя"""
        status, body, ok = self._request("GET", "/OrderStopLimit/GetByUser", silent=True)
        if ok and isinstance(body, dict):
            return body.get("data", [])
        return []

    def get_stop_limit_orders_for_security(self) -> List[Dict]:
        """GET /OrderStopLimit/GetByTradeAccountBySecId/{ta}/{secId} — StopLimit по инструменту"""
        if not self.sec_info or not self.trade_account:
            return self.get_stop_limit_orders()
        status, body, ok = self._request(
            "GET",
            f"/OrderStopLimit/GetByTradeAccountBySecId/{self.trade_account}/{self.sec_info.security_id}",
            silent=True
        )
        if ok and isinstance(body, dict):
            return body.get("data", [])
        return []

    # ──────────────────────────────────────────────────────────────────────
    # 9. SLTP через ConditionalOrder
    # ──────────────────────────────────────────────────────────────────────

    def set_sltp(self, price_raw: int, order_type: str = "SL") -> bool:
        """POST /ConditionalOrder/SLTPOrder"""
        data = {
            "tradeAccount": self.trade_account,
            "securityId": self.sec_info.security_id,
            "price": price_raw,
        }

        price_human = self._raw_to_price(price_raw)
        self._add_event(f"SLTP_{order_type}_SUBMIT", f"{order_type} @ {price_human:.2f}")

        status, body, ok = self._request("POST", "/ConditionalOrder/SLTPOrder", data=data)
        if ok and isinstance(body, dict) and body.get("data"):
            self._add_event(f"SLTP_{order_type}_SET", f"{order_type} установлен @ {price_human:.2f}")
            return True

        error_msg = body.get("message", str(body)) if isinstance(body, dict) else str(body)
        self._add_event(f"SLTP_{order_type}_ERROR", f"Ошибка: {error_msg}")
        return False

    # ──────────────────────────────────────────────────────────────────────
    # 10. МОНИТОРИНГ ПОЗИЦИИ
    # ──────────────────────────────────────────────────────────────────────

    def monitor_trade(self) -> TradeState:
        """Мониторинг позиции до срабатывания TP/SL/Timeout"""
        self.logger.banner("МОНИТОРИНГ СДЕЛКИ")

        entry = self.trade.entry_price
        tp = self.trade.tp_price
        sl = self.trade.sl_price
        poll_interval = self.config['poll_interval']
        max_minutes = self.config['max_monitor_minutes']

        self.log.info(f"  Вход:        {entry:.2f}")
        self.log.info(f"  Take-Profit: {tp:.2f} (+{self.config['tp_percent']}%)")
        self.log.info(f"  Stop-Loss:   {sl:.2f} (-{self.config['sl_percent']}%)")
        self.log.info(f"  Интервал:    {poll_interval} сек")
        self.log.info(f"  Макс. время: {max_minutes} мин")
        self.log.info("")

        deadline = datetime.now() + timedelta(minutes=max_minutes)
        tick = 0

        while datetime.now() < deadline and not self.should_stop:
            tick += 1
            try:
                snap = self.get_price(silent=True)
                if not snap:
                    self.log.warning(f"  [{tick}] Не удалось получить цену, пауза...")
                    time.sleep(poll_interval)
                    continue

                price = snap.mid_price
                bid = snap.best_bid
                ask = snap.best_ask

                self.trade.price_history.append({
                    "tick": tick,
                    "time": snap.timestamp,
                    "bid": bid,
                    "ask": ask,
                    "mid": price,
                })

                pnl_pct = ((price - entry) / entry) * 100
                indicator = "^" if price > entry else ("v" if price < entry else "=")

                self.log.info(
                    f"  [{tick:4d}] {indicator} Bid={bid:.2f} Ask={ask:.2f} "
                    f"Mid={price:.2f} | PnL: {pnl_pct:+.3f}% | "
                    f"SL={sl:.2f} .. TP={tp:.2f}"
                )

                self._emit_update("price_tick", {
                    "tick": tick, "bid": bid, "ask": ask, "mid": price,
                    "pnl_pct": pnl_pct, "tp": tp, "sl": sl
                })

                # --- Проверка TP ---
                if price >= tp or bid >= tp:
                    self._add_event("TP_TRIGGERED", f"Take-Profit! Цена: {price:.2f} >= TP: {tp:.2f}")
                    self.trade.exit_price = price
                    self.trade.exit_time = snap.timestamp
                    self.trade.exit_reason = "TAKE_PROFIT"
                    self._set_state(TradeState.TP_HIT)
                    return TradeState.TP_HIT

                # --- Проверка SL ---
                if price <= sl or ask <= sl:
                    self._add_event("SL_TRIGGERED", f"Stop-Loss! Цена: {price:.2f} <= SL: {sl:.2f}")
                    self.trade.exit_price = price
                    self.trade.exit_time = snap.timestamp
                    self.trade.exit_reason = "STOP_LOSS"
                    self._set_state(TradeState.SL_HIT)
                    return TradeState.SL_HIT

                # --- Проверка: позиция могла закрыться внешне ---
                if tick % 6 == 0:  # Каждые ~6 тиков проверяем позицию
                    pos = self.has_position(self.sec_info.security_id)
                    if pos is None and self.trade.state == TradeState.POSITION_OPEN.value:
                        self._add_event("POSITION_CLOSED_EXT",
                            f"Позиция закрыта внешне. Последняя цена: {price:.2f}")
                        self.trade.exit_price = price
                        self.trade.exit_time = snap.timestamp
                        if price >= entry:
                            self.trade.exit_reason = "TAKE_PROFIT (external)"
                            self._set_state(TradeState.TP_HIT)
                            return TradeState.TP_HIT
                        else:
                            self.trade.exit_reason = "STOP_LOSS (external)"
                            self._set_state(TradeState.SL_HIT)
                            return TradeState.SL_HIT

                    # Проверяем StopLimit ордера
                    sl_orders = self.get_stop_limit_orders_for_security()
                    active_ids = {o.get("id") for o in sl_orders}

                    if self.trade.tp_order_id and self.trade.tp_order_id not in active_ids:
                        self._add_event("TP_ORDER_EXECUTED", "TP StopLimit исполнен")
                        self.trade.exit_price = tp
                        self.trade.exit_time = snap.timestamp
                        self.trade.exit_reason = "TAKE_PROFIT (StopLimit)"
                        self._set_state(TradeState.TP_HIT)
                        return TradeState.TP_HIT

                    if self.trade.sl_order_id and self.trade.sl_order_id not in active_ids:
                        self._add_event("SL_ORDER_EXECUTED", "SL StopLimit исполнен")
                        self.trade.exit_price = sl
                        self.trade.exit_time = snap.timestamp
                        self.trade.exit_reason = "STOP_LOSS (StopLimit)"
                        self._set_state(TradeState.SL_HIT)
                        return TradeState.SL_HIT

            except KeyboardInterrupt:
                self._add_event("USER_INTERRUPT", "Мониторинг прерван пользователем")
                self._set_state(TradeState.CANCELLED)
                return TradeState.CANCELLED
            except Exception as e:
                self.log.error(f"  Ошибка мониторинга: {e}")
                self.trade.errors.append(str(e))

            time.sleep(poll_interval)

        if self.should_stop:
            self._add_event("USER_STOP", "Остановлено пользователем")
            self._set_state(TradeState.CANCELLED)
            return TradeState.CANCELLED

        self._add_event("TIMEOUT", f"Таймаут: {max_minutes} мин")
        snap = self.get_price(silent=True)
        if snap:
            self.trade.exit_price = snap.mid_price
            self.trade.exit_time = snap.timestamp
        self.trade.exit_reason = "TIMEOUT"
        self._set_state(TradeState.TIMEOUT)
        return TradeState.TIMEOUT

    # ──────────────────────────────────────────────────────────────────────
    # 11. ЗАКРЫТИЕ ПОЗИЦИИ
    # ──────────────────────────────────────────────────────────────────────

    def close_position(self) -> bool:
        """Закрыть позицию обратной сделкой"""
        self.logger.section("ЗАКРЫТИЕ ПОЗИЦИИ")
        self._set_state(TradeState.CLOSING)

        snap = self.get_price()
        if not snap:
            self.log.error("  Не удалось получить цену для закрытия")
            return False

        volume = self.config['volume']
        sell_price_raw = self._align_price(snap.best_bid_raw)
        price_human = self._raw_to_price(sell_price_raw)

        self._add_event("CLOSE_POSITION", f"Закрытие: SELL {volume} лот @ {price_human:.2f}")

        order_id = self.place_order(OrderSide.SELL, sell_price_raw, volume)
        if order_id:
            self.trade.exit_order_id = order_id
            self.trade.exit_price = price_human
            self.trade.exit_time = datetime.now().isoformat()
            self._set_state(TradeState.CLOSED)
            return True

        # Fallback: StopLimit SELL
        self.log.info("  Пробую закрыть через StopLimit SELL...")
        sl_id = self.place_stop_limit(OrderSide.SELL, sell_price_raw, volume)
        if sl_id:
            self.trade.exit_order_id = sl_id
            self.trade.exit_price = price_human
            self.trade.exit_time = datetime.now().isoformat()
            self._set_state(TradeState.CLOSED)
            return True

        self._add_event("CLOSE_FAILED", "Не удалось закрыть позицию")
        return False

    # ──────────────────────────────────────────────────────────────────────
    # 12. ОЧИСТКА
    # ──────────────────────────────────────────────────────────────────────

    def cleanup(self):
        """Отменить все оставшиеся StopLimit ордера"""
        self.logger.section("ОЧИСТКА")

        # Способ 1: Быстрое удаление всех StopLimit по счёту+инструменту
        # DELETE /OrderStopLimit/DelById/{tradeAccount}/{securityId}
        if self.sec_info and self.trade_account:
            status, body, ok = self._request(
                "DELETE",
                f"/OrderStopLimit/DelById/{self.trade_account}/{str(self.sec_info.security_id)}"
            )
            if ok and isinstance(body, dict) and body.get("data"):
                self._add_event("CLEANUP_BULK", body.get("message", "Все StopLimit удалены"))
                return

        # Способ 2: Удаление по одному
        orders = self.get_stop_limit_orders()
        if not orders:
            self.log.info("  Нет активных StopLimit ордеров")
            return

        for order in orders:
            oid = order.get("id", "")
            if oid:
                self.cancel_stop_limit(oid)
                time.sleep(REQUEST_DELAY)

    # ──────────────────────────────────────────────────────────────────────
    # 13. ФИНАЛЬНЫЙ ОТЧЁТ
    # ──────────────────────────────────────────────────────────────────────

    def get_real_pnl_from_exchange(self) -> Optional[float]:
        """Получить реальный PnL с биржи из закрытых позиций"""
        status, body, ok = self._request("GET", f"/api/Positions/close/{USERNAME}", silent=True)
        if ok and isinstance(body, list):
            for pos in body:
                # Проверяем, соответствует ли позиция нашему инструменту и времени
                pos_sec = pos.get('secId', pos.get('securityId', ''))
                if str(pos_sec) == str(self.sec_info.security_id):
                    # Проверяем время закрытия позиции
                    close_time = pos.get('closeTime', '')
                    if close_time and self.trade.exit_time:
                        try:
                            close_dt = datetime.fromisoformat(close_time)
                            exit_dt = datetime.fromisoformat(self.trade.exit_time)
                            # Если время закрытия совпадает с нашим временем выхода (в пределах 1 минуты)
                            if abs((close_dt - exit_dt).total_seconds()) <= 60:
                                real_pnl = pos.get('realizedPnl', 0)
                                self.log.info(f"  Реальный PnL с биржи: {real_pnl} руб")
                                return real_pnl
                        except Exception as e:
                            self.log.warning(f"  Ошибка парсинга времени: {e}")
        
        # Fallback: попробуем получить через /api/FinancialResult
        status, body, ok = self._request("GET", "/api/FinancialResult", silent=True)
        if ok and isinstance(body, dict):
            # Ищем финансовый результат по нашему инструменту
            # Формат может отличаться, поэтому просто логируем для анализа
            self.log.info(f"  FinancialResult: {json.dumps(body, ensure_ascii=False)[:500]}")
        
        return None

    def get_real_trade_data(self) -> Dict:
        """
        Получить реальные данные о сделке с биржи.

        FIX TZ:  session_start_utc сравнивается с closeTime из API — оба в UTC.
                 Раньше datetime.now() (MSK=UTC+2) сравнивался с UTC closeTime →
                 фильтр отбрасывал сделки как "старые" (15:22 UTC < 17:20 MSK).

        FIX COM: Комиссия рассчитывается тремя способами (приоритет по порядку):
                 1. Прямо из поля commission/fee в ответе API
                 2. Разница realizedPnl − gross_pnl (если realizedPnl есть в ответе)
                 3. Запрос /api/FinancialResult
        """
        real_data = {
            'entry_price': None,
            'exit_price': None,
            'real_pnl': None,
            'commission': None,
            'trade_time': None
        }

        status, body, ok = self._request("GET", f"/api/Positions/close/{USERNAME}", silent=True)
        if not (ok and isinstance(body, list)):
            self.log.warning("  /api/Positions/close вернул пустой ответ")
            return real_data

        # --- выбираем нашу сделку по инструменту и времени ---
        for pos in body:
            pos_sec = pos.get('secId', pos.get('securityId', ''))
            if str(pos_sec) != str(self.sec_info.security_id):
                continue

            # FIX TZ: API отдаёт closeTime в UTC (без суффикса Z, но это UTC)
            close_time_str = pos.get('closeTime', '')
            if close_time_str:
                try:
                    close_dt = datetime.fromisoformat(close_time_str.replace('Z', ''))
                    # сравниваем два наивных UTC datetime
                    if close_dt < self.session_start_utc:
                        self.log.debug(
                            f"  Пропуск старой позиции: closeTime={close_time_str} "
                            f"< session_start_utc={self.session_start_utc.isoformat()}"
                        )
                        continue
                except Exception as e:
                    self.log.debug(f"  Не смог распарсить closeTime={close_time_str!r}: {e}")
                    # если время не парсится — не отбрасываем, берём позицию

            real_data['entry_price'] = pos.get('averageEntry', 0)
            real_data['exit_price']  = pos.get('averageExit', 0)
            real_data['real_pnl']    = pos.get('realizedPnl', 0)
            real_data['trade_time']  = close_time_str

            # --- FIX COM: ищем комиссию ---
            # Способ 1: прямое поле в позиции
            for fee_key in ('commission', 'fee', 'Commission', 'Fee', 'totalFee'):
                fee = pos.get(fee_key)
                if fee and float(fee) != 0:
                    real_data['commission'] = abs(float(fee))
                    self.log.debug(f"  Комиссия из поля [{fee_key}]: {real_data['commission']:.2f} руб")
                    break

            # Способ 2: разница realizedPnl − gross_pnl
            if real_data['commission'] is None and real_data['real_pnl'] is not None:
                gross_pnl = self.trade.pnl_rub  # уже рассчитан в calculate_result
                real_pnl  = float(real_data['real_pnl'])
                diff = gross_pnl - real_pnl
                if abs(diff) > 0.01:  # есть реальная разница
                    real_data['commission'] = abs(diff)
                    self.log.debug(
                        f"  Комиссия из разницы gross({gross_pnl:.2f}) - realPnl({real_pnl:.2f})"
                        f" = {real_data['commission']:.2f} руб"
                    )

            # Способ 3: /api/FinancialResult
            if real_data['commission'] is None:
                fr_s, fr_b, fr_ok = self._request("GET", "/api/FinancialResult", silent=True)
                if fr_ok and isinstance(fr_b, dict):
                    self.log.debug(f"  FinancialResult raw: {json.dumps(fr_b, ensure_ascii=False)[:600]}")
                    for fee_key in ('commission', 'fee', 'totalFee', 'brokerFee'):
                        fee = fr_b.get(fee_key)
                        if fee and float(fee) != 0:
                            real_data['commission'] = abs(float(fee))
                            break
                elif fr_ok and isinstance(fr_b, list) and fr_b:
                    self.log.debug(f"  FinancialResult list[0]: {json.dumps(fr_b[0], ensure_ascii=False)[:400]}")

            self.log.info(f"  Найдена реальная сделка (UTC сессия с {self.session_start_utc.strftime('%H:%M:%S')}):")
            self.log.info(f"    Вход:     {real_data['entry_price']}")
            self.log.info(f"    Выход:    {real_data['exit_price']}")
            self.log.info(f"    PnL брутто: {real_data['real_pnl']}")
            if real_data['commission'] is not None:
                self.log.info(f"    Комиссия: {real_data['commission']:.2f} руб")
            return real_data

        self.log.info(
            f"  Реальная сделка в текущей UTC-сессии (с {self.session_start_utc.strftime('%H:%M:%S')} UTC) "
            f"не найдена в закрытых позициях"
        )
        return real_data

    def calculate_result(self):
        if self.trade.entry_price > 0 and self.trade.exit_price > 0:
            self.trade.pnl_points = self.trade.exit_price - self.trade.entry_price
            self.trade.pnl_percent = (self.trade.pnl_points / self.trade.entry_price) * 100
            shares = self.config['volume'] * self.trade.lot_size
            self.trade.pnl_rub = self.trade.pnl_points * shares
            # FIX COM: нетто считается ПОСЛЕ get_real_trade_data() которая уже заполнила commission
            self.trade.pnl_net = self.trade.pnl_rub - self.trade.commission

        if self.trade.entry_time and self.trade.exit_time:
            try:
                t1 = datetime.fromisoformat(self.trade.entry_time)
                t2 = datetime.fromisoformat(self.trade.exit_time)
                self.trade.duration_seconds = (t2 - t1).total_seconds()
            except Exception:
                pass

    def generate_report(self):
        self.calculate_result()
        self.logger.banner("ИТОГОВЫЙ ОТЧЁТ")

        t = self.trade
        self.log.info(f"  Инструмент:    {t.ticker} ({t.security_id})")
        self.log.info(f"  Счёт:          {t.trade_account}")
        self.log.info(f"  Объём:         {t.volume} лот ({t.volume * t.lot_size:.0f} шт)")
        self.log.info(f"  Состояние:     {t.state}")
        self.log.info("")
        self.log.info(f"  Вход:          {t.entry_price:.2f} @ {t.entry_time}")
        self.log.info(f"  Take-Profit:   {t.tp_price:.2f}")
        self.log.info(f"  Stop-Loss:     {t.sl_price:.2f}")
        self.log.info("")

        if t.exit_price > 0:
            self.log.info(f"  Выход:         {t.exit_price:.2f} @ {t.exit_time}")
            self.log.info(f"  Причина:       {t.exit_reason}")
            self.log.info("")
            self.log.info(f"  PnL (пункты):  {t.pnl_points:+.2f}")
            self.log.info(f"  PnL брутто:    {t.pnl_rub:+.2f} руб")
            if t.commission > 0:
                self.log.info(f"  Комиссия:      -{t.commission:.2f} руб  ← вычитается из PnL")
                self.log.info(f"  PnL нетто:     {t.pnl_net:+.2f} руб  ← РЕАЛЬНЫЙ результат")
            self.log.info(f"  PnL (%):       {t.pnl_percent:+.3f}%")
            if t.duration_seconds > 0:
                mins = t.duration_seconds / 60
                self.log.info(f"  Длительность:  {mins:.1f} мин ({t.duration_seconds:.0f} сек)")
        else:
            self.log.info(f"  Выход:         НЕ ЗАФИКСИРОВАН")

        self.log.info(f"  Тиков цены:    {len(t.price_history)}")
        self.log.info(f"  Событий:       {len(t.events)}")
        self.log.info(f"  Ошибок:        {len(t.errors)}")

        self._emit_update("pnl_calculated", {
            "pnl_points": t.pnl_points,
            "pnl_percent": t.pnl_percent,
            "pnl_rub": t.pnl_rub,
            "duration_seconds": t.duration_seconds,
        })
        self._emit_update("trade_completed", asdict(t))

        # JSON отчёт
        report_file = f"trade_report_{self.logger.session_id}.json"
        try:
            with open(report_file, 'w', encoding='utf-8') as f:
                json.dump(asdict(t), f, indent=2, ensure_ascii=False)
            self.log.info(f"  JSON-отчёт: {report_file}")
        except Exception:
            pass

    # ──────────────────────────────────────────────────────────────────────
    # ГЛАВНЫЙ СЦЕНАРИЙ
    # ──────────────────────────────────────────────────────────────────────

    def run(self):
        """Полный цикл: вход -> мониторинг -> выход"""
        self.logger.banner("CASSANDRA REAL ORDER TESTER v5.0 FINAL")
        self.log.info(f"  Инструмент:  {self.config['ticker']}")
        self.log.info(f"  TP:          +{self.config['tp_percent']}%")
        self.log.info(f"  SL:          -{self.config['sl_percent']}%")
        self.log.info(f"  Объём:       {self.config['volume']} лот")
        self.log.info(f"  Мониторинг:  до {self.config['max_monitor_minutes']} мин")
        self.log.info("")

        try:
            # ── 1. Авторизация ──
            if not self.login():
                self._set_state(TradeState.ERROR)
                return

            # ── 2. Счёт ──
            if not self.get_account():
                self._set_state(TradeState.ERROR)
                return

            # ── 3. Инструмент ──
            if not self.find_security(self.config['ticker']):
                self._set_state(TradeState.ERROR)
                return

            # ── 4. Текущие позиции (до начала) ──
            self.logger.section("ТЕКУЩИЕ ПОЗИЦИИ (до начала)")
            positions_before = self.get_positions()
            if positions_before:
                for pos in positions_before:
                    sec_name = pos.get('secName', pos.get('securityId', pos.get('secId', '?')))
                    vol = pos.get('currentPos', pos.get('volume', '?'))
                    price = pos.get('averageEntry', pos.get('price', '?'))
                    self.log.info(f"  {sec_name}: vol={vol} price={price}")
            else:
                self.log.info("  Нет открытых позиций")

            # ── 5. Текущая цена ──
            self.logger.section("ТЕКУЩАЯ ЦЕНА")
            snap = self.get_price()
            if not snap:
                self.log.error("  Не удалось получить цену")
                self._set_state(TradeState.ERROR)
                return

            # ── 6. Расчёт уровней ──
            self.logger.section("РАСЧЁТ УРОВНЕЙ ВХОДА / TP / SL")

            # Покупаем по ask (для мгновенного исполнения)
            entry_price_raw = self._align_price(snap.best_ask_raw)
            entry_price = self._raw_to_price(entry_price_raw)

            tp_price = entry_price * (1 + self.config['tp_percent'] / 100)
            sl_price = entry_price * (1 - self.config['sl_percent'] / 100)

            tp_price_raw = self._align_price(self._price_to_raw(tp_price))
            sl_price_raw = self._align_price(self._price_to_raw(sl_price))

            tp_price = self._raw_to_price(tp_price_raw)
            sl_price = self._raw_to_price(sl_price_raw)

            self.trade.volume = self.config['volume']
            self.trade.entry_price = entry_price
            self.trade.entry_price_raw = entry_price_raw
            self.trade.tp_price = tp_price
            self.trade.tp_price_raw = tp_price_raw
            self.trade.sl_price = sl_price
            self.trade.sl_price_raw = sl_price_raw

            cost = entry_price * self.config['volume'] * self.sec_info.lot
            self.log.info(f"  Цена входа:  {entry_price:.2f} (ask)")
            self.log.info(f"  Take-Profit: {tp_price:.2f} (+{self.config['tp_percent']}%)")
            self.log.info(f"  Stop-Loss:   {sl_price:.2f} (-{self.config['sl_percent']}%)")
            self.log.info(f"  Стоимость:   ~{cost:.2f} руб ({self.config['volume']} лот x {self.sec_info.lot:.0f} шт)")

            self._emit_update("levels_calculated", {
                "entry_price": entry_price,
                "tp_price": tp_price,
                "sl_price": sl_price,
                "cost": cost,
            })

            # ── 7. Размещение BUY ордера ──
            self.logger.banner("РАЗМЕЩЕНИЕ BUY ОРДЕРА")
            self._set_state(TradeState.BUYING)

            order_id = self.place_order(OrderSide.BUY, entry_price_raw, self.config['volume'])

            if not order_id:
                self.log.warning("  Обычный ордер не удался. Пробую через StopLimit BUY...")
                order_id = self.place_stop_limit(OrderSide.BUY, entry_price_raw, self.config['volume'])

            if not order_id:
                self.log.error("  Не удалось разместить ордер на покупку!")
                self._set_state(TradeState.ERROR)
                return

            self.trade.entry_order_id = order_id
            self.trade.entry_time = datetime.now().isoformat()
            self.log.info(f"  BUY ордер размещён: {order_id}")

            # Ждём исполнения
            self.log.info("  Ожидание исполнения BUY ордера (5 сек)...")
            time.sleep(5)

            # Проверяем позицию
            pos = self.has_position(self.sec_info.security_id)
            if pos:
                self._add_event("POSITION_CONFIRMED", "Позиция подтверждена", pos)
                self.log.info(f"  Позиция: {pos}")
            else:
                # Проверяем может ордер исполнился (state=3)
                filled = self.get_filled_orders()
                our_filled = [o for o in filled if str(o.get('securityId', '')) == str(self.sec_info.security_id)]
                if our_filled:
                    self._add_event("ORDER_FILLED_NO_POS",
                        "Ордер исполнен, но позиция не найдена (возможно уже закрыта)")
                else:
                    self._add_event("POSITION_NOT_FOUND",
                        "Позиция не обнаружена (ордер может быть в ожидании)")
                    # Ждём ещё
                    self.log.info("  Ожидание ещё 5 сек...")
                    time.sleep(5)
                    pos = self.has_position(self.sec_info.security_id)
                    if pos:
                        self._add_event("POSITION_CONFIRMED", "Позиция подтверждена (задержка)", pos)

            self._set_state(TradeState.POSITION_OPEN)

            # ── 8. Размещение TP и SL ордеров ──
            self.logger.section("РАЗМЕЩЕНИЕ TP / SL ОРДЕРОВ")

            # TP: StopLimit SELL при цене >= tp_price
            tp_id = self.place_stop_limit(OrderSide.SELL, tp_price_raw, self.config['volume'])
            if tp_id:
                self.trade.tp_order_id = tp_id
                self.log.info(f"  TP StopLimit SELL @ {tp_price:.2f}: {tp_id}")
            else:
                self.log.warning("  Не удалось установить TP через StopLimit, пробую SLTP API...")
                self.set_sltp(tp_price_raw, "TP")

            time.sleep(REQUEST_DELAY)

            # SL: StopLimit SELL при цене <= sl_price
            sl_id = self.place_stop_limit(OrderSide.SELL, sl_price_raw, self.config['volume'])
            if sl_id:
                self.trade.sl_order_id = sl_id
                self.log.info(f"  SL StopLimit SELL @ {sl_price:.2f}: {sl_id}")
            else:
                self.log.warning("  Не удалось установить SL через StopLimit, пробую SLTP API...")
                self.set_sltp(sl_price_raw, "SL")

            # ── 9. Мониторинг ──
            result = self.monitor_trade()

            # ── 10. Обработка результата ──
            self.logger.banner(f"РЕЗУЛЬТАТ: {result.value}")

            if result in (TradeState.TP_HIT, TradeState.SL_HIT):
                self.log.info(f"  Сделка завершена: {self.trade.exit_reason}")
                pos = self.has_position(self.sec_info.security_id)
                if pos:
                    self.log.info("  Позиция ещё открыта, закрываю...")
                    self.close_position()
                else:
                    self.log.info("  Позиция уже закрыта")
                    self._set_state(TradeState.CLOSED)

            elif result == TradeState.TIMEOUT:
                self.log.info("  Таймаут. Закрываю позицию...")
                self.close_position()

            elif result == TradeState.CANCELLED:
                self.log.info("  Остановлено. Закрываю позицию...")
                pos = self.has_position(self.sec_info.security_id)
                if pos:
                    self.close_position()

        except KeyboardInterrupt:
            self.log.warning("\n  Прервано (Ctrl+C)")
            self._set_state(TradeState.CANCELLED)
            self.trade.exit_reason = "USER_INTERRUPT"
            self.trade.exit_time = datetime.now().isoformat()
            try:
                if self.sec_info:
                    pos = self.has_position(self.sec_info.security_id)
                    if pos:
                        self.close_position()
            except Exception:
                pass

        except Exception as e:
            self.log.error(f"  КРИТИЧЕСКАЯ ОШИБКА: {e}")
            self.log.error(traceback.format_exc())
            self._set_state(TradeState.ERROR)
            self.trade.errors.append(f"{type(e).__name__}: {e}")

        finally:
            # Очистка
            try:
                self.cleanup()
            except Exception as e:
                self.log.error(f"  Ошибка при очистке: {e}")

            # ── Реальные данные с биржи ──────────────────────────────────
            # ВАЖНО: вызываем ДО generate_report, чтобы комиссия попала в trade
            real_data = self.get_real_trade_data()

            # Записываем комиссию в trade (calculate_result использует её для pnl_net)
            if real_data.get('commission') is not None:
                self.trade.commission = float(real_data['commission'])

            # Также: если realizedPnl из API отличается от нашего gross — это тоже комиссия
            if real_data.get('real_pnl') is not None and self.trade.commission == 0:
                gross = self.trade.pnl_rub
                real  = float(real_data['real_pnl'])
                diff  = gross - real
                if abs(diff) > 0.01:
                    self.trade.commission = abs(diff)
                    self.log.info(
                        f"  Комиссия (вычислена): gross={gross:+.2f} − realPnL={real:+.2f}"
                        f" = {self.trade.commission:.2f} руб"
                    )

            # ── Сравнение реальных и расчётных данных ───────────────────
            if real_data['entry_price'] is not None:
                entry_ok = abs(real_data['entry_price'] - self.trade.entry_price) < 0.01
                exit_ok  = abs(real_data['exit_price']  - self.trade.exit_price)  < 0.01

                self.log.info("  СРАВНЕНИЕ РЕАЛЬНЫХ И РАСЧЁТНЫХ ДАННЫХ:")
                self.log.info(
                    f"    Вход:   {real_data['entry_price']:.2f} (API) "
                    f"vs {self.trade.entry_price:.2f} (расчёт)  {'✓ СОВПАДАЕТ' if entry_ok else '✗ РАСХОЖДЕНИЕ!'}"
                )
                self.log.info(
                    f"    Выход:  {real_data['exit_price']:.2f} (API) "
                    f"vs {self.trade.exit_price:.2f} (расчёт)  {'✓ СОВПАДАЕТ' if exit_ok else '✗ РАСХОЖДЕНИЕ!'}"
                )

                shares = self.config['volume'] * self.trade.lot_size
                gross_pnl = (real_data['exit_price'] - real_data['entry_price']) * shares
                net_pnl   = gross_pnl - self.trade.commission

                self.log.info(f"    PnL брутто (API цены):  {gross_pnl:+.2f} руб")
                if self.trade.commission > 0:
                    self.log.info(f"    Комиссия:               -{self.trade.commission:.2f} руб")
                    self.log.info(f"    PnL нетто (реальный):   {net_pnl:+.2f} руб  ← итог")
                if real_data.get('real_pnl') is not None:
                    self.log.info(f"    realizedPnl (биржа):    {float(real_data['real_pnl']):+.2f} руб")

                self._emit_update("pnl_comparison", {
                    "real_entry_price": real_data['entry_price'],
                    "real_exit_price":  real_data['exit_price'],
                    "calculated_entry_price": self.trade.entry_price,
                    "calculated_exit_price":  self.trade.exit_price,
                    "entry_match": entry_ok,
                    "exit_match":  exit_ok,
                    "gross_pnl_rub": gross_pnl,
                    "commission":    self.trade.commission,
                    "net_pnl_rub":   net_pnl,
                    "realized_pnl":  real_data.get('real_pnl'),
                })

            # Отчёт (calculate_result внутри учтёт commission → pnl_net)
            self.generate_report()
            self.logger.banner("ТЕСТИРОВАНИЕ ЗАВЕРШЕНО")


# ════════════════════════════════════════════════════════════════════════════════
# FLASK API SERVER
# ════════════════════════════════════════════════════════════════════════════════

app = Flask(__name__)
app.config['JSON_SORT_KEYS'] = False
CORS(app)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading',
                    logger=False, engineio_logger=False)

tester: Optional[CassandraRealOrderTester] = None
tester_thread: Optional[threading.Thread] = None


def update_callback(event_type: str, data: Dict):
    """Callback для WebSocket обновлений"""
    try:
        socketio.emit('trade_update', {
            "timestamp": datetime.now().isoformat(),
            "event_type": event_type,
            "data": data
        })
    except Exception:
        pass


@app.route('/')
def index():
    return send_from_directory('.', 'dashboard.html')


@app.route('/api/status')
def get_status():
    if tester:
        return jsonify({
            "running": tester_thread is not None and tester_thread.is_alive(),
            "trade": asdict(tester.trade),
            "config": tester.config
        })
    return jsonify({"running": False, "trade": None})


@app.route('/api/start', methods=['POST'])
def start_trade():
    global tester, tester_thread

    config = request.json or {}

    if tester_thread and tester_thread.is_alive():
        return jsonify({"error": "Trade already running"}), 400

    tester = CassandraRealOrderTester(config, update_callback)
    tester_thread = threading.Thread(target=tester.run, daemon=True)
    tester_thread.start()

    return jsonify({"status": "started", "config": tester.config})


@app.route('/api/stop', methods=['POST'])
def stop_trade():
    global tester

    if tester:
        tester.should_stop = True
        return jsonify({"status": "stopping"})

    return jsonify({"error": "No trade running"}), 400


@app.route('/api/config', methods=['GET', 'PUT'])
def config_handler():
    if request.method == 'GET':
        config = tester.config if tester else DEFAULT_CONFIG
        return jsonify(config)

    if tester_thread and tester_thread.is_alive():
        return jsonify({"error": "Cannot change config while trading"}), 400

    return jsonify({"status": "updated", "config": request.json})


@socketio.on('connect')
def handle_connect():
    emit('response', {'data': 'Connected'})


if __name__ == '__main__':
    print("\n" + "=" * 90)
    print("  CASSANDRA REAL ORDER TESTER + API SERVER v5.0 FINAL")
    print("  ПОЛНЫЙ ЦИКЛ: Вход -> Мониторинг -> Выход")
    print("=" * 90)
    print(f"  API Server: http://localhost:{API_PORT}")
    print(f"  Dashboard:  http://localhost:{API_PORT}")
    print("=" * 90)
    print("\n  Сервер запущен!\n")

    def open_browser():
        time.sleep(1.5)
        try:
            webbrowser.open(f'http://localhost:{API_PORT}')
        except Exception:
            pass

    if AUTO_OPEN_BROWSER:
        threading.Thread(target=open_browser, daemon=True).start()

    socketio.run(app,
                 host='localhost',
                 port=API_PORT,
                 debug=False,
                 use_reloader=False,
                 allow_unsafe_werkzeug=True,
                 log_output=False)
