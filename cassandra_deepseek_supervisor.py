#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Cassandra DeepSeek Supervisor (OpenRouter).

CLI-утилита для генерации команд роботу-исполнителю:
- принимает текстовую инструкцию трейдера
- обращается к OpenRouter (DeepSeek)
- нормализует ответ в JSON-команду для `/api/executor/command`
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass, asdict
from typing import Dict, Any


OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"
MODEL = "deepseek/deepseek-chat-v3-0324:free"


@dataclass
class ExecutorCommand:
    action: str = "HOLD"
    ticker: str = "SBER"
    side: str = "buy"
    volume: int = 1
    order_type: str = "market"
    tp_percent: float = 0.0
    sl_percent: float = 0.0


def parse_command(text: str, default_ticker: str = "SBER") -> ExecutorCommand:
    t = (text or "").upper()
    cmd = ExecutorCommand(ticker=default_ticker)

    if "BUY" in t or "ЛОНГ" in t:
        cmd.action, cmd.side = "OPEN", "buy"
    elif "SELL" in t or "ШОРТ" in t:
        cmd.action, cmd.side = "OPEN", "sell"

    m = re.search(r"(?:VOLUME|VOL|ОБЪ[ЕЁ]М)\s*[:=]?\s*(\d+)", t)
    if m:
        cmd.volume = max(1, int(m.group(1)))

    tpm = re.search(r"TP\s*[:=]?\s*(\d+(?:[\.,]\d+)?)\s*%", t)
    slm = re.search(r"SL\s*[:=]?\s*(\d+(?:[\.,]\d+)?)\s*%", t)
    if tpm:
        cmd.tp_percent = float(tpm.group(1).replace(",", "."))
    if slm:
        cmd.sl_percent = float(slm.group(1).replace(",", "."))

    tm = re.search(r"(?:TICKER|ИНСТРУМЕНТ|СИМВОЛ)\s*[:=]?\s*([A-Z0-9]+)", t)
    if tm:
        cmd.ticker = tm.group(1)

    return cmd


def call_openrouter(message: str) -> str:
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        return "OPENROUTER_API_KEY не задан, используется локальный парсер команд."

    payload = {
        "model": MODEL,
        "messages": [
            {"role": "system", "content": "Ты AI-куратор робота Cassandra. Предлагай конкретные команды для исполнения."},
            {"role": "user", "content": message},
        ],
    }
    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://localhost",
        "X-Title": "cassandra-deepseek-supervisor",
    }

    import requests
    r = requests.post(OPENROUTER_URL, json=payload, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    return data.get("choices", [{}])[0].get("message", {}).get("content", "")


def build_response(user_message: str, default_ticker: str) -> Dict[str, Any]:
    ai_reply = call_openrouter(user_message)
    command = parse_command(user_message + "\n" + ai_reply, default_ticker)
    return {
        "reply": ai_reply,
        "command": asdict(command),
    }


def main() -> None:
    p = argparse.ArgumentParser(description="DeepSeek supervisor for Cassandra executor")
    p.add_argument("message", help="Инструкция трейдера")
    p.add_argument("--ticker", default="SBER", help="Тикер по умолчанию")
    args = p.parse_args()

    result = build_response(args.message, args.ticker)
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
