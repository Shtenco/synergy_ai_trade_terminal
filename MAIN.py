#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CASSANDRA SYSTEM MANAGER v2.0
Единая точка запуска компонентов Cassandra в process-supervisor стиле.
"""

from __future__ import annotations

import json
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Callable, Dict, Optional


# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class Config:
    data_dir: Path = Path("ai_training_data")
    log_dir: Path = Path("logs")
    data_poll_interval: float = 0.5
    max_processes: int = field(default_factory=lambda: min(os.cpu_count() or 2, 4))
    debug: bool = False

    @classmethod
    def load(cls, path: str = "config.json") -> "Config":
        config = cls()
        cfg_path = Path(path)

        if cfg_path.exists():
            with cfg_path.open("r", encoding="utf-8") as fh:
                payload = json.load(fh)
            config.data_dir = Path(payload.get("data_dir", config.data_dir))
            config.log_dir = Path(payload.get("log_dir", config.log_dir))
            config.data_poll_interval = float(payload.get("data_poll_interval", config.data_poll_interval))
            config.max_processes = int(payload.get("max_processes", config.max_processes))
            config.debug = bool(payload.get("debug", config.debug))

        config.data_dir.mkdir(exist_ok=True)
        config.log_dir.mkdir(exist_ok=True)
        return config


# ─────────────────────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────────────────────


def setup_logger(name: str, log_dir: Optional[Path] = None, debug: bool = False) -> logging.Logger:
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    logger.setLevel(logging.DEBUG if debug else logging.INFO)
    logger.propagate = False
    formatter = logging.Formatter(
        "%(asctime)s [%(processName)s] %(levelname)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    console = logging.StreamHandler(sys.stdout)
    console.setLevel(logging.DEBUG if debug else logging.INFO)
    console.setFormatter(formatter)
    logger.addHandler(console)

    if log_dir:
        file_handler = logging.FileHandler(
            log_dir / f"system_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger


# ─────────────────────────────────────────────────────────────────────────────
# WORKERS
# ─────────────────────────────────────────────────────────────────────────────


def run_data_collector(config: Config) -> None:
    logger = setup_logger("DataCollector", config.log_dir, config.debug)
    logger.info("Запуск сборщика данных")
    try:
        from cassandra_ai_collector import main as collector_main  # type: ignore

        collector_main()
    except Exception:
        logger.warning("Не удалось запустить реальный collector, переключение на safe-loop", exc_info=config.debug)
        while True:
            time.sleep(config.data_poll_interval)
            logger.debug("collector heartbeat")


def run_trade_tester(config: Config) -> None:
    logger = setup_logger("TradeTester", config.log_dir, config.debug)
    logger.info("Запуск торгового тестера")
    try:
        import runpy

        runpy.run_path("cassandra_v6_FIXED.py", run_name="__main__")
    except Exception:
        logger.warning("Не удалось запустить реальный tester, переключение на safe-loop", exc_info=config.debug)
        while True:
            time.sleep(1.0)
            logger.debug("tester heartbeat")


# ─────────────────────────────────────────────────────────────────────────────
# PROCESS SUPERVISOR
# ─────────────────────────────────────────────────────────────────────────────


@dataclass
class ManagedComponent:
    name: str
    target: Callable[[Config], None]
    process: Optional[mp.Process] = None

    def start(self, config: Config) -> None:
        self.process = mp.Process(target=self.target, args=(config,), name=self.name)
        self.process.start()

    def is_alive(self) -> bool:
        return bool(self.process and self.process.is_alive())

    def stop(self, timeout: float = 6.0) -> None:
        if not self.process:
            return
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(timeout=timeout)
        if self.process.is_alive():
            self.process.kill()
            self.process.join(timeout=2)


class SystemManager:
    def __init__(self) -> None:
        self.config = Config.load()
        self.log = setup_logger("CassandraSystem", self.config.log_dir, self.config.debug)
        self.running = False
        self.components: Dict[str, ManagedComponent] = {
            "DataCollector": ManagedComponent("DataCollector", run_data_collector),
            "TradeTester": ManagedComponent("TradeTester", run_trade_tester),
        }

    def start(self) -> None:
        self._banner("ЗАПУСК CASSANDRA SYSTEM MANAGER")
        self.running = True
        for component in self.components.values():
            self.log.info(f"Запуск {component.name}...")
            component.start(self.config)
        self._monitor()

    def _monitor(self) -> None:
        while self.running:
            for component in self.components.values():
                if not component.is_alive():
                    self.log.warning(f"{component.name} упал → перезапуск")
                    component.start(self.config)
            time.sleep(3)

    def stop(self) -> None:
        self.running = False
        self._banner("ОСТАНОВКА CASSANDRA SYSTEM MANAGER")
        for component in self.components.values():
            self.log.info(f"Остановка {component.name}...")
            component.stop()

    def _banner(self, text: str) -> None:
        line = "=" * 80
        self.log.info(line)
        self.log.info(f" {text} ".center(80))
        self.log.info(line)


def main() -> None:
    mp.set_start_method("spawn", force=True)
    manager = SystemManager()

    def signal_handler(_sig, _frame):
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        manager.start()
    except KeyboardInterrupt:
        manager.stop()


if __name__ == "__main__":
    main()
