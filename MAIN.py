#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔══════════════════════════════════════════════════════════════════════════════╗
║ CASSANDRA SYSTEM MANAGER v1.0 ║
║ ЕДИНАЯ ТОЧКА ЗАПУСКА ВСЕЙ СИСТЕМЫ ║
╚══════════════════════════════════════════════════════════════════════════════╝
"""
import multiprocessing as mp
import threading
import time
import json
import os
import sys
import signal
import logging
from datetime import datetime
from pathlib import Path

# ─────────────────────────────────────────────────────────────────────────────
# КОНФИГУРАЦИЯ
# ─────────────────────────────────────────────────────────────────────────────
class Config:
    def __init__(self):
        self.data_dir = Path("ai_training_data")
        self.log_dir = Path("logs")
        self.data_poll_interval = 0.5
        self.max_processes = min(os.cpu_count() or 2, 4)
        self.debug = False
        self.data_dir.mkdir(exist_ok=True)
        self.log_dir.mkdir(exist_ok=True)

# ─────────────────────────────────────────────────────────────────────────────
# ЛОГИРОВАНИЕ (будем создавать в каждом процессе отдельно)
# ─────────────────────────────────────────────────────────────────────────────
def setup_logger(name="CassandraSystem", log_dir=None):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    formatter = logging.Formatter(
        '%(asctime)s [%(processName)s] %(levelname)s: %(message)s',
        datefmt='%H:%M:%S'
    )

    # Консоль
    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.INFO)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # Файл (если передан log_dir)
    if log_dir:
        try:
            fh = logging.FileHandler(
                log_dir / f"system_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                encoding='utf-8'
            )
            fh.setLevel(logging.DEBUG)
            fh.setFormatter(formatter)
            logger.addHandler(fh)
        except Exception:
            pass

    return logger


# ─────────────────────────────────────────────────────────────────────────────
# ПРОСТАЯ ФУНКЦИЯ СБОРЩИКА ДАННЫХ (запускается в отдельном процессе)
# ─────────────────────────────────────────────────────────────────────────────
def run_data_collector(config: Config):
    logger = setup_logger("DataCollector", config.log_dir)
    logger.info("Запуск сборщика данных")

    try:
        # Здесь должен быть ваш реальный импорт
        # from cassandra_ai_collector import AIDataCollector
        # collector = AIDataCollector(...)

        # Имитация работы
        logger.info("Имитация сбора данных начата...")
        running = True
        while running:
            # collector.tick() или что-то подобное
            time.sleep(config.data_poll_interval)
            logger.debug("Собраны новые данные...")

    except Exception as e:
        logger.error(f"Критическая ошибка в сборщике: {e}", exc_info=True)
    finally:
        logger.info("Сборщик данных завершил работу")


# ─────────────────────────────────────────────────────────────────────────────
# ПРОСТАЯ ФУНКЦИЯ ТОРГОВОГО ТЕСТЕРА
# ─────────────────────────────────────────────────────────────────────────────
def run_trade_tester(config: Config):
    logger = setup_logger("TradeTester", config.log_dir)
    logger.info("Запуск торгового тестера")

    try:
        # Здесь ваш реальный тестер
        # from cassandra_v6_FIXED import CassandraRealOrderTester
        # tester = CassandraRealOrderTester(...)
        # tester.run()

        # Имитация
        logger.info("Имитация теста стратегии начата...")
        running = True
        i = 0
        while running:
            time.sleep(1.2)
            i += 1
            if i % 5 == 0:
                logger.info(f"Тестовый тик #{i}")

    except Exception as e:
        logger.error(f"Критическая ошибка в тестере: {e}", exc_info=True)
    finally:
        logger.info("Торговый тестер завершил работу")


# ─────────────────────────────────────────────────────────────────────────────
# ЦЕНТРАЛЬНЫЙ МЕНЕДЖЕР
# ─────────────────────────────────────────────────────────────────────────────
class SystemManager:
    def __init__(self):
        self.config = Config()
        self.main_logger = SystemLogger("CassandraSystem", self.config.log_dir)
        self.collector_process: mp.Process | None = None
        self.tester_process: mp.Process | None = None
        self.running = False

    def start(self):
        self.main_logger.banner("ЗАПУСК CASSANDRA SYSTEM MANAGER")
        self.main_logger.info("Оптимизированный запуск для максимальной скорости")

        try:
            self.main_logger.info("Запуск сборщика данных...")
            self.collector_process = mp.Process(
                target=run_data_collector,
                args=(self.config,),
                name="DataCollector"
            )
            self.collector_process.start()

            self.main_logger.info("Запуск торгового тестера...")
            self.tester_process = mp.Process(
                target=run_trade_tester,
                args=(self.config,),
                name="TradeTester"
            )
            self.tester_process.start()

            self.running = True
            self.main_logger.banner("СИСТЕМА ЗАПУЩЕНА УСПЕШНО")

            self._monitor()

        except Exception as e:
            self.main_logger.error(f"Ошибка запуска: {e}", exc_info=True)
            self.stop()

    def _monitor(self):
        while self.running:
            try:
                if self.collector_process and not self.collector_process.is_alive():
                    self.main_logger.warning("Сборщик упал → перезапуск...")
                    self.collector_process = mp.Process(
                        target=run_data_collector,
                        args=(self.config,),
                        name="DataCollector"
                    )
                    self.collector_process.start()

                if self.tester_process and not self.tester_process.is_alive():
                    self.main_logger.warning("Тестер упал → перезапуск...")
                    self.tester_process = mp.Process(
                        target=run_trade_tester,
                        args=(self.config,),
                        name="TradeTester"
                    )
                    self.tester_process.start()

                time.sleep(4)
            except Exception as e:
                self.main_logger.error(f"Ошибка в мониторинге: {e}")
                time.sleep(5)

    def stop(self):
        self.main_logger.banner("ОСТАНОВКА СИСТЕМЫ")
        self.running = False

        if self.collector_process and self.collector_process.is_alive():
            self.main_logger.info("Остановка сборщика...")
            self.collector_process.terminate()
            self.collector_process.join(timeout=6)

        if self.tester_process and self.tester_process.is_alive():
            self.main_logger.info("Остановка тестера...")
            self.tester_process.terminate()
            self.tester_process.join(timeout=6)

        self.main_logger.banner("СИСТЕМА ОСТАНОВЛЕНА")

    def banner(self, text):
        line = "=" * 80
        self.main_logger.info(line)
        self.main_logger.info(f" {text} ".center(80))
        self.main_logger.info(line)

class SystemLogger:
    def __init__(self, name="CassandraSystem", log_dir=None):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.INFO)
        self.logger.propagate = False

        formatter = logging.Formatter(
            '%(asctime)s [%(processName)s] %(levelname)s: %(message)s',
            datefmt='%H:%M:%S'
        )

        ch = logging.StreamHandler(sys.stdout)
        ch.setLevel(logging.INFO)
        ch.setFormatter(formatter)
        self.logger.addHandler(ch)

        if log_dir:
            try:
                fh = logging.FileHandler(
                    log_dir / f"system_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log",
                    encoding='utf-8'
                )
                fh.setLevel(logging.DEBUG)
                fh.setFormatter(formatter)
                self.logger.addHandler(fh)
            except Exception:
                pass

    def banner(self, text: str):
        line = "=" * 80
        self.logger.info(line)
        self.logger.info(f" {text} ".center(80))
        self.logger.info(line)

    def info(self, msg):
        self.logger.info(msg)

    def warning(self, msg):
        self.logger.warning(msg)

    def error(self, msg, exc_info=False):
        self.logger.error(msg, exc_info=exc_info)
# ─────────────────────────────────────────────────────────────────────────────
# ГЛАВНЫЙ ВХОД
# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    mp.set_start_method("spawn")  # явно указываем (на Windows это и так по умолчанию)

    manager = SystemManager()

    def signal_handler(sig, frame):
        manager.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    manager.start()

    # Чтобы главный процесс не завершился сразу
    try:
        while manager.running:
            time.sleep(1)
    except KeyboardInterrupt:
        manager.stop()
