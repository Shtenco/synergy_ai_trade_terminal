"""
LLM-trader-mvp: Торговый бот на основе анализа LLM.

Этот скрипт подключается к терминалу MetaTrader 5, получает рыночные данные,
анализирует их с помощью большой языковой модели (LLM) через OpenRouter
и выполняет торговые операции на основе полученных сигналов.

Автор: Oleg Doykov <baxter@tut.by>
Версия: 1.0.0
"""

# Импорт необходимых библиотек
import MetaTrader5 as mt5  # Для интеграции с MT5
import pandas as pd  # Для обработки данных OHLCV
import numpy as np  # Для математических вычислений
import re  # Для regex-парсинга ответа LLM
from openai import OpenAI  # Клиент для API OpenAI через OpenRouter
import logging  # Для логирования
import time  # Для задержек в реал-тайм мониторинге
import matplotlib.pyplot as plt  # Опционально для визуализации
from datetime import datetime  # Для работы с датами
from dotenv import load_dotenv  # Для загрузки .env
import os  # Для доступа к переменным окружения

# Загрузка .env
load_dotenv()

# Настройка логирования с UTF-8
logging.basicConfig(filename='LLM_trader.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s', encoding='utf-8')

# Функция для подключения к MT5
def connect_to_mt5():
    try:
        if not mt5.initialize():
            logging.error("MT5 initialize() failed")
            raise Exception("Failed to initialize MT5")
        # Если нужно, добавьте mt5.login(login=your_login, password=your_password, server=your_server)
        # Для демо-аккаунта это может не требоваться, но проверьте в терминале
        logging.info("Successfully connected to MT5")
    except Exception as e:
        logging.error(f"Connection error: {e}")
        raise

# Функция для получения OHLCV данных
def get_ohlcv(symbol, timeframe, count=100):
    try:
        rates = mt5.copy_rates_from_pos(symbol, timeframe, 0, count)
        if rates is None:
            logging.error("Failed to get OHLCV data")
            return None
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')  # Конвертируем время в datetime
        logging.info(f"Retrieved {len(df)} bars for {symbol}")
        return df[['time', 'open', 'high', 'low', 'close', 'tick_volume']]  # Используем tick_volume как объем
    except Exception as e:
        logging.error(f"Error getting OHLCV: {e}")
        return None

# Функция для анализа данных с помощью OpenRouter LLM
def analyze_with_llm(api_key, ohlcv_df):
    try:
        client = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=api_key
        )
        
        # Формируем промпт на основе стратегии (без строгих ограничений на формат, чтобы позволить рассуждения)
        prompt = """
        Ты - внутридневной трейдер, торгуешь и на основе уровней поддержки и сопротивления и паттернов Price Action.
        Анализируй предоставленные OHLCV данные (time, open, high, low, close, volume).
        Шаги анализа:
        1. Определи ключевые уровни поддержки и сопротивления (локальные max/min за последние 50 баров).
        2. На уоровнях ищи свечные паттерны Price Action.
        3. Если обнаружен bearish signal, рекомендуй SELL.
           Если обнаружен bullish signal, рекомендуй BUY.
           Иначе - HOLD.
        4. Для сигнала укажи предполагаемый SL (за экстремум) и TP (1:2 риск/прибыль от ENTRY_PRICE).
        
        Данные в CSV формате:
        {}
        
        Ответ в формате:
        ACTION: BUY/SELL/HOLD
        ENTRY_PRICE: цена входа
        SL: стоп-лосс
        TP: тейк-профит
        REASON: краткое объяснение
        """.format(ohlcv_df.to_csv(index=False))  # Преобразуем DF в CSV для промпта
        
        completion = client.chat.completions.create(
            extra_headers={
                "HTTP-Referer": "https://www.mql5.com/ru/users/baxter_by", # Optional. Site URL for rankings on openrouter.ai.
                "X-Title": "LLM-trader-mvp", # Optional. Site title for rankings on openrouter.ai.
            },
            extra_body={},
            # model="google/gemma-3-27b-it:free",
            model="tngtech/deepseek-r1t2-chimera:free",
            # model="tngtech/deepseek-r1t-chimera:free",
            # model="deepseek/deepseek-chat-v3-0324:free",
            messages=[
                {
                    "role": "user",
                    "content": prompt
                }
            ]
        )
        
        result = completion.choices[0].message.content.strip()
        logging.info(f"LLM response: {result}")
        
        # Robust парсинг с regex (ищет ключи независимо от порядка и лишнего текста)
        action_match = re.search(r'ACTION:\s*(BUY|SELL|HOLD)', result, re.IGNORECASE)
        entry_match = re.search(r'ENTRY_PRICE:\s*([\d.]+)', result, re.IGNORECASE)
        sl_match = re.search(r'SL:\s*([\d.]+)', result, re.IGNORECASE)
        tp_match = re.search(r'TP:\s*([\d.]+)', result, re.IGNORECASE)
        reason_match = re.search(r'REASON:\s*(.*)', result, re.IGNORECASE)
        
        action = action_match.group(1).upper() if action_match else 'HOLD'
        entry = float(entry_match.group(1)) if entry_match else 0.0
        sl = float(sl_match.group(1)) if sl_match else 0.0
        tp = float(tp_match.group(1)) if tp_match else 0.0
        reason = reason_match.group(1).strip() if reason_match else 'No reason provided'
        
        # Валидация: если action BUY/SELL, но цены нулевые - fallback на HOLD
        if action in ['BUY', 'SELL'] and (entry == 0 or sl == 0 or tp == 0):
            action = 'HOLD'
            reason = 'Invalid signal parameters'
        
        logging.info(f"Parsed signal: {action}, Entry: {entry}, SL: {sl}, TP: {tp}, Reason: {reason}")
        
        return {'action': action, 'entry': entry, 'sl': sl, 'tp': tp, 'reason': reason}
    except Exception as e:
        logging.error(f"LLM analysis error: {e}")
        return {'action': 'HOLD', 'reason': 'Analysis failed'}

# Функция для получения информации о символе и проверка его доступности
def get_symbol_info(symbol):
    """Получение информации о символе и проверка его доступности"""
    symbol_info = mt5.symbol_info(symbol)
    if symbol_info is None:
        logging.error(f"{symbol} not found")
        return None

    # Если символ не активен в Market Watch, добавляем его
    if not symbol_info.visible:
        if not mt5.symbol_select(symbol, True):
            logging.error(f"symbol_select({symbol}) failed, error code: {mt5.last_error()}")
            return None

    return symbol_info

# Функция для размещения ордера
def place_order(symbol, action, volume, entry, sl, tp, deviation=20, magic=234000, comment="LLM-trader"):
    """
    Размещение торгового ордера с улучшенной обработкой ошибок и актуальными ценами
    
    Args:
        symbol: Торговый инструмент
        action: Тип действия ('BUY' или 'SELL')
        volume: Объем лота
        entry: Цена входа
        sl: Стоп-лосс
        tp: Тейк-профит
        deviation: Максимальное отклонение цены
        magic: "Магический" номер для идентификации ордера
        comment: Комментарий к ордеру
    """
    print('place_order')

    # Получаем актуальные данные по символу
    symbol_info = get_symbol_info(symbol)
    if symbol_info is None:
        return False

    # Получаем текущую цену непосредственно перед отправкой ордера
    tick = mt5.symbol_info_tick(symbol)
    if tick is None:
        logging.error(f"Failed to get tick data for {symbol}")
        return False

    # Устанавливаем цену в зависимости от типа ордера
    order_type = mt5.ORDER_TYPE_BUY if action == 'BUY' else mt5.ORDER_TYPE_SELL
    if order_type == mt5.ORDER_TYPE_BUY:
        price = tick.ask
        # Для ордера на покупку корректируем SL и TP если они заданы
        sl_price = sl if sl > 0 else 0.0
        tp_price = tp if tp > 0 else 0.0
    elif order_type == mt5.ORDER_TYPE_SELL:
        price = tick.bid
        # Для ордера на продажу корректируем SL и TP если они заданы
        sl_price = sl if sl > 0 else 0.0
        tp_price = tp if tp > 0 else 0.0
    else:
        logging.error(f"Unsupported order type: {action}")
        return False

    # Подготавливаем структуру запроса
    request = {
        "action": mt5.TRADE_ACTION_DEAL,  # Тип операции: немедленное исполнение (рыночный)
        "symbol": symbol,
        "volume": float(volume),
        "type": order_type,
        "price": price,  # Используем актуальную цену
        "sl": sl_price,  # Цена стоп-лосса
        "tp": tp_price,  # Цена тейк-профита
        "deviation": deviation,  # Допустимое отклонение цены в пунктах
        "magic": magic,
        "comment": comment,
        "type_time": mt5.ORDER_TIME_GTC,  # Время действия ордера: Good till cancel
        "type_filling": mt5.ORDER_FILLING_FOK,  # Тип исполнения ордера
    }

    # Отправляем торговый запрос
    result = mt5.order_send(request)

    # Проверяем результат исполнения
    if result.retcode != mt5.TRADE_RETCODE_DONE:
        logging.error(f"Order failed, retcode={result.retcode}, comment={result.comment}")
        # Выводим всю информацию из результата для отладки
        result_dict = result._asdict()
        for field in result_dict.keys():
            logging.error(f"   {field}={result_dict[field]}")
        return False
    else:
        logging.info(f"Order placed successfully: {action} {volume} lots at {price}, SL: {sl_price}, TP: {tp_price}, Ticket: {result.order}")
        # Выводим информацию о результате в виде словаря
        result_dict = result._asdict()
        for field in result_dict.keys():
            logging.info(f"   {field}={result_dict[field]}")
        return True

# Опциональная функция для визуализации
def visualize_chart(ohlcv_df, signal=None):
    try:
        plt.figure(figsize=(12, 6))
        plt.plot(ohlcv_df['time'], ohlcv_df['close'], label='Close')
        plt.title('OHLCV Chart')
        if signal and signal['action'] != 'HOLD':
            plt.axhline(y=signal['entry'], color='g', linestyle='--', label='Entry')
            plt.axhline(y=signal['sl'], color='r', linestyle='--', label='SL')
            plt.axhline(y=signal['tp'], color='b', linestyle='--', label='TP')
        plt.legend()
        plt.savefig('chart.png')  # Сохраняем в файл
        logging.info("Chart visualized and saved to chart.png")
    except Exception as e:
        logging.error(f"Visualization error: {e}")

# Основная функция мониторинга
def main_loop(symbol='EURUSD', timeframe=mt5.TIMEFRAME_M5, volume=0.01, api_key='', interval=60, visualize=False):
    connect_to_mt5()
    try:
        while True:
            print('main_loop')
            ohlcv = get_ohlcv(symbol, timeframe)
            if ohlcv is not None:
                signal = analyze_with_llm(api_key, ohlcv)
                if signal['action'] != 'HOLD':
                    place_order(symbol, signal['action'], volume, signal['entry'], signal['sl'], signal['tp'], deviation=20, magic=5115, comment="LLM Trader")
                    if visualize:
                        visualize_chart(ohlcv, signal)
                else:
                    logging.info("No signal: HOLD")
            time.sleep(interval)  # Задержка в секундах (e.g., 60 для минуты)
    except KeyboardInterrupt:
        logging.info("Script stopped by user")
    except Exception as e:
        logging.error(f"Main loop error: {e}")
    finally:
        mt5.shutdown()
        logging.info("Disconnected from MT5")

# Запуск скрипта
if __name__ == "__main__":
    print('The script is running. To stop, press Ctrl+C.')
    # Настройте параметры здесь
    SYMBOL = "EURUSD"  # Символ для торговли
    TIMEFRAME = mt5.TIMEFRAME_M5  # Таймфрейм (M5 для внутридневной торговли)
    VOLUME = 0.10  # Объем лота (малый для теста)
    OPENROUTER_API_KEY = os.getenv('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')  # Загружаем из .env
    INTERVAL = 300  # Интервал проверки в секундах (5 минут для M5)
    VISUALIZE = True  # Включить визуализацию (True/False)
    
    if not OPENROUTER_API_KEY:
        logging.error("OPENROUTER_API_KEY not found in .env")
        raise ValueError("API key missing")
    
    main_loop(SYMBOL, TIMEFRAME, VOLUME, OPENROUTER_API_KEY, INTERVAL, VISUALIZE)
