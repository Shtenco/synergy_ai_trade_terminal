"""
╔═══════════════════════════════════════════════════════════════════════════════╗
║ ║
║ 🔬 QUANTUM MIDAS - 2-YEAR WALK-FORWARD BACKTEST ║
║ ║
║ КРИТИЧЕСКИЕ ОСОБЕННОСТИ: ║
║ ✅ 2 ГОДА тестирования (730 дней) ║
║ ✅ Walk-Forward Analysis (честный бэктест) ║
║ ✅ Обучение ТОЛЬКО на прошлых данных ║
║ ✅ Переобучение моделей каждые 3 дня ║
║ ✅ Реалистичное исполнение (спред, slippage, комиссии) ║
║ ✅ NO LOOK-AHEAD BIAS - модели не видят будущее! ║
║ ✅ КУЧА КРАСИВЫХ ГРАФИКОВ - каждый отдельно! ║
║ ✅ Зелёно-чёрная схема оформления ║
║ ║
║ Автор: Евгений Коштенко + Claude ║
║ Дата: 30 января 2026 ║
║ ║
╚═══════════════════════════════════════════════════════════════════════════════╝
"""
import os
import sys
import time
import json
import pickle
import logging
import warnings
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass, field
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
# MT5
import MetaTrader5 as mt5
# ML
from catboost import CatBoostClassifier, CatBoostRegressor
from sklearn.preprocessing import StandardScaler
from sklearn.cluster import KMeans
from sklearn.metrics import accuracy_score, precision_score, recall_score, f1_score
# Quantum (опционально)
try:
    from qiskit import QuantumCircuit
    from qiskit_aer import AerSimulator
    QISKIT_AVAILABLE = True
except:
    QISKIT_AVAILABLE = False
warnings.filterwarnings('ignore')
# ═══════════════════════════════════════════════════════════════════════════════
# КОНФИГУРАЦИЯ
# ═══════════════════════════════════════════════════════════════════════════════
os.makedirs("backtest_results", exist_ok=True)
os.makedirs("backtest_models", exist_ok=True)
os.makedirs("backtest_logs", exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('backtest_logs/walk_forward_2years.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("WalkForward2Years")
# Параметры бэктеста
BACKTEST_CONFIG = {
    'initial_balance': 10000.0,
    'symbols': ["EURUSD", "GBPUSD", "USDCHF", "USDCAD", "AUDUSD", "NZDUSD", "EURGBP"],
    'timeframe': mt5.TIMEFRAME_H1,
    'test_days': 2600, # 2 ГОДА!
    'initial_training_days': 1000, # Начальное обучение на году
    'retrain_every_days': 3, # Переобучение каждые 3 дня
    'min_training_samples': 800,
   
    # Реалистичное исполнение
    'spread_pips': {
        'EURUSD': 0.8, 'GBPUSD': 1.2, 'USDCHF': 1.5,
        'USDCAD': 1.8, 'AUDUSD': 1.0, 'NZDUSD': 2.0, 'EURGBP': 1.5
    },
    'slippage_pips': 0.5,
    'commission_per_lot': 4.0,
   
    # Risk management
    'max_risk_per_trade_pct': 1.5,
    'stop_loss_pips': 40,
    'max_positions': 6,
    'daily_loss_limit_pct': 4.0,
    'weekly_loss_limit_pct': 9.0,
}
TERMINAL_PATH = "C:/Program Files/RoboForex MT5 Terminal/terminal64.exe"
# ═══════════════════════════════════════════════════════════════════════════════
# ТЕХНИЧЕСКИЕ ИНДИКАТОРЫ
# ═══════════════════════════════════════════════════════════════════════════════
def calculate_rsi(prices, period=14):
    """RSI calculation"""
    deltas = np.diff(prices)
    seed = deltas[:period]
    up = seed[seed >= 0].sum() / period
    down = -seed[seed < 0].sum() / period
    rs = up / down if down != 0 else 0
    rsi = np.zeros_like(prices)
    rsi[:period] = 100. - 100. / (1. + rs)
    for i in range(period, len(prices)):
        delta = deltas[i-1]
        up_val = delta if delta > 0 else 0
        down_val = -delta if delta < 0 else 0
        up = (up * (period - 1) + up_val) / period
        down = (down * (period - 1) + down_val) / period
        rs = up / down if down != 0 else 0
        rsi[i] = 100. - 100. / (1. + rs)
    return rsi
def calculate_ma(prices, period):
    """Moving Average"""
    return pd.Series(prices).rolling(window=period, min_periods=1).mean().values
def calculate_ema(prices, period):
    """EMA"""
    return pd.Series(prices).ewm(span=period, adjust=False).mean().values
def calculate_bollinger_bands(prices, period=20, std_dev=2):
    """Bollinger Bands"""
    ma = calculate_ma(prices, period)
    std = pd.Series(prices).rolling(window=period, min_periods=1).std().values
    upper = ma + std_dev * std
    lower = ma - std_dev * std
    return upper, ma, lower
def calculate_macd(prices, fast=12, slow=26, signal=9):
    """MACD"""
    ema_fast = calculate_ema(prices, fast)
    ema_slow = calculate_ema(prices, slow)
    macd_line = ema_fast - ema_slow
    signal_line = calculate_ema(macd_line, signal)
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram
def calculate_atr(high, low, close, period=14):
    """Average True Range"""
    tr1 = high - low
    tr2 = np.abs(high - np.roll(close, 1))
    tr3 = np.abs(low - np.roll(close, 1))
    tr = np.maximum(tr1, np.maximum(tr2, tr3))
    atr = pd.Series(tr).rolling(window=period, min_periods=1).mean().values
    return atr
# ═══════════════════════════════════════════════════════════════════════════════
# ЗАГРУЗКА ДАННЫХ (БЕЗ LOOK-AHEAD! БЕЗ СПАМА!)
# ═══════════════════════════════════════════════════════════════════════════════
def get_historical_data(symbol: str, start_date: datetime, end_date: datetime) -> pd.DataFrame:
    """
    Загружает исторические данные СТРОГО до end_date
    КРИТИЧНО: Не загружает данные из будущего!
    БЕЗ СПАМА: Фильтрация через DEBUG
    """
    try:
        rates = mt5.copy_rates_range(
            symbol,
            BACKTEST_CONFIG['timeframe'],
            start_date,
            end_date
        )
       
        if rates is None or len(rates) == 0:
            return None
       
        df = pd.DataFrame(rates)
        df['time'] = pd.to_datetime(df['time'], unit='s')
       
        # ТИХО фильтруем future data (DEBUG уровень)
        future_data = df[df['time'] > end_date]
        if len(future_data) > 0:
            df = df[df['time'] <= end_date]
            logger.debug(f"Filtered {len(future_data)} future bars from {symbol}")
       
        logger.debug(f"Loaded {len(df)} bars for {symbol}")
        return df
       
    except Exception as e:
        logger.error(f"Error loading data for {symbol}: {e}")
        return None
def prepare_features(df: pd.DataFrame) -> pd.DataFrame:
    """Создаёт фичи из OHLCV данных"""
    if df is None or len(df) < 50:
        return None
   
    df = df.copy()
   
    # Технические индикаторы
    df['RSI'] = calculate_rsi(df['close'].values)
    df['MA_20'] = calculate_ma(df['close'].values, 20)
    df['MA_50'] = calculate_ma(df['close'].values, 50)
    df['EMA_12'] = calculate_ema(df['close'].values, 12)
    df['EMA_26'] = calculate_ema(df['close'].values, 26)
   
    bb_upper, bb_middle, bb_lower = calculate_bollinger_bands(df['close'].values)
    df['BB_upper'] = bb_upper
    df['BB_middle'] = bb_middle
    df['BB_lower'] = bb_lower
   
    macd, macd_signal, macd_hist = calculate_macd(df['close'].values)
    df['MACD'] = macd
    df['MACD_signal'] = macd_signal
    df['MACD_hist'] = macd_hist
   
    df['ATR'] = calculate_atr(df['high'].values, df['low'].values, df['close'].values)
   
    # Price features
    df['returns'] = df['close'].pct_change()
    df['log_returns'] = np.log(df['close'] / df['close'].shift(1))
    df['volatility'] = df['returns'].rolling(20).std()
   
    # Volume
    df['volume_ma'] = df['tick_volume'].rolling(20).mean()
    df['volume_ratio'] = df['tick_volume'] / (df['volume_ma'] + 1e-8)
   
    # Price position
    df['high_low_ratio'] = (df['high'] - df['low']) / (df['close'] + 1e-8)
    df['close_position'] = (df['close'] - df['low']) / (df['high'] - df['low'] + 1e-8)
   
    # Momentum
    df['momentum_5'] = df['close'] / df['close'].shift(5) - 1
    df['momentum_10'] = df['close'] / df['close'].shift(10) - 1
    df['momentum_20'] = df['close'] / df['close'].shift(20) - 1
   
    # Remove NaN
    df = df.dropna()
   
    return df
def create_labels(df: pd.DataFrame, lookahead_bars: int = 10) -> pd.DataFrame:
    """Создаёт лейблы для обучения"""
    df = df.copy()
   
    df['future_return'] = df['close'].shift(-lookahead_bars) / df['close'] - 1
   
    threshold = 0.001
    df['label'] = 0 # HOLD
    df.loc[df['future_return'] > threshold, 'label'] = 1 # BUY
    df.loc[df['future_return'] < -threshold, 'label'] = 2 # SELL
   
    df = df.iloc[:-lookahead_bars]
   
    return df
# ═══════════════════════════════════════════════════════════════════════════════
# МОДЕЛИ (ONLINE LEARNING)
# ═══════════════════════════════════════════════════════════════════════════════
class OnlineMLModel:
    """Модель с онлайн-обучением"""
   
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.model = None
        self.scaler = StandardScaler()
        self.feature_columns = None
        self.last_train_date = None
        self.training_history = []
       
    def train(self, train_df: pd.DataFrame):
        """Обучение модели"""
        if train_df is None or len(train_df) < BACKTEST_CONFIG['min_training_samples']:
            return False
       
        try:
            feature_cols = [
                'RSI', 'MA_20', 'MA_50', 'EMA_12', 'EMA_26',
                'BB_upper', 'BB_middle', 'BB_lower',
                'MACD', 'MACD_signal', 'MACD_hist', 'ATR',
                'returns', 'log_returns', 'volatility',
                'volume_ratio', 'high_low_ratio', 'close_position',
                'momentum_5', 'momentum_10', 'momentum_20'
            ]
           
            missing_cols = [col for col in feature_cols if col not in train_df.columns]
            if missing_cols:
                return False
           
            train_df = create_labels(train_df.copy())
           
            if len(train_df) < BACKTEST_CONFIG['min_training_samples']:
                return False
           
            X = train_df[feature_cols].values
            y = train_df['label'].values
           
            X_scaled = self.scaler.fit_transform(X)
           
            self.model = CatBoostClassifier(
                iterations=200,
                depth=6,
                learning_rate=0.1,
                loss_function='MultiClass',
                verbose=False,
                random_state=42
            )
           
            self.model.fit(X_scaled, y)
            self.feature_columns = list(feature_cols)
            self.last_train_date = train_df['time'].max()
           
            train_preds = self.model.predict(X_scaled)
            accuracy = accuracy_score(y, train_preds)
           
            self.training_history.append({
                'date': self.last_train_date,
                'samples': len(train_df),
                'accuracy': accuracy
            })
           
            logger.info(f"{self.symbol}: Trained on {len(train_df)} samples, accuracy: {accuracy:.3f}")
            return True
           
        except Exception as e:
            logger.error(f"{self.symbol}: Training error: {e}")
            return False
   
    def predict(self, current_df: pd.DataFrame) -> Dict:
        """Предсказание"""
        if self.model is None:
            return {'signal': 'HOLD', 'confidence': 0.0}
       
        try:
            if not all(col in current_df.columns for col in self.feature_columns):
                return {'signal': 'HOLD', 'confidence': 0.0}
           
            last_row = current_df.iloc[-1]
           
            feature_values = []
            for col in self.feature_columns:
                feature_values.append(last_row[col])
           
            X = np.array(feature_values).reshape(1, -1)
            X_scaled = self.scaler.transform(X)
           
            pred_class = self.model.predict(X_scaled)[0]
            pred_proba = self.model.predict_proba(X_scaled)[0]
           
            signal_map = {0: 'HOLD', 1: 'BUY', 2: 'SELL'}
            signal = signal_map[int(pred_class)]
            confidence = pred_proba[int(pred_class)]
           
            return {
                'signal': signal,
                'confidence': float(confidence)
            }
           
        except:
            return {'signal': 'HOLD', 'confidence': 0.0}
# ═══════════════════════════════════════════════════════════════════════════════
# ТОРГОВАЯ ЛОГИКА
# ═══════════════════════════════════════════════════════════════════════════════
@dataclass
class Position:
    """Открытая позиция"""
    symbol: str
    direction: str
    entry_price: float
    entry_time: datetime
    lot: float
    stop_loss: float
    take_profit: Optional[float] = None
@dataclass
class Trade:
    """Закрытая сделка"""
    symbol: str
    direction: str
    entry_price: float
    exit_price: float
    entry_time: datetime
    exit_time: datetime
    lot: float
    pips: float
    pnl: float
    commission: float
    slippage: float
    result: str
class WalkForwardBacktest:
    """Walk-Forward бэктест с онлайн-обучением"""
   
    def __init__(self):
        self.balance = BACKTEST_CONFIG['initial_balance']
        self.initial_balance = BACKTEST_CONFIG['initial_balance']
        self.equity_history = []
        self.balance_history = []
        self.dates = []
        self.positions: Dict[str, Position] = {}
        self.trades: List[Trade] = []
        self.models: Dict[str, OnlineMLModel] = {}
       
        # Для графиков
        self.training_dates = []
        self.testing_dates = []
       
        self.daily_pnl = 0.0
        self.weekly_pnl = 0.0
        self.current_day = None
        self.current_week = None
        self.trading_blocked = False
       
        self.total_trades = 0
        self.winning_trades = 0
        self.losing_trades = 0
       
    def initialize_mt5(self):
        """Инициализация MT5"""
        if not mt5.initialize(path=TERMINAL_PATH):
            logger.error("MT5 initialization failed")
            return False
        logger.info("MT5 initialized successfully")
        return True
   
    def calculate_lot_size(self, symbol: str, stop_loss_pips: float) -> float:
        """Расчёт размера лота"""
        risk_amount = self.balance * (BACKTEST_CONFIG['max_risk_per_trade_pct'] / 100)
        pip_value = 10.0
        max_lot = risk_amount / (stop_loss_pips * pip_value)
        return max(0.01, min(2.0, round(max_lot, 2)))
   
    def calculate_realistic_pnl(self, symbol: str, direction: str, entry_price: float,
                               exit_price: float, lot: float) -> Tuple[float, float, float]:
        """Расчёт РЕАЛИСТИЧНОГО PnL"""
        spread_pips = BACKTEST_CONFIG['spread_pips'].get(symbol, 1.0)
        spread_cost = spread_pips * 10 * lot
       
        slippage_pips = BACKTEST_CONFIG['slippage_pips']
        slippage_cost = slippage_pips * 10 * lot
       
        commission = BACKTEST_CONFIG['commission_per_lot'] * lot
       
        price_diff = (exit_price - entry_price) * 10000
        if direction == 'SELL':
            price_diff = -price_diff
       
        gross_pnl = price_diff * 10 * lot
        net_pnl = gross_pnl - spread_cost - slippage_cost - commission
       
        return net_pnl, commission, slippage_cost + spread_cost
   
    def check_risk_limits(self, current_date: datetime) -> bool:
        """Проверка риск-лимитов"""
        if self.current_day != current_date.date():
            self.current_day = current_date.date()
            self.daily_pnl = 0.0
       
        current_week = current_date.isocalendar()[1]
        if self.current_week != current_week:
            self.current_week = current_week
            self.weekly_pnl = 0.0
       
        daily_loss_pct = (self.daily_pnl / self.initial_balance) * 100
        if daily_loss_pct < -BACKTEST_CONFIG['daily_loss_limit_pct']:
            self.trading_blocked = True
            return False
       
        weekly_loss_pct = (self.weekly_pnl / self.initial_balance) * 100
        if weekly_loss_pct < -BACKTEST_CONFIG['weekly_loss_limit_pct']:
            self.trading_blocked = True
            return False
       
        self.trading_blocked = False
        return True
   
    def open_position(self, symbol: str, signal: Dict, current_price: float,
                     current_time: datetime, atr: float):
        """Открытие позиции"""
        if self.trading_blocked or len(self.positions) >= BACKTEST_CONFIG['max_positions']:
            return
       
        if symbol in self.positions:
            return
       
        direction = signal['signal']
        if direction not in ['BUY', 'SELL']:
            return
       
        stop_loss_pips = max(BACKTEST_CONFIG['stop_loss_pips'], atr * 10000 * 1.5)
        lot = self.calculate_lot_size(symbol, stop_loss_pips)
       
        if direction == 'BUY':
            stop_loss = current_price - (stop_loss_pips / 10000)
        else:
            stop_loss = current_price + (stop_loss_pips / 10000)
       
        position = Position(
            symbol=symbol,
            direction=direction,
            entry_price=current_price,
            entry_time=current_time,
            lot=lot,
            stop_loss=stop_loss
        )
       
        self.positions[symbol] = position
        logger.info(f"OPENED {symbol} {direction} @ {current_price:.5f}, lot: {lot:.2f}")
   
    def close_position(self, symbol: str, exit_price: float, exit_time: datetime, reason: str):
        """Закрытие позиции"""
        if symbol not in self.positions:
            return
       
        pos = self.positions[symbol]
       
        pnl, commission, costs = self.calculate_realistic_pnl(
            symbol, pos.direction, pos.entry_price, exit_price, pos.lot
        )
       
        price_diff = (exit_price - pos.entry_price) * 10000
        if pos.direction == 'SELL':
            price_diff = -price_diff
       
        self.balance += pnl
        self.daily_pnl += pnl
        self.weekly_pnl += pnl
       
        trade = Trade(
            symbol=symbol,
            direction=pos.direction,
            entry_price=pos.entry_price,
            exit_price=exit_price,
            entry_time=pos.entry_time,
            exit_time=exit_time,
            lot=pos.lot,
            pips=price_diff,
            pnl=pnl,
            commission=commission,
            slippage=costs,
            result='WIN' if pnl > 0 else 'LOSS'
        )
       
        self.trades.append(trade)
        self.total_trades += 1
        if pnl > 0:
            self.winning_trades += 1
        else:
            self.losing_trades += 1
       
        logger.info(f"CLOSED {symbol} @ {exit_price:.5f}, PnL: ${pnl:+.2f} ({price_diff:+.1f} pips)")
       
        del self.positions[symbol]
   
    def check_stop_loss(self, symbol: str, current_price: float, current_time: datetime):
        """Проверка стоп-лосса"""
        if symbol not in self.positions:
            return
       
        pos = self.positions[symbol]
       
        if pos.direction == 'BUY':
            if current_price <= pos.stop_loss:
                self.close_position(symbol, pos.stop_loss, current_time, "STOP LOSS")
        else:
            if current_price >= pos.stop_loss:
                self.close_position(symbol, pos.stop_loss, current_time, "STOP LOSS")
   
    def run(self):
        """Запуск Walk-Forward бэктеста"""
        logger.info("=" * 80)
        logger.info("🚀 STARTING 2-YEAR WALK-FORWARD BACKTEST")
        logger.info("=" * 80)
       
        if not self.initialize_mt5():
            return
       
        end_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        test_start_date = end_date - timedelta(days=BACKTEST_CONFIG['test_days'])
        initial_train_start = test_start_date - timedelta(days=BACKTEST_CONFIG['initial_training_days'])
       
        logger.info(f"Initial training: {initial_train_start.date()} to {test_start_date.date()}")
        logger.info(f"Testing period: {test_start_date.date()} to {end_date.date()}")
        logger.info(f"Retraining every: {BACKTEST_CONFIG['retrain_every_days']} days")
        logger.info(f"Initial balance: ${self.initial_balance:.2f}")
       
        for symbol in BACKTEST_CONFIG['symbols']:
            self.models[symbol] = OnlineMLModel(symbol)
       
        logger.info("\n" + "=" * 80)
        logger.info("📚 INITIAL TRAINING PHASE")
        logger.info("=" * 80)
       
        for symbol in BACKTEST_CONFIG['symbols']:
            hist_data = get_historical_data(symbol, initial_train_start, test_start_date)
            if hist_data is not None:
                hist_data = prepare_features(hist_data)
                self.models[symbol].train(hist_data)
       
        logger.info("\n" + "=" * 80)
        logger.info("🎯 WALK-FORWARD TESTING (2 YEARS)")
        logger.info("=" * 80)
       
        current_date = test_start_date
        last_retrain_date = test_start_date
        test_bar_count = 0
       
        while current_date < end_date:
            current_date += timedelta(days=1)
           
            if current_date.weekday() >= 5:
                continue
           
            test_bar_count += 1
           
            # ПЕРЕОБУЧЕНИЕ
            days_since_retrain = (current_date - last_retrain_date).days
            if days_since_retrain >= BACKTEST_CONFIG['retrain_every_days']:
                logger.info(f"\n🔄 RETRAINING (Day {test_bar_count}/730)")
                self.training_dates.append(current_date)
               
                for symbol in BACKTEST_CONFIG['symbols']:
                    train_data = get_historical_data(symbol, initial_train_start, current_date)
                    if train_data is not None:
                        train_data = prepare_features(train_data)
                        self.models[symbol].train(train_data)
               
                last_retrain_date = current_date
            else:
                self.testing_dates.append(current_date)
           
            if test_bar_count % 30 == 0:
                logger.info(f"\n📅 Progress: Day {test_bar_count}/730 ({test_bar_count*100//730}%)")
                logger.info(f" Balance: ${self.balance:.2f}, Trades: {self.total_trades}")
           
            self.check_risk_limits(current_date)
           
            for symbol in BACKTEST_CONFIG['symbols']:
                current_data = get_historical_data(
                    symbol,
                    current_date - timedelta(days=30),
                    current_date
                )
               
                if current_data is None or len(current_data) < 50:
                    continue
               
                current_data = prepare_features(current_data)
               
                if current_data is None or len(current_data) == 0:
                    continue
               
                current_price = current_data.iloc[-1]['close']
                current_time = current_data.iloc[-1]['time']
                atr = current_data.iloc[-1]['ATR']
               
                self.check_stop_loss(symbol, current_price, current_time)
               
                signal = self.models[symbol].predict(current_data)
               
                if symbol in self.positions:
                    pos = self.positions[symbol]
                   
                    if (pos.direction == 'BUY' and signal['signal'] == 'SELL') or \
                       (pos.direction == 'SELL' and signal['signal'] == 'BUY'):
                        self.close_position(symbol, current_price, current_time, "SIGNAL REVERSAL")
                    elif signal['confidence'] < 0.5:
                        self.close_position(symbol, current_price, current_time, "LOW CONFIDENCE")
                else:
                    if signal['signal'] in ['BUY', 'SELL'] and signal['confidence'] > 0.6:
                        self.open_position(symbol, signal, current_price, current_time, atr)
           
            equity = self.balance
            for symbol, pos in self.positions.items():
                current_data = get_historical_data(symbol, current_date - timedelta(days=1), current_date)
                if current_data is not None and len(current_data) > 0:
                    current_price = current_data.iloc[-1]['close']
                    floating_pnl, _, _ = self.calculate_realistic_pnl(
                        symbol, pos.direction, pos.entry_price, current_price, pos.lot
                    )
                    equity += floating_pnl
           
            self.equity_history.append(equity)
            self.balance_history.append(self.balance)
            self.dates.append(current_date)
       
        logger.info("\n📊 Closing all positions...")
        for symbol in list(self.positions.keys()):
            final_data = get_historical_data(symbol, end_date - timedelta(days=1), end_date)
            if final_data is not None and len(final_data) > 0:
                final_price = final_data.iloc[-1]['close']
                final_time = final_data.iloc[-1]['time']
                self.close_position(symbol, final_price, final_time, "END OF TEST")
       
        self.print_results()
        self.plot_all_charts()
        self.save_results()
       
        mt5.shutdown()
        logger.info("\n✅ 2-YEAR BACKTEST COMPLETED!")
   
    def print_results(self):
        """Вывод результатов"""
        logger.info("\n" + "=" * 80)
        logger.info("📊 2-YEAR BACKTEST RESULTS")
        logger.info("=" * 80)
       
        final_balance = self.balance
        total_pnl = final_balance - self.initial_balance
        total_return_pct = (total_pnl / self.initial_balance) * 100
       
        logger.info(f"\n💰 FINANCIAL RESULTS:")
        logger.info(f" Initial Balance: ${self.initial_balance:.2f}")
        logger.info(f" Final Balance: ${final_balance:.2f}")
        logger.info(f" Total PnL: ${total_pnl:+.2f} ({total_return_pct:+.2f}%)")
       
        if len(self.trades) > 0:
            wins = [t for t in self.trades if t.pnl > 0]
            losses = [t for t in self.trades if t.pnl <= 0]
           
            win_rate = len(wins) / len(self.trades)
            avg_win = np.mean([t.pnl for t in wins]) if wins else 0
            avg_loss = np.mean([t.pnl for t in losses]) if losses else 0
           
            logger.info(f"\n📈 TRADING STATISTICS:")
            logger.info(f" Total Trades: {len(self.trades)}")
            logger.info(f" Winning Trades: {len(wins)}")
            logger.info(f" Losing Trades: {len(losses)}")
            logger.info(f" Win Rate: {win_rate:.1%}")
            logger.info(f" Average Win: ${avg_win:.2f}")
            logger.info(f" Average Loss: ${avg_loss:.2f}")
           
            if avg_loss != 0:
                profit_factor = abs(sum(t.pnl for t in wins) / sum(t.pnl for t in losses))
                logger.info(f" Profit Factor: {profit_factor:.2f}")
           
            total_commission = sum(t.commission for t in self.trades)
            total_slippage = sum(t.slippage for t in self.trades)
            logger.info(f"\n💸 COSTS:")
            logger.info(f" Total Commissions: ${total_commission:.2f}")
            logger.info(f" Total Slippage+Spread: ${total_slippage:.2f}")
            logger.info(f" Total Costs: ${total_commission + total_slippage:.2f}")
       
        if len(self.equity_history) > 0:
            equity_array = np.array(self.equity_history)
            peak = np.maximum.accumulate(equity_array)
            drawdown = (peak - equity_array) / (peak + 1e-8)
            max_dd = np.max(drawdown) * 100
           
            logger.info(f"\n📉 RISK METRICS:")
            logger.info(f" Maximum Drawdown: {max_dd:.2f}%")
           
            if len(self.trades) > 1:
                returns = np.diff(self.balance_history) / (np.array(self.balance_history[:-1]) + 1e-8)
                sharpe = np.mean(returns) / (np.std(returns) + 1e-8) * np.sqrt(252)
                logger.info(f" Sharpe Ratio: {sharpe:.2f}")
   
    def plot_all_charts(self):
        """СОЗДАЁТ ВСЕ ГРАФИКИ - КАЖДЫЙ ОТДЕЛЬНО!"""
        logger.info("\n" + "=" * 80)
        logger.info("🎨 CREATING BEAUTIFUL CHARTS...")
        logger.info("=" * 80)
       
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
       
        # 1. EQUITY ONLY
        self.plot_equity_only(timestamp)
       
        # 2. BALANCE ONLY
        self.plot_balance_only(timestamp)
       
        # 3. EQUITY + BALANCE TOGETHER
        self.plot_equity_and_balance(timestamp)
       
        # 4. DRAWDOWN
        self.plot_drawdown(timestamp)
       
        # 5. SHARPE RATIO (rolling)
        self.plot_sharpe(timestamp)
       
        # 6. PROFIT FACTOR (rolling)
        self.plot_profit_factor(timestamp)
       
        # 7. TRAINING vs TESTING PERIODS
        self.plot_training_testing_periods(timestamp)
       
        logger.info(f"\n💚 ALL CHARTS SAVED!")
   
    def plot_equity_only(self, timestamp):
        """1. ТОЛЬКО EQUITY"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        equity = np.array(self.equity_history)
        initial = self.initial_balance
       
        ax.plot(self.dates, equity, color='#00ff41', linewidth=3.5, label='Equity', alpha=0.95)
        ax.axhline(initial, color='#666666', linewidth=2, linestyle=':', alpha=0.6)
       
        ax.fill_between(self.dates, equity, initial,
                        where=(equity >= initial), color='#00ff41', alpha=0.15)
        ax.fill_between(self.dates, equity, initial,
                        where=(equity < initial), color='#ff0000', alpha=0.15)
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Equity ($)', fontsize=18, color='#ffffff', fontweight='bold')
       
        final_pnl = equity[-1] - initial
        final_pct = (final_pnl / initial) * 100
        title_color = '#00ff41' if final_pnl >= 0 else '#ff0000'
       
        ax.set_title(
            f'🚀 EQUITY CURVE (2 YEARS)\n'
            f'PnL: ${final_pnl:+,.2f} ({final_pct:+.2f}%)',
            fontsize=24, color=title_color, fontweight='bold', pad=25
        )
       
        ax.legend(fontsize=16, loc='upper left', facecolor='#1a1a1a',
                 edgecolor='#00ff41', framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00ff41')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/EQUITY_ONLY_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Equity chart: {filename}")
        plt.close()
   
    def plot_balance_only(self, timestamp):
        """2. ТОЛЬКО BALANCE"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        balance = np.array(self.balance_history)
        initial = self.initial_balance
       
        ax.plot(self.dates, balance, color='#00aa2e', linewidth=3.5, label='Balance', alpha=0.95)
        ax.axhline(initial, color='#666666', linewidth=2, linestyle=':', alpha=0.6)
       
        ax.fill_between(self.dates, balance, initial,
                        where=(balance >= initial), color='#00aa2e', alpha=0.15)
        ax.fill_between(self.dates, balance, initial,
                        where=(balance < initial), color='#ff0000', alpha=0.15)
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Balance ($)', fontsize=18, color='#ffffff', fontweight='bold')
       
        final_pnl = balance[-1] - initial
        final_pct = (final_pnl / initial) * 100
        title_color = '#00aa2e' if final_pnl >= 0 else '#ff0000'
       
        ax.set_title(
            f'💰 BALANCE CURVE (2 YEARS)\n'
            f'PnL: ${final_pnl:+,.2f} ({final_pct:+.2f}%)',
            fontsize=24, color=title_color, fontweight='bold', pad=25
        )
       
        ax.legend(fontsize=16, loc='upper left', facecolor='#1a1a1a',
                 edgecolor='#00aa2e', framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00aa2e')  # ← ИСПРАВЛЕНИЕ ЗДЕСЬ
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/BALANCE_ONLY_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Balance chart: {filename}")
        plt.close()
   
    def plot_equity_and_balance(self, timestamp):
        """3. EQUITY + BALANCE ВМЕСТЕ"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        equity = np.array(self.equity_history)
        balance = np.array(self.balance_history)
        initial = self.initial_balance
       
        ax.plot(self.dates, equity, color='#00ff41', linewidth=3.5, label='Equity', alpha=0.95)
        ax.plot(self.dates, balance, color='#00aa2e', linewidth=2.5,
                label='Balance', alpha=0.8, linestyle='--')
        ax.axhline(initial, color='#666666', linewidth=2, linestyle=':', alpha=0.6)
       
        ax.fill_between(self.dates, equity, initial,
                        where=(equity >= initial), color='#00ff41', alpha=0.15)
        ax.fill_between(self.dates, equity, initial,
                        where=(equity < initial), color='#ff0000', alpha=0.15)
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Value ($)', fontsize=18, color='#ffffff', fontweight='bold')
       
        final_pnl = equity[-1] - initial
        final_pct = (final_pnl / initial) * 100
        title_color = '#00ff41' if final_pnl >= 0 else '#ff0000'
       
        ax.set_title(
            f'🚀 EQUITY & BALANCE (2 YEARS)\n'
            f'PnL: ${final_pnl:+,.2f} ({final_pct:+.2f}%)',
            fontsize=24, color=title_color, fontweight='bold', pad=25
        )
       
        ax.legend(fontsize=16, loc='upper left', facecolor='#1a1a1a',
                 edgecolor='#00ff41', framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00ff41')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/EQUITY_AND_BALANCE_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Equity+Balance chart: {filename}")
        plt.close()
   
    def plot_drawdown(self, timestamp):
        """4. DRAWDOWN"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        equity_array = np.array(self.equity_history)
        peak = np.maximum.accumulate(equity_array)
        drawdown = (peak - equity_array) / (peak + 1e-8) * 100
       
        ax.fill_between(self.dates, drawdown, 0, color='#ff0000', alpha=0.4)
        ax.plot(self.dates, drawdown, color='#ff0000', linewidth=3, alpha=0.9)
       
        max_dd = np.max(drawdown)
        max_dd_idx = np.argmax(drawdown)
        ax.axhline(max_dd, color='#ff4444', linewidth=2, linestyle='--', alpha=0.8)
        ax.text(self.dates[max_dd_idx], max_dd, f' Max DD: {max_dd:.2f}%',
                color='#ff4444', fontsize=14, fontweight='bold', va='bottom')
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Drawdown (%)', fontsize=18, color='#ffffff', fontweight='bold')
       
        ax.set_title(
            f'📉 DRAWDOWN (2 YEARS)\n'
            f'Maximum Drawdown: {max_dd:.2f}%',
            fontsize=24, color='#ff0000', fontweight='bold', pad=25
        )
       
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#ff0000')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/DRAWDOWN_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Drawdown chart: {filename}")
        plt.close()
   
    def plot_sharpe(self, timestamp):
        """5. SHARPE RATIO (rolling)"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        returns = np.diff(self.balance_history) / (np.array(self.balance_history[:-1]) + 1e-8)
       
        window = 60
        sharpe_rolling = []
        dates_rolling = []
       
        for i in range(window, len(returns)):
            window_returns = returns[i-window:i]
            sharpe = np.mean(window_returns) / (np.std(window_returns) + 1e-8) * np.sqrt(252)
            sharpe_rolling.append(sharpe)
            dates_rolling.append(self.dates[i])
       
        ax.plot(dates_rolling, sharpe_rolling, color='#00ff41', linewidth=3, alpha=0.9)
        ax.axhline(0, color='#666666', linewidth=2, linestyle=':', alpha=0.6)
        ax.axhline(1, color='#00aa2e', linewidth=1.5, linestyle='--', alpha=0.5, label='Sharpe = 1.0')
        ax.axhline(2, color='#00ff41', linewidth=1.5, linestyle='--', alpha=0.5, label='Sharpe = 2.0')
       
        ax.fill_between(dates_rolling, sharpe_rolling, 0,
                        where=np.array(sharpe_rolling) >= 0, color='#00ff41', alpha=0.15)
        ax.fill_between(dates_rolling, sharpe_rolling, 0,
                        where=np.array(sharpe_rolling) < 0, color='#ff0000', alpha=0.15)
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Sharpe Ratio', fontsize=18, color='#ffffff', fontweight='bold')
       
        final_sharpe = sharpe_rolling[-1] if sharpe_rolling else 0
        title_color = '#00ff41' if final_sharpe >= 1 else '#ff0000'
       
        ax.set_title(
            f'📊 SHARPE RATIO (60-day rolling)\n'
            f'Final Sharpe: {final_sharpe:.2f}',
            fontsize=24, color=title_color, fontweight='bold', pad=25
        )
       
        ax.legend(fontsize=14, loc='upper left', facecolor='#1a1a1a',
                 edgecolor='#00ff41', framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00ff41')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/SHARPE_RATIO_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Sharpe Ratio chart: {filename}")
        plt.close()
   
    def plot_profit_factor(self, timestamp):
        """6. PROFIT FACTOR (rolling)"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        if len(self.trades) < 50:
            plt.close()
            return
       
        window = 50
        pf_rolling = []
        dates_rolling = []
       
        for i in range(window, len(self.trades)):
            window_trades = self.trades[i-window:i]
            wins = sum(t.pnl for t in window_trades if t.pnl > 0)
            losses = abs(sum(t.pnl for t in window_trades if t.pnl <= 0))
           
            pf = wins / losses if losses > 0 else 0
            pf_rolling.append(pf)
            dates_rolling.append(window_trades[-1].exit_time)
       
        ax.plot(dates_rolling, pf_rolling, color='#00ff41', linewidth=3, alpha=0.9)
        ax.axhline(1, color='#666666', linewidth=2, linestyle=':', alpha=0.6, label='PF = 1.0')
        ax.axhline(1.5, color='#00aa2e', linewidth=1.5, linestyle='--', alpha=0.5, label='PF = 1.5')
        ax.axhline(2, color='#00ff41', linewidth=1.5, linestyle='--', alpha=0.5, label='PF = 2.0')
       
        ax.fill_between(dates_rolling, pf_rolling, 1,
                        where=np.array(pf_rolling) >= 1, color='#00ff41', alpha=0.15)
        ax.fill_between(dates_rolling, pf_rolling, 1,
                        where=np.array(pf_rolling) < 1, color='#ff0000', alpha=0.15)
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Profit Factor', fontsize=18, color='#ffffff', fontweight='bold')
       
        final_pf = pf_rolling[-1] if pf_rolling else 0
        title_color = '#00ff41' if final_pf >= 1.5 else '#ff0000'
       
        ax.set_title(
            f'💹 PROFIT FACTOR (50-trade rolling)\n'
            f'Final PF: {final_pf:.2f}',
            fontsize=24, color=title_color, fontweight='bold', pad=25
        )
       
        ax.legend(fontsize=14, loc='upper left', facecolor='#1a1a1a',
                 edgecolor='#00ff41', framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00ff41')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/PROFIT_FACTOR_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Profit Factor chart: {filename}")
        plt.close()
   
    def plot_training_testing_periods(self, timestamp):
        """7. TRAINING vs TESTING PERIODS"""
        fig = plt.figure(figsize=(20, 10), facecolor='#0a0a0a')
        ax = fig.add_subplot(111, facecolor='#0a0a0a')
       
        # График equity
        ax.plot(self.dates, self.equity_history, color='#00ff41',
                linewidth=2.5, label='Equity', alpha=0.7)
       
        # Маркеры периодов обучения
        for train_date in self.training_dates:
            if train_date in self.dates:
                idx = self.dates.index(train_date)
                ax.axvline(train_date, color='#ff9900', linewidth=1.5,
                          alpha=0.3, linestyle='--')
       
        # Легенда
        from matplotlib.patches import Patch
        legend_elements = [
            Patch(facecolor='#00ff41', label='Equity'),
            Patch(facecolor='#ff9900', alpha=0.3, label='Training Period'),
            Patch(facecolor='#0a0a0a', label='Testing Period')
        ]
       
        ax.grid(True, color='#1a1a1a', linewidth=1.5, alpha=0.3)
        ax.set_xlabel('Date', fontsize=18, color='#ffffff', fontweight='bold')
        ax.set_ylabel('Equity ($)', fontsize=18, color='#ffffff', fontweight='bold')
       
        ax.set_title(
            f'🎓 TRAINING vs TESTING PERIODS (2 YEARS)\n'
            f'Training Events: {len(self.training_dates)} | Testing Days: {len(self.testing_dates)}',
            fontsize=24, color='#00ff41', fontweight='bold', pad=25
        )
       
        ax.legend(handles=legend_elements, fontsize=14, loc='upper left',
                 facecolor='#1a1a1a', edgecolor='#00ff41',
                 framealpha=0.9, labelcolor='#ffffff')
        ax.tick_params(axis='both', colors='#ffffff', labelsize=14)
       
        for spine in ax.spines.values():
            spine.set_color('#00ff41')
            spine.set_linewidth(2)
       
        plt.tight_layout()
        filename = f"backtest_results/TRAINING_TESTING_{timestamp}.png"
        plt.savefig(filename, dpi=300, bbox_inches='tight', facecolor='#0a0a0a')
        logger.info(f"💚 Training/Testing chart: {filename}")
        plt.close()
   
    def save_results(self):
        """Сохранение результатов"""
        results = {
            'config': BACKTEST_CONFIG,
            'initial_balance': self.initial_balance,
            'final_balance': self.balance,
            'total_pnl': self.balance - self.initial_balance,
            'total_trades': len(self.trades),
            'winning_trades': self.winning_trades,
            'losing_trades': self.losing_trades,
            'win_rate': self.winning_trades / len(self.trades) if self.trades else 0,
            'equity_history': self.equity_history,
            'balance_history': self.balance_history,
            'dates': [d.isoformat() for d in self.dates],
            'trades': [
                {
                    'symbol': t.symbol,
                    'direction': t.direction,
                    'entry_price': t.entry_price,
                    'exit_price': t.exit_price,
                    'entry_time': t.entry_time.isoformat(),
                    'exit_time': t.exit_time.isoformat(),
                    'pnl': t.pnl,
                    'pips': t.pips
                }
                for t in self.trades
            ]
        }
       
        filename = f"backtest_results/results_2years_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(results, f, indent=2)
       
        logger.info(f"💾 Results saved: {filename}")
def main():
    print("""
╔═══════════════════════════════════════════════════════════════════════════════╗
║ ║
║ 🔬 QUANTUM MIDAS - 2-YEAR WALK-FORWARD BACKTEST ║
║ ║
║ ✅ 2 ГОДА тестирования (730 дней) ║
║ ✅ Honest testing on UNKNOWN data ║
║ ✅ Online learning - models retrain every 3 days ║
║ ✅ NO LOOK-AHEAD BIAS ║
║ ✅ Realistic execution (spread, slippage, commission) ║
║ ✅ 7 BEAUTIFUL GREEN-BLACK CHARTS ║
║ ║
╚═══════════════════════════════════════════════════════════════════════════════╝
    """)
   
    print(f"\n⚙️ CONFIGURATION:")
    print(f" Test Period: {BACKTEST_CONFIG['test_days']} days (2 YEARS)")
    print(f" Initial Training: {BACKTEST_CONFIG['initial_training_days']} days (1 year)")
    print(f" Retrain Every: {BACKTEST_CONFIG['retrain_every_days']} days")
    print(f" Initial Balance: ${BACKTEST_CONFIG['initial_balance']:.2f}")
    print(f" Risk per Trade: {BACKTEST_CONFIG['max_risk_per_trade_pct']}%")
    print(f" Max Positions: {BACKTEST_CONFIG['max_positions']}")
    print(f" Symbols: {', '.join(BACKTEST_CONFIG['symbols'])}")
   
    print(f"\n📊 CHARTS TO BE CREATED:")
    print(f" 1. Equity Only (green-black)")
    print(f" 2. Balance Only (green-black)")
    print(f" 3. Equity + Balance Together")
    print(f" 4. Drawdown")
    print(f" 5. Sharpe Ratio (rolling)")
    print(f" 6. Profit Factor (rolling)")
    print(f" 7. Training vs Testing Periods")
   
    input("\n⚡ Press ENTER to start 2-YEAR backtest...")
   
    backtest = WalkForwardBacktest()
    backtest.run()
if __name__ == "__main__":
    main()
