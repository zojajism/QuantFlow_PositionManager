from collections import deque, defaultdict
import datetime
import decimal
import logging
from typing import Deque, Dict, Any, Optional, List, DefaultDict

from database.db_general import get_ema_states_from_db, insert_ema_state_to_db
import public_module
from indicators.ema import calc_ema_from_candles

logger = logging.getLogger(__name__)


class EMAState:
    def __init__(self, exchange: str, symbol: str, timeframe: str, close_time: datetime, ema_slow: decimal, ema_fast: decimal, bios: int, trend: str, flipped: bool, candle_open: str, candle_close: str, ema_distance: decimal):
        self.exchange = exchange
        self.symbol = symbol
        self.timeframe = timeframe
        self.close_time = close_time
        self.ema_slow = ema_slow
        self.ema_fast = ema_fast
        self.bios = bios  # -1, 0, 1
        self.trend = trend  # 'U', 'D', 'N'
        self.flipped = flipped
        self.candle_open = candle_open  # 'A', 'B', 'C'
        self.candle_close = candle_close  # 'A', 'B', 'C'
        self.ema_distance = ema_distance


# Dictionary to hold deques of EMAState objects, keyed by (exchange, symbol, timeframe)
ema_states: DefaultDict[tuple[str, str, str], Deque[EMAState]] = defaultdict(lambda: deque(maxlen=100))


def ema_state_initializer(exchange: str, symbols: List[str], timeframes: List[str], n: int = 100) -> int:
    """
    Initialize ema_states by loading the last n EMAState records from the database
    for each combination of exchange, symbol, and timeframe.
    Clears existing data for each key before loading.
    Returns the total number of EMAState items loaded across all keys.
    """
    for symbol in symbols:
        for timeframe in timeframes:
            key = (exchange, symbol, timeframe)
            states_data = get_ema_states_from_db(exchange, symbol, timeframe, n)
            
            # Clear the existing deque
            ema_states[key].clear()
            
            # Create EMAState objects and append to the deque
            for state_dict in states_data:
                state = EMAState(**state_dict)
                ema_states[key].append(state)
    
    # Return the total count of items in all deques
    total_count = sum(len(deque) for deque in ema_states.values())
    return total_count



async def calculate_ema_state(exchange: str, symbol: str, timeframe: str, candles: Deque[Dict[str, Any]], open_price: decimal, close_price: decimal, close_time: datetime) -> None:
    
    try:

        ema_fast = calc_ema_from_candles(candles, public_module.EMA_FAST)
        ema_slow = calc_ema_from_candles(candles, public_module.EMA_SLOW)
       
        if ema_fast == ema_slow:
            bios = 0
        elif ema_fast > ema_slow:
            bios = 1
        else:
            bios = -1

        if open_price > ema_fast:
            candle_open = "A"
        elif open_price < ema_slow:
            candle_open = "C"
        elif open_price <= ema_fast and open_price >= ema_slow:
            candle_open = "B"

        if close_price > ema_fast:
            candle_close = "A"
        elif close_price < ema_slow:
            candle_close = "C"
        elif close_price <= ema_fast and close_price >= ema_slow:
            candle_close = "B"
        
        #ema_distance = abs(ema_fast - ema_slow) / ema_slow * decimal.Decimal(100) if ema_slow != 0 else decimal.Decimal(0)
        ema_distance = abs(ema_fast - ema_slow) / public_module._pip_size(symbol)

        new_state = EMAState(
            exchange=exchange,
            symbol=symbol,
            timeframe=timeframe,
            close_time=close_time,
            ema_slow=ema_slow,
            ema_fast=ema_fast,
            bios=bios,
            trend="N",  
            flipped=False, 
            candle_open=candle_open,
            candle_close=candle_close,
            ema_distance=ema_distance
        )
        key = (new_state.exchange, new_state.symbol, new_state.timeframe)
        ema_states[key].append(new_state)


        await insert_ema_state_to_db(
            new_state.exchange, new_state.symbol, new_state.timeframe, new_state.close_time,
            new_state.ema_slow, new_state.ema_fast, new_state.bios, new_state.trend,
            new_state.flipped, new_state.candle_open, new_state.candle_close, new_state.ema_distance
        )

        logger.info(f"Calculated EMAs for {symbol} {timeframe}: EMA_{public_module.EMA_FAST} = {ema_fast}, EMA_{public_module.EMA_SLOW} = {ema_slow}"
                    f"  bios = {bios}, candle_open = {candle_open}, candle_close = {candle_close}, trend = N, flipped = False, ema_distance = {ema_distance}")

    except Exception as e:
        logger.error(f"Error calculating EMAs for {symbol} {timeframe}: {e}")


