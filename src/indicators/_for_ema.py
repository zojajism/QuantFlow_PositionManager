from typing import Deque, Dict, Any, List
import math

def _extract_closes(dq: Deque[Dict[str, Any]]) -> List[float]:
    # dq is already oldest -> newest
    closes: List[float] = []
    for c in dq:
        try:
            closes.append(float(c["close"]))
        except (KeyError, TypeError, ValueError):
            closes.append(float("nan"))
    return closes

def ema_from_closes(closes: List[float], period: int) -> float:
    if len(closes) < period:
        return float("nan")

    # Optional: if you want to be strict about bad data
    if any(math.isnan(x) for x in closes):
        return float("nan")

    alpha = 2.0 / (period + 1.0)

    # Classic init: SMA of first 'period'
    ema = sum(closes[:period]) / period

    # Run forward on the rest
    for price in closes[period:]:
        ema = alpha * price + (1.0 - alpha) * ema

    return ema

# --- USAGE in your message handler, right after append() ---

key = Keys(exchange, symbol, timeframe)

dq = buffers.CANDLE_BUFFER.get_or_create(key)   # Deque[Dict[str, Any]] (oldest -> newest)
closes = _extract_closes(dq)                    # build once

ema20 = ema_from_closes(closes, 20)
ema50 = ema_from_closes(closes, 50)

# Example: warmup / guard
if not (math.isnan(ema20) or math.isnan(ema50)):
    # use ema20/ema50 for bias logic here
    pass
