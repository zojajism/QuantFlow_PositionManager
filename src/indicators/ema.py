from decimal import Decimal
from typing import Deque, Dict, Any, Optional

def calc_ema_from_candles(
    candles: Deque[Dict[str, Any]],
    period: int,
    price_key: str = "close",
) -> Optional[Decimal]:
    if len(candles) < period:
        return None

    # k = 2 / (period + 1) as Decimal
    k = Decimal(2) / Decimal(period + 1)
    one_minus_k = Decimal(1) - k

    first = list(candles)[:period]

    # Ensure prices are Decimal (in case some are int/float/str)
    ema = sum(Decimal(c[price_key]) for c in first) / Decimal(period)

    for c in list(candles)[period:]:
        price = Decimal(c[price_key])
        ema = price * k + ema * one_minus_k
        
        ema = round(ema, 5)

    return ema
