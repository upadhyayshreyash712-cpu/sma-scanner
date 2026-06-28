from typing import Optional

from .config import SMA_FAST, SMA_SLOW


class SignalType:
    GOLDEN_CROSS = "golden_cross"
    DEATH_CROSS = "death_cross"


def compute_sma(prices: list, period: int) -> list:
    if len(prices) < period:
        return []
    sma = []
    running_sum = sum(prices[:period])
    sma.append(running_sum / period)
    for i in range(period, len(prices)):
        running_sum += prices[i] - prices[i - period]
        sma.append(running_sum / period)
    return [None] * (period - 1) + sma


def compute_sma_50_200(closes: list) -> tuple:
    sma50 = compute_sma(closes, SMA_FAST)
    sma200 = compute_sma(closes, SMA_SLOW)
    return sma50, sma200


def detect_crossover(sma50: list, sma200: list, idx: int) -> Optional[str]:
    if idx < 1 or idx >= len(sma50) or idx >= len(sma200):
        return None
    prev_fast = sma50[idx - 1]
    prev_slow = sma200[idx - 1]
    curr_fast = sma50[idx]
    curr_slow = sma200[idx]
    if any(v is None for v in (prev_fast, prev_slow, curr_fast, curr_slow)):
        return None
    if prev_fast <= prev_slow and curr_fast > curr_slow:
        return SignalType.GOLDEN_CROSS
    if prev_fast >= prev_slow and curr_fast < curr_slow:
        return SignalType.DEATH_CROSS
    return None
