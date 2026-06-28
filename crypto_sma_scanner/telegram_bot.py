import logging
import time
from datetime import datetime, timezone

import aiohttp

from .config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID, DISCLAIMER

logger = logging.getLogger(__name__)


class TelegramAlerter:
    def __init__(self):
        self._session = None
        self._session_created: float = 0
        self._session_ttl: float = 3600

    async def _get_session(self):
        now = time.time()
        if (self._session is None or self._session.closed or
                now - self._session_created > self._session_ttl):
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=10)
            )
            self._session_created = now
        return self._session

    async def send_message(self, text: str) -> bool:
        if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
            logger.warning("Telegram not configured, skipping message")
            return False
        session = await self._get_session()
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "Markdown",
            "disable_web_page_preview": True,
        }
        try:
            async with session.post(url, json=payload, timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("ok"):
                        return True
                    logger.error(f"Telegram API error: {data}")
                else:
                    body = await resp.text()
                    logger.error(f"Telegram HTTP {resp.status}: {body}")
                return False
        except Exception as e:
            logger.error(f"Failed to send Telegram message: {e}")
            return False

    @staticmethod
    def _fmt_price(val: float) -> str:
        if val is None:
            return "N/A"
        if val >= 1000:
            return f"{val:,.2f}"
        s = f"{val:.8f}".rstrip("0").rstrip(".")
        parts = s.split(".")
        if len(parts) == 1:
            return s
        sig_digits = len(parts[1])
        return f"{val:.{max(2, sig_digits)}f}"

    @staticmethod
    def _time_str(timestamp: int) -> str:
        return datetime.fromtimestamp(timestamp / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    @staticmethod
    def _timeframe_label(tf: str) -> str:
        return tf.upper()

    def _build_signal(self, emoji: str, label: str, symbol: str, timeframe: str,
                      price: float, sl: float, tp: float, direction: str,
                      timestamp: int, source: str) -> str:
        ts = self._time_str(timestamp)
        sl_pct = round((sl / price - 1) * 100, 2) if price else 0
        tp_pct = round((tp / price - 1) * 100, 2) if price else 0
        sep = "━" * 31
        sub = "─" * 31
        return (
            f"{emoji} {label} · {symbol} · {self._timeframe_label(timeframe)}\n"
            f"{sep}\n"
            f"Price: {self._fmt_price(price)}\n"
            f"SL: {self._fmt_price(sl)} ({sl_pct:+.2f}%) · TP: {self._fmt_price(tp)} ({tp_pct:+.2f}%)\n"
            f"{sub}\n"
            f"50 SMA crossed {direction} 200 SMA | {source}\n"
            f"{ts}\n"
            f"⚠ SIGNAL ONLY — Not financial advice"
        )

    def format_golden_cross(self, symbol: str, timeframe: str, price: float,
                            sma50: float, sma200: float, sl: float, tp: float,
                            timestamp: int, source: str = "Binance") -> str:
        return self._build_signal("🚀", "GOLDEN CROSSOVER", symbol, timeframe,
                                  price, sl, tp, "ABOVE", timestamp, source)

    def format_death_cross(self, symbol: str, timeframe: str, price: float,
                           sma50: float, sma200: float, sl: float, tp: float,
                           timestamp: int, source: str = "Binance") -> str:
        return self._build_signal("❌", "DEATH CROSSOVER", symbol, timeframe,
                                  price, sl, tp, "BELOW", timestamp, source)

    def format_startup(self, n_coins: int, timeframes: list) -> str:
        return (
            f"🤖 *SMA Crossover Scanner Started*\n\n"
            f"Monitoring top {n_coins} USDT pairs\n"
            f"Timeframes: {', '.join(timeframes)}\n"
            f"Strategy: SMA 50/200 Crossover\n"
            f"Data: Binance → Bybit → CoinGecko\n"
            f"Status: SIGNAL ONLY — No auto trading"
        )

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
