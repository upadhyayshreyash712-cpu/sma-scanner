import asyncio
import logging
import time
from typing import Optional

import aiohttp

from .config import (
    TOP_N_COINS, OHLCV_LIMIT, REQUEST_RETRIES, REQUEST_RETRY_DELAY,
    COINGECKO_BASE, COINGECKO_TIMEOUT
)

logger = logging.getLogger(__name__)

BINANCE_API = "https://api.binance.com"
BYBIT_API = "https://api.bybit.com"

TF_MAP = {
    "1m": "1m", "5m": "5m", "15m": "15m", "30m": "30m",
    "1h": "1h", "2h": "2h", "4h": "4h", "6h": "6h",
    "8h": "8h", "1d": "1d",
}


class ExchangeManager:
    def __init__(self):
        self._session: Optional[aiohttp.ClientSession] = None
        self._session_created: float = 0
        self._session_ttl: float = 1800
        self._binance_symbols: set = set()
        self._bybit_symbols: set = set()
        self._last_provider: str = "binance"

    async def _get_session(self) -> aiohttp.ClientSession:
        now = time.time()
        if (self._session is None or self._session.closed or
                now - self._session_created > self._session_ttl):
            if self._session and not self._session.closed:
                await self._session.close()
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15, connect=10),
                connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300)
            )
            self._session_created = now
        return self._session

    async def initialize(self):
        session = await self._get_session()
        try:
            async with session.get(f"{BINANCE_API}/api/v3/exchangeInfo", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for s in data.get("symbols", []):
                        if s["status"] == "TRADING" and s["quoteAsset"] == "USDT" and s["isSpotTradingAllowed"]:
                            self._binance_symbols.add(s["symbol"])
                    logger.info(f"Binance: {len(self._binance_symbols)} USDT spot symbols")
        except Exception as e:
            logger.warning(f"Binance exchangeInfo failed: {e}")
        try:
            async with session.get(f"{BYBIT_API}/v5/market/instruments-info?category=spot", timeout=10) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    for item in data.get("result", {}).get("list", []):
                        sym = item.get("symbol", "")
                        if sym.endswith("USDT") and item.get("status") == "Trading":
                            self._bybit_symbols.add(sym)
                    logger.info(f"Bybit: {len(self._bybit_symbols)} USDT spot symbols")
        except Exception as e:
            logger.warning(f"Bybit instruments failed: {e}")

    def _provider_label(self, provider: str) -> str:
        return {"binance": "Binance", "bybit": "Bybit", "coingecko": "CoinGecko"}.get(provider, provider)

    async def _fetch_binance_ohlcv(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        session = await self._get_session()
        url = f"{BINANCE_API}/api/v3/klines"
        params = {"symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return [[int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5])] for c in data]
                elif resp.status == 429:
                    logger.warning(f"Binance rate limited on {symbol}")
                    return None
                else:
                    logger.debug(f"Binance {symbol}: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.debug(f"Binance {symbol} error: {e}")
            return None

    async def _fetch_bybit_ohlcv(self, symbol: str, interval: str, limit: int) -> Optional[list]:
        session = await self._get_session()
        url = f"{BYBIT_API}/v5/market/kline"
        params = {"category": "spot", "symbol": symbol, "interval": interval, "limit": limit}
        try:
            async with session.get(url, params=params) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("retCode") == 0:
                        items = data.get("result", {}).get("list", [])
                        items.reverse()
                        return [[int(it[0]), float(it[1]), float(it[2]), float(it[3]), float(it[4]), float(it[5])] for it in items]
                    else:
                        logger.debug(f"Bybit {symbol}: {data.get('retMsg')}")
                        return None
                else:
                    logger.debug(f"Bybit {symbol}: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.debug(f"Bybit {symbol} error: {e}")
            return None

    async def _fetch_coingecko_ohlcv(self, coin_id: str, days: int) -> Optional[list]:
        session = await self._get_session()
        url = f"{COINGECKO_BASE}/coins/{coin_id}/ohlc"
        params = {"vs_currency": "usd", "days": days}
        try:
            async with session.get(url, params=params, timeout=COINGECKO_TIMEOUT) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data
                else:
                    logger.debug(f"CoinGecko {coin_id}: HTTP {resp.status}")
                    return None
        except Exception as e:
            logger.debug(f"CoinGecko {coin_id} error: {e}")
            return None

    async def fetch_ohlcv(self, symbol: str, tmf: str, limit: int = OHLCV_LIMIT,
                          coin_id: Optional[str] = None) -> tuple:
        interval = TF_MAP.get(tmf, tmf)
        last_err = None
        for attempt in range(REQUEST_RETRIES):
            ohlcv = await self._fetch_binance_ohlcv(symbol, interval, limit)
            if ohlcv and len(ohlcv) >= 2:
                self._last_provider = "binance"
                return ohlcv, "Binance"
            last_err = f"Binance returned insufficient data"
            ohlcv = await self._fetch_bybit_ohlcv(symbol, interval, limit)
            if ohlcv and len(ohlcv) >= 2:
                self._last_provider = "bybit"
                return ohlcv, "Bybit"
            last_err = f"Bybit returned insufficient data"
            if coin_id:
                days_map = {"15m": 30, "1h": 90, "4h": 365, "1d": 365}
                cg_days = days_map.get(tmf, 90)
                raw = await self._fetch_coingecko_ohlcv(coin_id, cg_days)
                if raw and len(raw) >= 50:
                    co = [[int(c[0]), float(c[1]), float(c[2]), float(c[3]), float(c[4]), 0.0] for c in raw]
                    self._last_provider = "coingecko"
                    return co, "CoinGecko"
                last_err = f"CoinGecko returned insufficient data"
            if attempt < REQUEST_RETRIES - 1:
                await asyncio.sleep(REQUEST_RETRY_DELAY * (attempt + 1))
        raise Exception(f"All sources failed for {symbol} {tmf}: {last_err}")

    async def get_usdt_symbols(self) -> list:
        all_syms = sorted(self._binance_symbols & self._bybit_symbols)
        if not all_syms:
            all_syms = sorted(self._binance_symbols) if self._binance_symbols else sorted(self._bybit_symbols)
        return [s for s in all_syms if s.endswith("USDT")]

    async def get_top_coins_with_metadata(self) -> list:
        session = await self._get_session()
        url = f"{COINGECKO_BASE}/coins/markets"
        params = {
            "vs_currency": "usd",
            "order": "market_cap_desc",
            "per_page": TOP_N_COINS,
            "page": 1,
            "sparkline": "false",
            "price_change_percentage": "24h",
        }
        try:
            async with session.get(url, params=params, timeout=COINGECKO_TIMEOUT) as resp:
                if resp.status == 200:
                    return await resp.json()
                else:
                    logger.error(f"CoinGecko top coins: HTTP {resp.status}")
                    return []
        except Exception as e:
            logger.error(f"CoinGecko top coins failed: {e}")
            return []

    async def get_top_n_usdt_symbols(self, n: int = TOP_N_COINS) -> list:
        coins = await self.get_top_coins_with_metadata()
        usdt_set = set(await self.get_usdt_symbols())
        ranked = []
        coin_id_map = {}
        for c in coins:
            sym = c["symbol"].upper() + "USDT"
            if sym in usdt_set:
                ranked.append(sym)
                coin_id_map[sym] = c["id"]
        for sym in sorted(usdt_set):
            if sym not in ranked and len(ranked) < n:
                ranked.append(sym)
        return ranked[:n], coin_id_map

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()
