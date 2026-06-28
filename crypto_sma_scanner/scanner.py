import asyncio
import logging
import time
from aiohttp import web

from .config import (
    SMA_FAST, SMA_SLOW, TIMEFRAMES, TOP_N_COINS,
    SL_PERCENT, TP_PERCENT, WORKER_COUNT, SCAN_INTERVAL_SECONDS
)
from .exchange import ExchangeManager
from .database import DatabaseManager
from .indicators import compute_sma, detect_crossover, SignalType
from .telegram_bot import TelegramAlerter

logger = logging.getLogger(__name__)


class SMAScanner:
    def __init__(self, exchange: ExchangeManager, db: DatabaseManager, telegram: TelegramAlerter):
        self.exchange = exchange
        self.db = db
        self.telegram = telegram
        self.running = False
        self.watchlist = []
        self.coin_id_map = {}
        self._last_watchlist_update = 0

    async def _update_watchlist(self, force=False):
        now = time.time()
        if not force and (now - self._last_watchlist_update) < 86400:
            cached = self.db.get_watchlist()
            if cached:
                self.watchlist = cached
                return
        symbols, coin_ids = await self.exchange.get_top_n_usdt_symbols(TOP_N_COINS)
        if not symbols:
            cached = self.db.get_watchlist()
            if cached:
                self.watchlist = cached
                logger.info("Using cached watchlist")
            return
        formatted = [{"symbol": s, "name": s.replace("USDT", ""), "market_cap": 0} for s in symbols]
        self.db.update_watchlist(formatted)
        self.watchlist = symbols
        self.coin_id_map = coin_ids
        self._last_watchlist_update = now
        logger.info(f"Watchlist updated: {len(self.watchlist)} coins")

    async def _process_symbol(self, symbol: str, timeframe: str) -> list:
        signals = []
        try:
            needed = SMA_SLOW + 20
            coin_id = self.coin_id_map.get(symbol)
            ohlcv, source = await self.exchange.fetch_ohlcv(symbol, timeframe, limit=needed, coin_id=coin_id)
            if not ohlcv or len(ohlcv) < SMA_SLOW + 2:
                logger.debug(f"Insufficient data for {symbol} {timeframe}: {len(ohlcv) if ohlcv else 0}")
                return signals
            closes = [c[4] for c in ohlcv]
            sma50 = compute_sma(closes, SMA_FAST)
            sma200 = compute_sma(closes, SMA_SLOW)
            last_idx = len(closes) - 1
            signal_type = detect_crossover(sma50, sma200, last_idx)
            if signal_type is None:
                return signals
            candle = ohlcv[last_idx]
            candle_ts = candle[0]
            if self.db.is_candle_processed(symbol, timeframe, candle_ts):
                return signals
            self.db.mark_candle_processed(symbol, timeframe, candle_ts)
            if self.db.signal_exists(symbol, timeframe, candle_ts, signal_type):
                return signals
            entry_price = candle[4]
            price_str = f"{entry_price:.10f}"
            decimal_places = len(price_str.split('.')[1].rstrip('0')) if '.' in price_str else 0
            price_precision = max(decimal_places + 2, 2)
            if signal_type == SignalType.GOLDEN_CROSS:
                sl = round(entry_price * (1 - SL_PERCENT), price_precision)
                tp = round(entry_price * (1 + TP_PERCENT), price_precision)
            else:
                sl = round(entry_price * (1 + SL_PERCENT), price_precision)
                tp = round(entry_price * (1 - TP_PERCENT), price_precision)
            self.db.record_signal(
                symbol, timeframe, signal_type, entry_price,
                sma50[last_idx], sma200[last_idx], sl, tp, candle_ts, source
            )
            state = self.db.get_sma_state(symbol, timeframe)
            prev_fast = state["sma_fast"] if state else sma50[last_idx]
            prev_slow = state["sma_slow"] if state else sma200[last_idx]
            self.db.update_sma_state(
                symbol, timeframe, sma50[last_idx], sma200[last_idx],
                prev_fast, prev_slow, candle_ts, candle_ts, signal_type
            )
            signals.append({
                "symbol": symbol,
                "timeframe": timeframe,
                "signal_type": signal_type,
                "price": entry_price,
                "sma50": sma50[last_idx],
                "sma200": sma200[last_idx],
                "sl": sl,
                "tp": tp,
                "timestamp": candle_ts,
                "source": source,
            })
            logger.info(f"Signal: {signal_type} {symbol} {timeframe} @ {entry_price} ({source})")
        except Exception as e:
            logger.error(f"Error processing {symbol} {timeframe}: {e}")
        return signals

    async def scan_symbol(self, symbol: str) -> list:
        tasks = [self._process_symbol(symbol, tf) for tf in TIMEFRAMES]
        results = await asyncio.gather(*tasks)
        flat = []
        for r in results:
            flat.extend(r)
        return flat

    async def scan_all(self) -> list:
        await self._update_watchlist()
        all_signals = []
        sem = asyncio.Semaphore(WORKER_COUNT)

        async def worker(sym):
            async with sem:
                return await self.scan_symbol(sym)

        tasks = [worker(sym) for sym in self.watchlist]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, list):
                all_signals.extend(r)
            elif isinstance(r, Exception):
                logger.error(f"Worker error: {r}")
        return all_signals

    async def _send_signals(self, signals: list):
        for s in signals:
            try:
                sym_display = s["symbol"]
                if s["signal_type"] == SignalType.GOLDEN_CROSS:
                    msg = self.telegram.format_golden_cross(
                        sym_display, s["timeframe"], s["price"],
                        s["sma50"], s["sma200"], s["sl"], s["tp"],
                        s["timestamp"], s["source"]
                    )
                else:
                    msg = self.telegram.format_death_cross(
                        sym_display, s["timeframe"], s["price"],
                        s["sma50"], s["sma200"], s["sl"], s["tp"],
                        s["timestamp"], s["source"]
                    )
                ok = await self.telegram.send_message(msg)
                if ok:
                    logger.info(f"Sent {s['signal_type']} alert for {s['symbol']} {s['timeframe']}")
                else:
                    logger.error(f"Failed to send alert for {s['symbol']} {s['timeframe']}")
            except Exception as e:
                logger.error(f"Error sending alert: {e}")

    async def run_once(self) -> list:
        logger.info("Starting scan cycle...")
        start = time.time()
        signals = await self.scan_all()
        elapsed = time.time() - start
        logger.info(f"Scanned {len(self.watchlist)} coins in {elapsed:.1f}s, found {len(signals)} signals")
        if signals:
            await self._send_signals(signals)
        return signals

    async def _health_server(self):
        async def health(request):
            return web.Response(text="OK")
        app = web.Application()
        app.router.add_get("/health", health)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", 8080)
        await site.start()
        logger.info("Health server started on :8080")
        while self.running:
            await asyncio.sleep(60)
        await runner.cleanup()

    async def run_loop(self):
        self.running = True
        async with asyncio.TaskGroup() as tg:
            tg.create_task(self._health_server())
            tg.create_task(self._run_scanner())
        logger.info("Scanner loop stopped")

    async def _run_scanner(self):
        await self.telegram.send_message(
            self.telegram.format_startup(len(self.watchlist) or TOP_N_COINS, TIMEFRAMES)
        )
        logger.info("Scanner loop started")
        cycle_count = 0
        heartbeat_interval = max(1, 3600 // SCAN_INTERVAL_SECONDS)
        while self.running:
            try:
                await self.run_once()
                cycle_count += 1
                if cycle_count % heartbeat_interval == 0:
                    h = f"🤖 Heartbeat — {cycle_count} cycles · {len(self.watchlist)} coins · OK"
                    await self.telegram.send_message(h)
                    logger.info(h)
                await asyncio.sleep(SCAN_INTERVAL_SECONDS)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Loop error: {e}", exc_info=True)
                await asyncio.sleep(30)
        logger.info("Scanner loop stopped")

    def stop(self):
        self.running = False
