#!/usr/bin/env python3
"""
SMA Crossover Crypto Signal Bot
Binance → Bybit → CoinGecko — Signal Only, No Auto Trading
"""
import argparse
import asyncio
import logging
import os
import sys
import signal

from .config import DATABASE_PATH, TIMEFRAMES, TOP_N_COINS
from .exchange import ExchangeManager
from .database import DatabaseManager
from .telegram_bot import TelegramAlerter
from .scanner import SMAScanner

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.FileHandler("crypto_scanner.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger("main")


async def async_main(args):
    exchange = ExchangeManager()
    db = DatabaseManager(DATABASE_PATH)
    telegram = TelegramAlerter()

    try:
        await exchange.initialize()

        if args.command == "scan":
            scanner = SMAScanner(exchange, db, telegram)
            await scanner._update_watchlist(force=True)
            if args.once:
                await scanner.run_once()
            else:
                loop_task = asyncio.create_task(scanner.run_loop())

                def shutdown():
                    scanner.stop()
                    loop_task.cancel()

                loop = asyncio.get_running_loop()
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.add_signal_handler(sig, shutdown)
                    except NotImplementedError:
                        pass
                try:
                    await loop_task
                except asyncio.CancelledError:
                    pass

        elif args.command == "watchlist":
            symbols, _ = await exchange.get_top_n_usdt_symbols(TOP_N_COINS)
            coins = await exchange.get_top_coins_with_metadata()
            coin_map = {c["symbol"].upper(): c for c in coins}
            print(f"\n{'Rank':<6} {'Symbol':<16} {'Name':<30} {'Market Cap':<22}")
            print("-" * 75)
            for i, sym in enumerate(symbols[:TOP_N_COINS], 1):
                base = sym.replace("USDT", "")
                c = coin_map.get(base)
                name = c["name"][:28] if c else ""
                mc = f"${c['market_cap']:,.0f}" if c and c.get("market_cap") else "N/A"
                print(f"{i:<6} {sym:<16} {name:<30} {mc:<22}")

        elif args.command == "signals":
            signals = db.get_all_signals()
            print(f"\n{'ID':<5} {'Symbol':<16} {'TF':<5} {'Type':<14} {'Price':<14} {'SL':<14} {'TP':<14} {'Source':<12} {'Time':<20}")
            print("-" * 115)
            for s in signals[:50]:
                print(f"{s['id']:<5} {s['symbol']:<16} {s['timeframe']:<5} {s['signal_type']:<14} {s['entry_price']:<14} {s['stop_loss']:<14} {s['take_profit']:<14} {s['data_source']:<12} {s['detected_at'][:19]:<20}")

        elif args.command == "export":
            path = args.output
            db.export_signals_csv(path)
            print(f"Exported signals to {path}")

    finally:
        await exchange.close()
        await telegram.close()


def main():
    parser = argparse.ArgumentParser(
        description="SMA Crossover Crypto Signal Bot (Binance → Bybit → CoinGecko)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s scan                    # Run continuous scanner
  %(prog)s scan --once             # Single scan cycle
  %(prog)s watchlist               # Show top 100 coins
  %(prog)s signals                 # Show recent signals
  %(prog)s export                  # Export signals to CSV
        """
    )
    sub = parser.add_subparsers(dest="command", required=True)

    scan_p = sub.add_parser("scan", help="Run live signal scanner")
    scan_p.add_argument("--once", action="store_true", help="Single scan cycle")

    sub.add_parser("watchlist", help="Show top 100 coins tracked")
    sub.add_parser("signals", help="Show recent signals")

    export_p = sub.add_parser("export", help="Export signals to CSV")
    export_p.add_argument("--output", default="signals_export.csv", help="Output CSV path")

    args = parser.parse_args()
    try:
        asyncio.run(async_main(args))
    except KeyboardInterrupt:
        print("\nShutting down...")
    except Exception as e:
        logger.exception("Fatal error")
        sys.exit(1)


if __name__ == "__main__":
    main()
