import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

DATABASE_PATH = os.getenv("DATABASE_PATH", "crypto_scanner.db")
SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", "30"))

TOP_N_COINS = 50
SMA_FAST = 50
SMA_SLOW = 200
TIMEFRAMES = ["15m", "1h"]

COINGECKO_BASE = "https://api.coingecko.com/api/v3"
COINGECKO_TIMEOUT = 15

OHLCV_LIMIT = 250
REQUEST_RETRIES = 2
REQUEST_RETRY_DELAY = 1

SL_PERCENT = 0.02
TP_PERCENT = 0.05

WORKER_COUNT = 10

DISCLAIMER = (
    "\n\n⚠ Educational/Informational Only.\n"
    "Not Financial Advice.\n"
    "No trades executed automatically."
)
