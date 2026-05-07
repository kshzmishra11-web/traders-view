import logging
from logging.handlers import RotatingFileHandler
import os

from base_topic_service import run_topic_service


HOST = "127.0.0.1"
PORT = 8792
SECTION_NAME = "Financial"
HEADLINES_PER_SOURCE = 8
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 20

SOURCES = [
    {
        "source_id": "financial:financial-times",
        "source_name": "Financial Times",
        "feed_url": "https://www.ft.com/?format=rss",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Aft.com+finance&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.ft.com/",
        "type": "feed",
    },
    {
        "source_id": "financial:yahoo-finance",
        "source_name": "Yahoo Finance",
        "feed_url": "https://finance.yahoo.com/rss/topstories",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Afinance.yahoo.com&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://finance.yahoo.com/",
        "type": "feed",
    },
    {
        "source_id": "financial:cnbc",
        "source_name": "CNBC",
        "feed_url": "https://www.cnbc.com/id/100003114/device/rss/rss.html",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Acnbc.com+finance&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.cnbc.com/",
        "type": "feed",
    },
    {
        "source_id": "financial:marketwatch",
        "source_name": "MarketWatch",
        "feed_url": "https://news.google.com/rss/search?q=site%3Amarketwatch.com&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.marketwatch.com/",
        "type": "feed",
    },
    {
        "source_id": "financial:bloomberg-etf",
        "source_name": "Bloomberg ETF (Google News)",
        "feed_url": "https://news.google.com/rss/search?q=site%3Abloomberg.com+ETF&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.bloomberg.com/",
        "type": "feed",
    },
    {
        "source_id": "financial:seeking-alpha",
        "source_name": "Seeking Alpha",
        "feed_url": "https://seekingalpha.com/market_currents.xml",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Aseekingalpha.com+markets&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://seekingalpha.com/",
        "type": "feed",
    },
]


log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "financial_service.log")
handler = RotatingFileHandler(log_path, maxBytes=1024000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[handler],
)
logging.info("Financial service started.")


def main():
    run_topic_service(
        section_name=SECTION_NAME,
        port=PORT,
        sources=SOURCES,
        headlines_per_source=HEADLINES_PER_SOURCE,
        request_timeout=REQUEST_TIMEOUT,
        cache_ttl_seconds=CACHE_TTL_SECONDS,
        section_build_timeout=SECTION_BUILD_TIMEOUT,
        host=HOST,
    )


if __name__ == "__main__":
    main()
