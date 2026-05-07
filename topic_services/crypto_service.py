import logging
from logging.handlers import RotatingFileHandler
import os

from base_topic_service import run_topic_service


HOST = "127.0.0.1"
PORT = 8793
SECTION_NAME = "Crypto"
HEADLINES_PER_SOURCE = 8
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 20

SOURCES = [
    {
        "source_id": "crypto:coindesk",
        "source_name": "CoinDesk",
        "feed_url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Acoindesk.com+crypto&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.coindesk.com/",
        "type": "feed",
    },
    {
        "source_id": "crypto:cointelegraph",
        "source_name": "Cointelegraph",
        "feed_url": "https://cointelegraph.com/rss",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Acointelegraph.com+crypto&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://cointelegraph.com/",
        "type": "feed",
    },
    {
        "source_id": "crypto:bitcoinmagazine",
        "source_name": "Bitcoin Magazine",
        "feed_url": "https://bitcoinmagazine.com/feed",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Abitcoinmagazine.com&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://bitcoinmagazine.com/",
        "type": "feed",
    },
    {
        "source_id": "crypto:decrypt",
        "source_name": "Decrypt",
        "feed_url": "https://decrypt.co/feed",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Adecrypt.co+crypto&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://decrypt.co/",
        "type": "feed",
    },
    {
        "source_id": "crypto:theblock",
        "source_name": "The Block",
        "feed_url": "https://news.google.com/rss/search?q=site%3Atheblock.co&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.theblock.co/",
        "type": "feed",
    },
    {
        "source_id": "crypto:cryptoslate",
        "source_name": "CryptoSlate",
        "feed_url": "https://cryptoslate.com/feed/",
        "fallback_feed_url": "https://news.google.com/rss/search?q=site%3Acryptoslate.com%20cryptocurrency&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://cryptoslate.com/",
        "type": "feed",
    },
]


log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "crypto_service.log")
handler = RotatingFileHandler(log_path, maxBytes=1024000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[handler],
)
logging.info("Crypto service started.")


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
