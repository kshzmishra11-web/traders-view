import logging
from logging.handlers import RotatingFileHandler
import os

from base_topic_service import run_topic_service


HOST = "127.0.0.1"
PORT = 8791
SECTION_NAME = "Geopolitical"
HEADLINES_PER_SOURCE = 8
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 20

SOURCES = [
    {
        "source_id": "geopolitical:bbc-world",
        "source_name": "BBC World",
        "feed_url": "https://feeds.bbci.co.uk/news/world/rss.xml",
        "site_url": "https://www.bbc.com/news/world",
        "type": "feed",
    },
    {
        "source_id": "geopolitical:aljazeera",
        "source_name": "Al Jazeera",
        "feed_url": "https://www.aljazeera.com/xml/rss/all.xml",
        "site_url": "https://www.aljazeera.com/",
        "type": "feed",
    },
    {
        "source_id": "geopolitical:foreign-policy",
        "source_name": "Foreign Policy",
        "feed_url": "https://foreignpolicy.com/feed/",
        "site_url": "https://foreignpolicy.com/",
        "type": "feed",
    },
    {
        "source_id": "geopolitical:the-diplomat",
        "source_name": "The Diplomat",
        "feed_url": "https://thediplomat.com/feed/",
        "site_url": "https://thediplomat.com/",
        "type": "feed",
    },
    {
        "source_id": "geopolitical:geopolitical-futures",
        "source_name": "Geopolitical Futures",
        "feed_url": "https://geopoliticalfutures.com/feed/",
        "site_url": "https://geopoliticalfutures.com/",
        "type": "feed",
    },
    {
        "source_id": "geopolitical:un-news-global",
        "source_name": "UN News (Global)",
        "feed_url": "https://news.un.org/feed/subscribe/en/news/all/rss.xml",
        "site_url": "https://news.un.org/en/",
        "type": "feed",
    },
]


log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "geopolitical_service.log")
handler = RotatingFileHandler(log_path, maxBytes=1024000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[handler],
)
logging.info("Geopolitical service started.")


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
