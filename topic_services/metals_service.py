import logging
from logging.handlers import RotatingFileHandler
import os

from base_topic_service import run_topic_service


HOST = "127.0.0.1"
PORT = 8794
SECTION_NAME = "Metals"
HEADLINES_PER_SOURCE = 8
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 20

SOURCES = [
    {
        "source_id": "metals:mining",
        "source_name": "Mining.com",
        "feed_url": "https://www.mining.com/feed/",
        "site_url": "https://www.mining.com/",
        "type": "feed",
    },
    {
        "source_id": "metals:kitco-direct",
        "source_name": "Kitco News (Google RSS)",
        "feed_url": "https://news.google.com/rss/search?q=site%3Akitco.com+gold+silver&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.kitco.com/",
        "type": "feed",
    },
    {
        "source_id": "metals:bullionvault",
        "source_name": "BullionVault (Google RSS)",
        "feed_url": "https://news.google.com/rss/search?q=site%3Abullionvault.com+gold+news&hl=en-US&gl=US&ceid=US%3Aen",
        "site_url": "https://www.bullionvault.com/",
        "type": "feed",
    },
    {
        "source_id": "metals:precious-metals-gnews",
        "source_name": "Precious Metals (Google News)",
        "feed_url": "https://news.google.com/rss/search?q=gold+silver+precious+metals+price&hl=en-US&gl=US&ceid=US:en",
        "site_url": "https://news.google.com/",
        "type": "feed",
    },
]


log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_path = os.path.join(log_dir, "metals_service.log")
handler = RotatingFileHandler(log_path, maxBytes=1024000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[handler],
)
logging.info("Metals service started.")


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
