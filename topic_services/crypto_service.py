import logging
from logging.handlers import RotatingFileHandler
import os

log_dir = os.path.join(os.path.dirname(__file__), "logs")
log_path = os.path.join(log_dir, "crypto_service.log")
handler = RotatingFileHandler(log_path, maxBytes=1024000, backupCount=5)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[handler]
)

logging.info("Crypto service started.")
import logging
logging.basicConfig(filename='crypto_service.log', level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from urllib.parse import parse_qs, unquote, urlparse
import xml.etree.ElementTree as ET

import requests


HOST = "127.0.0.1"
PORT = 8793
SECTION_NAME = "Crypto"
HEADLINES_PER_SOURCE = 5
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 16

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TradersView/1.0"
}

# Only real RSS/Atom feeds are included as sources
SOURCES = [
    {
        "source_id": "crypto:coindesk",
        "source_name": "CoinDesk",
        "feed_url": "https://www.coindesk.com/arc/outboundfeeds/rss/",
        "site_url": "https://www.coindesk.com/",
        "type": "feed"
    },
    {
        "source_id": "crypto:cointelegraph",
        "source_name": "Cointelegraph",
        "feed_url": "https://cointelegraph.com/rss",
        "site_url": "https://cointelegraph.com/",
        "type": "feed"
    },
    {
        "source_id": "crypto:bitcoinmagazine",
        "source_name": "Bitcoin Magazine",
        "feed_url": "https://bitcoinmagazine.com/feed",
        "site_url": "https://bitcoinmagazine.com/",
        "type": "feed"
    }
]

_cache_lock = threading.Lock()
_source_cache = {}


def get_text(element, path, default=""):
    node = element.find(path)
    if node is None or node.text is None:
        return default
    return node.text.strip()


def parse_rss_or_atom(xml_bytes):
    headlines = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return headlines
    for item in root.findall(".//item"):
        title = get_text(item, "title")
        link = get_text(item, "link")
        # Fallback: If <link> is missing, try <guid> if it looks like a URL
        if not link:
            guid = get_text(item, "guid")
            if guid and guid.startswith("http"):
                link = guid
        if title and link:
            headlines.append({"title": title, "link": link})
    if headlines:
        return headlines
    for entry in root.findall(".//{*}entry"):
        title = get_text(entry, "{*}title")
        link = ""
        for link_node in entry.findall("{*}link"):
            href = link_node.attrib.get("href", "").strip()
            rel = link_node.attrib.get("rel", "").strip()
            if href and (not rel or rel == "alternate"):
                link = href
                break
        if title and link:
            headlines.append({"title": title, "link": link})
    return headlines


def normalize_title(title):
    cleaned = " ".join(title.replace("’", "'").replace("‘", "'").split())
    return cleaned.casefold()


def dedupe_headlines(headlines, limit=HEADLINES_PER_SOURCE):
    seen = set()
    unique = []
    for item in headlines:
        title = item.get("title", "").strip()
        link = item.get("link", "").strip()
        if not title or not link:
            continue
        key = normalize_title(title)
        if key in seen:
            continue
        seen.add(key)
        unique.append({"title": title, "link": link})
        if len(unique) >= limit:
            break
    return unique


def normalize_request_error(exc):
    text = str(exc).lower()
    if isinstance(exc, requests.HTTPError) and exc.response is not None:
        status = exc.response.status_code
        if status in (401, 403):
            return f"Source blocked ({status})"
        if status == 404:
            return "Feed/page not found (404)"
        if status == 429:
            return "Rate-limited by source (429)"
        if status >= 500:
            return f"Source server error ({status})"
        return f"HTTP error ({status})"
    if "timed out" in text or "connecttimeout" in text or "readtimeout" in text:
        return "Connection timed out"
    if "failed to establish a new connection" in text or "winerror 10061" in text:
        return "Connection refused by source"
    return "Source temporarily unavailable"


def fetch_source_headlines(feed_url):
    try:
        response = requests.get(feed_url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return dedupe_headlines(parse_rss_or_atom(response.content)), None
    except requests.RequestException as exc:
        return [], normalize_request_error(exc)


def collect_source_data(source):
    if source.get("type") == "link":
        return {
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "source_url": source.get("url"),
            "type": "link",
            "headlines": [],
            "mode": "link",
            "error": None,
            "section": SECTION_NAME,
            "service": "topic-service",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }
    headlines, feed_error = fetch_source_headlines(source["feed_url"])
    mode = "feed"
    error = None
    if not headlines:
        error = feed_error or "Feed unavailable"
    return {
        "source_id": source["source_id"],
        "source_name": source["source_name"],
        "source_url": source.get("site_url", source["feed_url"]),
        "type": "feed",
        "headlines": headlines,
        "mode": mode,
        "error": error,
        "section": SECTION_NAME,
        "service": "topic-service",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


def get_source_payload(source, force_refresh=False):
    source_id = source["source_id"]
    now = time.time()
    with _cache_lock:
        entry = _source_cache.get(source_id)
        if entry and not force_refresh and (now - entry["epoch"]) < CACHE_TTL_SECONDS:
            payload = dict(entry["payload"])
            payload["cache"] = "hit"
            payload["cache_age_seconds"] = round(now - entry["epoch"], 2)
            return payload

    payload = collect_source_data(source)
    payload["cache"] = "miss"
    payload["cache_age_seconds"] = 0
    with _cache_lock:
        _source_cache[source_id] = {"payload": dict(payload), "epoch": now}
    return payload


def get_stale_payload(source):
    source_id = source["source_id"]
    now = time.time()
    with _cache_lock:
        entry = _source_cache.get(source_id)
        if entry:
            payload = dict(entry["payload"])
            payload["cache"] = "stale"
            payload["cache_age_seconds"] = round(now - entry["epoch"], 2)
            payload["error"] = payload.get("error") or "Serving stale data due to source timeout"
            return payload
    if source.get("type") == "link":
        return {
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "source_url": source.get("url"),
            "type": "link",
            "headlines": [],
            "mode": "link",
            "error": "Link source (no headlines)",
            "section": SECTION_NAME,
            "service": "topic-service",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "cache": "miss",
            "cache_age_seconds": 0,
        }
    return {
        "source_id": source["source_id"],
        "source_name": source["source_name"],
        "source_url": source["feed_url"],
        "type": "feed",
        "headlines": [],
        "mode": "feed",
        "error": "Source timed out",
        "section": SECTION_NAME,
        "service": "topic-service",
        "updated_at": datetime.now().isoformat(timespec="seconds"),
        "cache": "miss",
        "cache_age_seconds": 0,
    }


def build_section_payload(force_refresh=False):
    max_workers = max(1, len(SOURCES))
    payload_by_source_id = {}

    executor = ThreadPoolExecutor(max_workers=max_workers)
    try:
        future_map = {
            executor.submit(get_source_payload, source, force_refresh): source
            for source in SOURCES
        }

        try:
            for future in as_completed(future_map, timeout=SECTION_BUILD_TIMEOUT):
                source = future_map[future]
                try:
                    payload_by_source_id[source["source_id"]] = future.result()
                except Exception:
                    payload_by_source_id[source["source_id"]] = get_stale_payload(source)
        except TimeoutError:
            pass

        for future, source in future_map.items():
            if source["source_id"] not in payload_by_source_id:
                future.cancel()
                payload_by_source_id[source["source_id"]] = get_stale_payload(source)
    finally:
        executor.shutdown(wait=False, cancel_futures=True)

    sources = [payload_by_source_id[source["source_id"]] for source in SOURCES]
    return {
        "service": "topic-service",
        "section": SECTION_NAME,
        "sources": sources,
        "total_headlines": sum(len(item["headlines"]) for item in sources),
        "updated_at": datetime.now().isoformat(timespec="seconds"),
    }


class TopicHandler(BaseHTTPRequestHandler):
    def _send_json(self, payload, status=200):
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        parsed = urlparse(self.path)
        refresh_flag = parse_qs(parsed.query).get("refresh", ["0"])[0].strip().lower() in ("1", "true", "yes")

        if parsed.path == "/health":
            return self._send_json({"ok": True, "service": "topic-service", "section": SECTION_NAME})

        if parsed.path == "/data":
            return self._send_json(build_section_payload(force_refresh=refresh_flag))

        if parsed.path.startswith("/source/"):
            source_id = unquote(parsed.path[len("/source/"):])
            source = next((item for item in SOURCES if item["source_id"] == source_id), None)
            if not source:
                return self._send_json({"error": "Unknown source"}, status=404)
            return self._send_json(get_source_payload(source, force_refresh=refresh_flag))

        return self._send_json({"error": "Not Found"}, status=404)

    def log_message(self, _format, *_args):
        return


def main():
    server = ThreadingHTTPServer((HOST, PORT), TopicHandler)
    print(f"topic-service::{SECTION_NAME} running at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
