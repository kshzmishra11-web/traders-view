from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import datetime
from html.parser import HTMLParser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from urllib.parse import parse_qs, unquote, urljoin, urlparse
import xml.etree.ElementTree as ET

import requests


HOST = "127.0.0.1"
PORT = 8800
SECTION_NAME = "New Section"
HEADLINES_PER_SOURCE = 5
REQUEST_TIMEOUT = 12
CACHE_TTL_SECONDS = 45
SECTION_BUILD_TIMEOUT = 16
ALLOW_PAGE_SCRAPING = False

USER_AGENT = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TradersView/1.0"
}

SOURCES = [
    {
        "source_id": "newsection:source-one",
        "source_name": "Source One",
        "feed_url": "https://example.com/feed",
        "site_url": "https://example.com/",
    },
    {
        "source_id": "newsection:source-two",
        "source_name": "Source Two",
        "feed_url": "https://example.org/feed",
        "site_url": "https://example.org/",
    },
]

_cache_lock = threading.Lock()
_source_cache = {}


class HeadlineAnchorParser(HTMLParser):
    def __init__(self):
        super().__init__()
        self.links = []
        self._href = None
        self._buffer = []

    def handle_starttag(self, tag, attrs):
        if tag.lower() != "a":
            return
        self._href = dict(attrs).get("href")
        self._buffer = []

    def handle_data(self, data):
        if self._href is not None:
            self._buffer.append(data)

    def handle_endtag(self, tag):
        if tag.lower() != "a" or self._href is None:
            return
        text = " ".join(part.strip() for part in self._buffer if part.strip()).strip()
        self.links.append({"title": text, "href": self._href})
        self._href = None
        self._buffer = []


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


def summarize_source_error(feed_error, page_error):
    feed_msg = feed_error or "No feed items"
    page_msg = page_error or "No page headlines"
    if feed_msg == page_msg:
        return feed_msg
    return f"Feed unavailable; page fallback unavailable ({page_msg.lower()})"


def looks_like_headline(text, href, source_domain):
    if not text or not href:
        return False
    if len(text) < 24 or len(text) > 220:
        return False
    href_lower = href.lower()
    if href_lower.startswith("javascript:") or href_lower.startswith("mailto:"):
        return False
    parsed = urlparse(href)
    if parsed.scheme and parsed.scheme not in ("http", "https"):
        return False
    if parsed.netloc and source_domain not in parsed.netloc.lower().replace("www.", ""):
        return False
    lowered = text.lower()
    blocked = ("cookie", "privacy", "subscribe", "sign in", "newsletter")
    return not any(token in lowered for token in blocked)


def fetch_source_headlines(feed_url):
    try:
        response = requests.get(feed_url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        return dedupe_headlines(parse_rss_or_atom(response.content)), None
    except requests.RequestException as exc:
        return [], normalize_request_error(exc)


def fetch_page_headlines(page_url):
    try:
        response = requests.get(page_url, headers=USER_AGENT, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        parser = HeadlineAnchorParser()
        parser.feed(response.text)
        base_domain = urlparse(page_url).netloc.lower().replace("www.", "")
        seen = set()
        out = []
        for link in parser.links:
            title = " ".join(link["title"].split())
            full = urljoin(page_url, link["href"]).strip()
            key = (title.lower(), full)
            if key in seen or not looks_like_headline(title, full, base_domain):
                continue
            seen.add(key)
            out.append({"title": title, "link": full})
            if len(out) >= HEADLINES_PER_SOURCE:
                break
        if out:
            return dedupe_headlines(out), None
        return [], "No headline links detected"
    except requests.RequestException as exc:
        return [], normalize_request_error(exc)


def collect_source_data(source):
    headlines, feed_error = fetch_source_headlines(source["feed_url"])
    mode = "feed"
    error = None
    allow_page_fallback = ALLOW_PAGE_SCRAPING and source.get("allow_page_fallback", False)

    if not headlines:
        if allow_page_fallback:
            page_headlines, page_error = fetch_page_headlines(source["site_url"])
            if page_headlines:
                headlines = page_headlines
                mode = "page"
                error = None
            else:
                error = summarize_source_error(feed_error, page_error)
        else:
            error = feed_error or "Feed unavailable (page scraping disabled for compliance)"

    return {
        "source_id": source["source_id"],
        "source_name": source["source_name"],
        "source_url": source.get("site_url", source["feed_url"]),
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
    return {
        "source_id": source["source_id"],
        "source_name": source["source_name"],
        "source_url": source["page_url"],
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
