from concurrent.futures import ThreadPoolExecutor, TimeoutError, as_completed
from datetime import datetime
from email.utils import parsedate_to_datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import threading
import time
from urllib.parse import parse_qs, unquote, urlparse
import xml.etree.ElementTree as ET

import requests


def get_text(element, path, default=""):
    node = element.find(path)
    if node is None or node.text is None:
        return default
    return node.text.strip()


def normalize_title(title):
    cleaned = " ".join(title.replace("’", "'").replace("‘", "'").split())
    return cleaned.casefold()


def _normalize_published_time(raw_value):
    if not raw_value:
        return ""
    text = str(raw_value).strip()
    if not text:
        return ""

    try:
        parsed = parsedate_to_datetime(text)
        if parsed is not None:
            return parsed.isoformat()
    except Exception:
        pass

    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return parsed.isoformat()
    except Exception:
        return text


def parse_rss_or_atom(xml_bytes):
    headlines = []
    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError:
        return headlines

    for item in root.findall(".//item"):
        title = get_text(item, "title")
        link = get_text(item, "link")
        if not link:
            guid = get_text(item, "guid")
            if guid and guid.startswith("http"):
                link = guid
        published = get_text(item, "pubDate") or get_text(item, "dc:date")
        if title and link:
            headlines.append(
                {
                    "title": title,
                    "link": link,
                    "published_at": _normalize_published_time(published),
                }
            )

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
        published = get_text(entry, "{*}updated") or get_text(entry, "{*}published")
        if title and link:
            headlines.append(
                {
                    "title": title,
                    "link": link,
                    "published_at": _normalize_published_time(published),
                }
            )
    return headlines


def dedupe_headlines(headlines, limit=5):
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
        unique.append(
            {
                "title": title,
                "link": link,
                "published_at": str(item.get("published_at", "")).strip(),
            }
        )
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


def run_topic_service(
    section_name,
    port,
    sources,
    headlines_per_source=5,
    request_timeout=12,
    cache_ttl_seconds=45,
    section_build_timeout=16,
    host="127.0.0.1",
):
    user_agent = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) TradersView/1.0"
    }
    cache_lock = threading.Lock()
    source_cache = {}

    def fetch_source_headlines(feed_url):
        try:
            response = requests.get(
                feed_url,
                headers=user_agent,
                timeout=request_timeout,
                allow_redirects=True,
            )
            response.raise_for_status()
            return dedupe_headlines(
                parse_rss_or_atom(response.content),
                limit=headlines_per_source,
            ), None
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
                "section": section_name,
                "service": "topic-service",
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }

        mode = "feed"
        headlines, feed_error = fetch_source_headlines(source["feed_url"])

        fallback_feed_url = source.get("fallback_feed_url")
        if not headlines and fallback_feed_url:
            fallback_headlines, fallback_error = fetch_source_headlines(fallback_feed_url)
            if fallback_headlines:
                headlines = fallback_headlines
                feed_error = None
                mode = "fallback"
            elif not feed_error:
                feed_error = fallback_error

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
            "section": section_name,
            "service": "topic-service",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
        }

    def get_source_payload(source, force_refresh=False):
        source_id = source["source_id"]
        now = time.time()
        with cache_lock:
            entry = source_cache.get(source_id)
            if entry and not force_refresh and (now - entry["epoch"]) < cache_ttl_seconds:
                payload = dict(entry["payload"])
                payload["cache"] = "hit"
                payload["cache_age_seconds"] = round(now - entry["epoch"], 2)
                return payload

        payload = collect_source_data(source)
        payload["cache"] = "miss"
        payload["cache_age_seconds"] = 0
        with cache_lock:
            source_cache[source_id] = {"payload": dict(payload), "epoch": now}
        return payload

    def get_stale_payload(source):
        source_id = source["source_id"]
        now = time.time()
        with cache_lock:
            entry = source_cache.get(source_id)
            if entry:
                payload = dict(entry["payload"])
                payload["cache"] = "stale"
                payload["cache_age_seconds"] = round(now - entry["epoch"], 2)
                payload["error"] = payload.get("error") or "Serving stale data due to source timeout"
                return payload
        
        is_link = source.get("type") == "link"
        return {
            "source_id": source["source_id"],
            "source_name": source["source_name"],
            "source_url": source.get("url") if is_link else source.get("feed_url"),
            "type": "link" if is_link else "feed",
            "headlines": [],
            "mode": "link" if is_link else "feed",
            "error": "Link source (no headlines)" if is_link else "Source timed out",
            "section": section_name,
            "service": "topic-service",
            "updated_at": datetime.now().isoformat(timespec="seconds"),
            "cache": "miss",
            "cache_age_seconds": 0,
        }

    def build_section_payload(force_refresh=False):
        max_workers = max(1, len(sources))
        payload_by_source_id = {}
        executor = ThreadPoolExecutor(max_workers=max_workers)
        try:
            future_map = {
                executor.submit(get_source_payload, source, force_refresh): source
                for source in sources
            }
            try:
                for future in as_completed(future_map, timeout=section_build_timeout):
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

        rows = [payload_by_source_id[source["source_id"]] for source in sources]
        return {
            "service": "topic-service",
            "section": section_name,
            "sources": rows,
            "total_headlines": sum(len(item["headlines"]) for item in rows),
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
            refresh_flag = parse_qs(parsed.query).get("refresh", ["0"])[0].strip().lower() in (
                "1",
                "true",
                "yes",
            )

            if parsed.path == "/health":
                return self._send_json({"ok": True, "service": "topic-service", "section": section_name})

            if parsed.path == "/data":
                return self._send_json(build_section_payload(force_refresh=refresh_flag))

            if parsed.path.startswith("/source/"):
                source_id = unquote(parsed.path[len("/source/"):])
                source = next((item for item in sources if item["source_id"] == source_id), None)
                if not source:
                    return self._send_json({"error": "Unknown source"}, status=404)
                return self._send_json(get_source_payload(source, force_refresh=refresh_flag))

            return self._send_json({"error": "Not Found"}, status=404)

        def log_message(self, _format, *_args):
            return

    server = ThreadingHTTPServer((host, port), TopicHandler)
    print(f"topic-service::{section_name} running at http://{host}:{port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
