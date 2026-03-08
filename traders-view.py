# --- Security Headers Helper ---
def _set_security_headers(handler):
  handler.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; object-src 'none';")
  handler.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
  handler.send_header("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
  handler.send_header("X-Content-Type-Options", "nosniff")
  handler.send_header("X-Frame-Options", "DENY")
  handler.send_header("Cross-Origin-Resource-Policy", "same-origin")
from http.server import BaseHTTPRequestHandler, HTTPServer
import base64
import binascii
from datetime import datetime
import hmac
import json
import os
from pathlib import Path
import threading
from urllib.parse import quote, unquote, urlparse
from urllib.error import URLError
from urllib.request import Request, urlopen
from html import escape

from service_discovery import build_service_urls, discover_topic_services


HOST = "127.0.0.1"
PORT = 8787
HEALTH_TIMEOUT = 3
ROOT = Path(__file__).resolve().parent

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
COINBASE_BTC_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
COINBASE_ETH_SPOT_URL = "https://api.coinbase.com/v2/prices/ETH-USD/spot"
FRANKFURTER_USD_SGD_URL = "https://api.frankfurter.app/latest?from=USD&to=SGD"
OPEN_ER_USD_URL = "https://open.er-api.com/v6/latest/USD"
STOOQ_CSV_URL_TEMPLATE = "https://stooq.com/q/l/?s={symbol}&i=d"
YAHOO_CHART_URL_TEMPLATE = "https://query1.finance.yahoo.com/v8/finance/chart/{symbol}?interval=1d&range=5d"
GOLD_API_URL_TEMPLATE = "https://api.gold-api.com/price/{symbol}"
ALLOW_FUTURES_METALS_FALLBACK = os.environ.get("TRADERS_VIEW_ALLOW_FUTURES_METALS", "0").strip() == "1"

_market_cache_lock = threading.Lock()
_market_last_good_metrics = {
  "silver_spot_usd_oz": None,
  "gold_spot_usd_oz": None,
  "btc_usd": None,
  "eth_usd": None,
  "usd_sgd": None,
  "sp500": None,
}


AUTH_USERNAME = os.environ.get("TRADERS_VIEW_USER", "").strip()
AUTH_PASSWORD = os.environ.get("TRADERS_VIEW_PASS", "").strip()
AUTH_ENABLED = os.environ.get("TRADERS_VIEW_AUTH_ENABLED", "1").strip() != "0"

if AUTH_ENABLED and (not AUTH_USERNAME or not AUTH_PASSWORD):
  raise RuntimeError(
    "Auth is enabled, but TRADERS_VIEW_USER/TRADERS_VIEW_PASS are not set. "
    "Set environment variables or set TRADERS_VIEW_AUTH_ENABLED=0 for local-only testing."
  )


def section_slug(section_name):
  return section_name.strip().lower().replace(" ", "-")


def _safe_float(raw_value):
  try:
    if raw_value is None:
      return None
    return float(str(raw_value).strip())
  except (TypeError, ValueError):
    return None


def _fetch_json(url, timeout=10):
  request = Request(url, headers={"User-Agent": "TradersViewGateway/1.0"})
  with urlopen(request, timeout=timeout) as response:
    return json.loads(response.read().decode("utf-8", errors="replace"))


def _fetch_stooq_close(symbol, timeout=10):
  encoded_symbol = quote(symbol, safe="")
  url = STOOQ_CSV_URL_TEMPLATE.format(symbol=encoded_symbol)
  request = Request(url, headers={"User-Agent": "TradersViewGateway/1.0"})
  with urlopen(request, timeout=timeout) as response:
    raw = response.read().decode("utf-8", errors="replace").strip()

  if not raw:
    return None

  line = raw.splitlines()[-1]
  cols = [item.strip() for item in line.split(",")]
  if len(cols) < 7:
    return None
  return _safe_float(cols[6])


def _fetch_yahoo_last_close(symbol, timeout=10):
  encoded_symbol = quote(symbol, safe="")
  url = YAHOO_CHART_URL_TEMPLATE.format(symbol=encoded_symbol)
  payload = _fetch_json(url, timeout=timeout)

  chart = (payload or {}).get("chart") or {}
  result = chart.get("result") or []
  if not result:
    return None

  first = result[0] if isinstance(result[0], dict) else {}
  indicators = first.get("indicators") or {}
  quote_rows = indicators.get("quote") or []
  if not quote_rows:
    return None

  closes = (quote_rows[0] or {}).get("close") or []
  for raw in reversed(closes):
    value = _safe_float(raw)
    if value is not None:
      return value
  return None


def _is_reasonable_metal_price(symbol, value):
  if value is None:
    return False
  if symbol == "XAU":
    return 300.0 <= value <= 20000.0
  if symbol == "XAG":
    return 3.0 <= value <= 500.0
  return False


def _fetch_gold_api_spot(symbol, timeout=10):
  url = GOLD_API_URL_TEMPLATE.format(symbol=quote(symbol, safe=""))
  payload = _fetch_json(url, timeout=timeout)
  price = _safe_float((payload or {}).get("price"))
  if _is_reasonable_metal_price(symbol, price):
    return price
  return None


def _fetch_metals_prices():
  try:
    gold = _fetch_stooq_close("xauusd")
    silver = _fetch_stooq_close("xagusd")
    if gold is not None and silver is not None:
      return gold, silver, "Stooq"
  except Exception:
    pass

  try:
    gold = _fetch_gold_api_spot("XAU")
    silver = _fetch_gold_api_spot("XAG")
    if gold is not None and silver is not None:
      return gold, silver, "Gold-API"
  except Exception:
    pass

  if ALLOW_FUTURES_METALS_FALLBACK:
    try:
      gold = _fetch_yahoo_last_close("GC=F")
      silver = _fetch_yahoo_last_close("SI=F")
      if gold is not None and silver is not None:
        return gold, silver, "Yahoo Finance Futures"
    except Exception:
      pass

  return None, None, None


def _fetch_usd_sgd_rate():
  try:
    fx = _fetch_json(FRANKFURTER_USD_SGD_URL)
    value = _safe_float(((fx or {}).get("rates") or {}).get("SGD"))
    if value is not None:
      return value, "Frankfurter"
  except Exception:
    pass

  try:
    fx = _fetch_json(OPEN_ER_USD_URL)
    value = _safe_float(((fx or {}).get("rates") or {}).get("SGD"))
    if value is not None:
      return value, "Open Exchange Rates (open.er-api)"
  except Exception:
    pass

  try:
    value = _fetch_stooq_close("usdsgd")
    if value is not None:
      return value, "Stooq"
  except Exception:
    pass

  return None, None


def _fetch_crypto_prices():
  try:
    crypto = _fetch_json(COINGECKO_SIMPLE_PRICE_URL)
    btc = _safe_float(((crypto or {}).get("bitcoin") or {}).get("usd"))
    eth = _safe_float(((crypto or {}).get("ethereum") or {}).get("usd"))
    if btc is not None and eth is not None:
      return btc, eth, "CoinGecko"
  except Exception:
    pass

  try:
    btc_payload = _fetch_json(COINBASE_BTC_SPOT_URL)
    eth_payload = _fetch_json(COINBASE_ETH_SPOT_URL)
    btc = _safe_float(((btc_payload or {}).get("data") or {}).get("amount"))
    eth = _safe_float(((eth_payload or {}).get("data") or {}).get("amount"))
    if btc is not None and eth is not None:
      return btc, eth, "Coinbase"
  except Exception:
    pass

  try:
    btc = _fetch_stooq_close("btcusd")
    eth = _fetch_stooq_close("ethusd")
    if btc is not None and eth is not None:
      return btc, eth, "Stooq"
  except Exception:
    pass

  return None, None, None


def build_market_snapshot():
  snapshot = {
    "updated_at": None,
    "metrics": {
      "silver_spot_usd_oz": None,
      "gold_spot_usd_oz": None,
      "btc_usd": None,
      "eth_usd": None,
      "usd_sgd": None,
      "sp500": None,
    },
    "sources": {
      "metals": "Stooq",
      "crypto": "CoinGecko",
      "fx": "Frankfurter",
      "indices": "Stooq",
    },
  }

  gold_value, silver_value, metals_source = _fetch_metals_prices()
  snapshot["metrics"]["gold_spot_usd_oz"] = gold_value
  snapshot["metrics"]["silver_spot_usd_oz"] = silver_value
  if metals_source:
    snapshot["sources"]["metals"] = metals_source

  try:
    # Use Yahoo Finance as primary source for S&P 500
    sp500_value = _fetch_yahoo_last_close("^GSPC")
    snapshot["sources"]["indices"] = "Yahoo Finance"
    if sp500_value is None:
      # Fallback to Stooq if Yahoo fails
      sp500_value = _fetch_stooq_close("^spx")
      if sp500_value is not None:
        snapshot["sources"]["indices"] = "Stooq"
    snapshot["metrics"]["sp500"] = sp500_value
  except Exception as e:
    print("[ERROR] S&P 500 fetch failed:", e, flush=True)

  btc_value, eth_value, crypto_source = _fetch_crypto_prices()
  snapshot["metrics"]["btc_usd"] = btc_value
  snapshot["metrics"]["eth_usd"] = eth_value
  if crypto_source:
    snapshot["sources"]["crypto"] = crypto_source

  usd_sgd_value, usd_sgd_source = _fetch_usd_sgd_rate()
  snapshot["metrics"]["usd_sgd"] = usd_sgd_value
  if usd_sgd_source:
    snapshot["sources"]["fx"] = usd_sgd_source

  with _market_cache_lock:
    for metric_key, metric_value in snapshot["metrics"].items():
      if metric_value is None and _market_last_good_metrics.get(metric_key) is not None:
        snapshot["metrics"][metric_key] = _market_last_good_metrics[metric_key]
      elif metric_value is not None:
        _market_last_good_metrics[metric_key] = metric_value

  snapshot["updated_at"] = datetime.utcnow().isoformat() + "Z"
  return snapshot


def render_page():
  service_urls = build_service_urls(ROOT, HOST)
  section_names = list(service_urls.keys())
  service_endpoints = {
    section: f"/api/section/{section_slug(section)}"
    for section in section_names
  }
  services_json = json.dumps(service_endpoints)

  nav = "".join(
    f"<a href='#{section_slug(name)}'>{name}</a>"
    for name in section_names
  )

  section_shells = "".join(
    (
      f"<section id='{section_slug(name)}' class='section-block'>"
      f"<div class='section-head'><h2>{name}</h2><span id='count-{section_slug(name)}'>0 sources</span></div>"
      f"<div id='grid-{section_slug(name)}' class='card-grid'>"
      "<article class='card placeholder'><p>Loading service...</p></article>"
      "</div>"
      "</section>"
    )
    for name in section_names
  )

  return f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Traders View</title>
  <style>
    :root {{
      --bg: #f3f5fa;
      --surface: #fff;
      --text: #161b2e;
      --muted: #64708d;
      --line: #dce3f2;
      --accent: #2f6df6;
      --shadow: 0 12px 30px rgba(17, 30, 62, 0.1);
    }}
    * {{ box-sizing: border-box; }}
    body {{ margin: 0; background: radial-gradient(circle at top right, #e6edff 0, #f3f5fa 42%, #f3f5fa 100%); color: var(--text); font-family: Inter, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; }}
    .shell {{ max-width: 1260px; margin: 0 auto; padding: 18px 20px 26px; }}
    .header {{ background: linear-gradient(145deg, #0b1632, #142653); color: #fff; border-radius: 16px; padding: 16px 20px; box-shadow: var(--shadow); }}
    .header-top {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; }}
    .brand-title {{ font-size: 25px; font-weight: 820; letter-spacing: -0.35px; }}
    .brand-sub {{ font-size: 12px; color: #bfcbf5; letter-spacing: 0.22px; text-transform: uppercase; }}
    .refresh {{ border: 1px solid rgba(255,255,255,0.34); background: rgba(255,255,255,0.08); color: #fff; border-radius: 10px; padding: 8px 14px; font-size: 13px; font-weight: 600; cursor: pointer; }}
    .refresh:hover {{ background: rgba(255,255,255,0.2); }}
    .refresh:disabled {{ opacity: 0.65; cursor: not-allowed; }}
    .market-strip {{ margin-top: 12px; display: grid; grid-template-columns: repeat(6, minmax(0, 1fr)); gap: 8px; }}
    .market-tile {{ background: rgba(0,0,0,0.24); border: 1px solid rgba(210,224,255,0.24); border-radius: 10px; padding: 7px 8px; }}
    .market-label {{ font-size: 10px; letter-spacing: 0.35px; text-transform: uppercase; color: #c8d5ff; font-weight: 700; margin-bottom: 2px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .market-value {{ font-size: 16px; font-weight: 800; letter-spacing: -0.2px; color: #ffffff; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .market-value.market-up {{ color: #8cffb9; }}
    .market-value.market-down {{ color: #ff9fb0; }}
    .market-value.market-flat {{ color: #d8e2ff; }}
    .nav {{ display: flex; flex-wrap: wrap; gap: 8px; margin-top: 12px; }}
    .nav a {{ text-decoration: none; font-size: 11px; letter-spacing: 0.36px; text-transform: uppercase; font-weight: 760; color: #d9e2ff; padding: 6px 11px; border: 1px solid rgba(217,226,255,0.34); border-radius: 999px; }}
    .nav a:hover {{ background: rgba(255,255,255,0.16); color: #fff; }}
    .stats {{ display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 12px; margin: 14px 0 20px; }}
    .stat {{ background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 13px 15px; box-shadow: 0 6px 16px rgba(13, 26, 60, 0.04); }}
    .stat-label {{ margin: 0 0 4px; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.4px; font-weight: 700; }}
    .stat-value {{ margin: 0; font-size: 30px; font-weight: 820; letter-spacing: -0.75px; color: var(--text); }}
    .section-block {{ margin-bottom: 20px; }}
    .section-head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin-bottom: 9px; border-bottom: 1px solid #dbe3f1; padding-bottom: 8px; }}
    .section-head h2 {{ margin: 0; font-size: 25px; letter-spacing: -0.55px; font-weight: 820; }}
    .section-head span {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.35px; font-weight: 700; }}
    .card-grid {{ display: grid; grid-template-columns: repeat(2, minmax(0,1fr)); gap: 12px; }}
    .card {{ background: var(--surface); border: 1px solid var(--line); border-radius: 14px; padding: 14px 15px 12px; box-shadow: 0 10px 24px rgba(16, 30, 66, 0.06); transition: transform 140ms ease, box-shadow 140ms ease; }}
    .card:hover {{ transform: translateY(-2px); box-shadow: 0 14px 26px rgba(16, 30, 66, 0.1); }}
    .card-head {{ display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 10px; }}
    .card-head-right {{ display: flex; align-items: center; gap: 8px; }}
    .source-name {{ margin: 0; font-size: 18px; line-height: 1.18; letter-spacing: -0.28px; font-weight: 770; }}
    .source-name a {{ text-decoration: none; color: inherit; }}
    .source-name a:hover {{ text-decoration: underline; }}
    .mode-badge {{ font-size: 10px; text-transform: uppercase; letter-spacing: 0.5px; border-radius: 999px; padding: 4px 8px; font-weight: 700; border: 1px solid transparent; white-space: nowrap; }}
    .mode-feed {{ background: #e9fbf1; color: #166534; border-color: #bce6ce; }}
    .mode-page {{ background: #edf3ff; color: #1e4eb5; border-color: #cddcff; }}
    .card-refresh {{ border: 1px solid #c7d2ec; background: #f7f9ff; color: #2b3f73; border-radius: 8px; padding: 4px 8px; font-size: 11px; font-weight: 700; cursor: pointer; }}
    .card-refresh:hover {{ background: #edf2ff; }}
    .card-refresh:disabled {{ opacity: 0.6; cursor: not-allowed; }}
    .headline-list {{ list-style: none; margin: 0; padding: 0; }}
    .headline-list li {{ padding: 8px 0; border-top: 1px solid #edf1fa; }}
    .headline-list li:first-child {{ border-top: 0; padding-top: 2px; }}
    .headline-list a {{ text-decoration: none; color: #1a2442; font-size: 14px; line-height: 1.35; font-weight: 550; display: -webkit-box; -webkit-line-clamp: 2; line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .headline-list a:hover {{ color: var(--accent); }}
    .placeholder, .service-error {{ display: grid; place-items: center; min-height: 120px; color: var(--muted); }}
    .service-error strong {{ color: #a11; }}
    @media (max-width: 1200px) {{ .market-strip {{ grid-template-columns: repeat(3, minmax(0, 1fr)); }} }}
    @media (max-width: 980px) {{ .stats {{ grid-template-columns: 1fr; }} .card-grid {{ grid-template-columns: 1fr; }} .section-head h2 {{ font-size: 23px; }} .market-strip {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }} }}
  </style>
</head>
<body>
  <div class='shell'>
    <header class='header'>
      <div class='header-top'>
        <div>
          <div class='brand-title'>Traders View</div>
          <div class='brand-sub'>Microservice-driven market dashboard</div>
        </div>
        <button id='refresh-btn' class='refresh' type='button'>Refresh</button>
      </div>
      <div id='market-strip' class='market-strip'>
        <div class='market-tile'><div class='market-label'>Silver Spot Price (USD/oz)</div><div id='mk-silver' class='market-value'>--</div></div>
        <div class='market-tile'><div class='market-label'>Gold Spot Price (USD/oz)</div><div id='mk-gold' class='market-value'>--</div></div>
        <div class='market-tile'><div class='market-label'>BTC/USD</div><div id='mk-btc' class='market-value'>--</div></div>
        <div class='market-tile'><div class='market-label'>ETH/USD</div><div id='mk-eth' class='market-value'>--</div></div>
        <div class='market-tile'><div class='market-label'>USD/SGD Rate</div><div id='mk-usdsgd' class='market-value'>--</div></div>
        <div class='market-tile'><div class='market-label'>S&amp;P 500 Index</div><div id='mk-sp500' class='market-value'>--</div></div>
      </div>
      <nav class='nav'>{nav}</nav>
    </header>

    <section class='stats'>
      <article class='stat'><p class='stat-label'>Updated</p><p id='stat-updated' class='stat-value'>--:--</p></article>
      <article class='stat'><p class='stat-label'>Tracked Sources</p><p id='stat-sources' class='stat-value'>0</p></article>
      <article class='stat'><p class='stat-label'>Loaded Headlines</p><p id='stat-headlines' class='stat-value'>0</p></article>
    </section>

    <main>{section_shells}</main>
  </div>

  <script>
    const SERVICES = {services_json};

    function escapeHtml(value) {{
      return value
        .replaceAll('&', '&amp;')
        .replaceAll('<', '&lt;')
        .replaceAll('>', '&gt;')
        .replaceAll('"', '&quot;')
        .replaceAll("'", '&#039;');
    }}

    function sectionSlug(name) {{
      return name.trim().toLowerCase().replaceAll(' ', '-');
    }}

    function sourceDomId(sourceId, sourceName) {{
      const base = sourceId && sourceId.length ? sourceId : sourceName;
      return base.toLowerCase().replaceAll(/[^a-z0-9]+/g, '-').replaceAll(/^-+|-+$/g, '');
    }}

    function headlineHtmlBlock(source) {{
      if (!source.headlines || !source.headlines.length) {{
        return `<p>No headlines available right now.</p>`;
      }}
      return `<ul class="headline-list">${{source.headlines.map(item => `<li><a href="${{escapeHtml(item.link)}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(item.title)}}</a></li>`).join('')}}</ul>`;
    }}

    function sourceCardHtml(sectionName, source) {{
      const badgeClass = source.mode === 'page' ? 'mode-page' : 'mode-feed';
      const badgeText = source.mode === 'page' ? 'Page Fallback' : 'Live Feed';
      const sourceId = source.source_id || '';
      const domId = sourceDomId(sourceId, source.source_name || 'source');
      const headlineHtml = headlineHtmlBlock(source);

      return `
        <article class="card" id="card-${{escapeHtml(domId)}}">
          <div class="card-head">
            <h3 class="source-name"><a href="${{escapeHtml(source.source_url)}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(source.source_name)}}</a></h3>
            <div class="card-head-right">
              <span class="mode-badge ${{badgeClass}}">${{badgeText}}</span>
              <button class="card-refresh" type="button" data-section="${{escapeHtml(sectionName)}}" data-source-id="${{escapeHtml(sourceId)}}" data-source-name="${{escapeHtml(source.source_name)}}">Refresh</button>
            </div>
          </div>
          ${{headlineHtml}}
        </article>
      `;
    }}

    const REQUEST_TIMEOUT_MS = 22000;

    async function fetchJsonWithTimeout(url) {{
      const controller = new AbortController();
      const timeoutId = setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS);
      let response;
      try {{
        response = await fetch(url, {{
          cache: 'no-store',
          signal: controller.signal,
        }});
      }} catch (error) {{
        if (error.name === 'AbortError') {{
          throw new Error('request timeout');
        }}
        throw error;
      }} finally {{
        clearTimeout(timeoutId);
      }}

      if (!response.ok) {{
        throw new Error(`Service returned ${{response.status}}`);
      }}
      return response.json();
    }}

    function withRefreshFlag(url, forceRefresh) {{
      const target = new URL(url, window.location.origin);
      if (forceRefresh) {{
        target.searchParams.set('refresh', '1');
      }}
      return target.toString();
    }}

    async function fetchSectionWithOptions(sectionName, baseUrl, forceRefresh) {{
      const url = withRefreshFlag(`${{baseUrl}}/data`, forceRefresh);
      return fetchJsonWithTimeout(url);
    }}

    async function fetchSource(sectionName, sourceId, forceRefresh = true) {{
      const baseUrl = SERVICES[sectionName];
      if (!baseUrl) {{
        throw new Error('source endpoint not configured');
      }}
      const encoded = encodeURIComponent(sourceId);
      const url = `${{baseUrl}}/source/${{encoded}}`;
      return fetchJsonWithTimeout(withRefreshFlag(url, forceRefresh));
    }}

    function renderSection(sectionName, payload) {{
      const slug = sectionSlug(sectionName);
      const grid = document.getElementById(`grid-${{slug}}`);
      const count = document.getElementById(`count-${{slug}}`);

      grid.innerHTML = payload.sources.map(source => sourceCardHtml(sectionName, source)).join('');
      count.textContent = `${{payload.sources.length}} sources`;
    }}

    function renderSectionError(sectionName, message) {{
      const slug = sectionSlug(sectionName);
      const grid = document.getElementById(`grid-${{slug}}`);
      const count = document.getElementById(`count-${{slug}}`);
      grid.innerHTML = `<article class="card service-error"><p><strong>Service unavailable</strong><br>${{escapeHtml(message)}}</p></article>`;
      count.textContent = 'service offline';
    }}

    function renderSingleSourceCard(sectionName, source) {{
      const domId = sourceDomId(source.source_id || '', source.source_name || 'source');
      const currentCard = document.getElementById(`card-${{domId}}`);
      if (!currentCard) {{
        return;
      }}
      currentCard.outerHTML = sourceCardHtml(sectionName, source);
    }}

    function updateStats(totalSources, totalHeadlines) {{
      const now = new Date();
      document.getElementById('stat-updated').textContent = now.toLocaleTimeString([], {{ hour: '2-digit', minute: '2-digit' }});
      document.getElementById('stat-sources').textContent = String(totalSources);
      document.getElementById('stat-headlines').textContent = String(totalHeadlines);
    }}

    function fmt(value, digits = 2) {{
      if (value === null || value === undefined || Number.isNaN(Number(value))) {{
        return '--';
      }}
      return Number(value).toLocaleString(undefined, {{ minimumFractionDigits: digits, maximumFractionDigits: digits }});
    }}

    const MARKET_META = [
      {{ id: 'mk-silver', key: 'silver_spot_usd_oz', digits: 2 }},
      {{ id: 'mk-gold', key: 'gold_spot_usd_oz', digits: 2 }},
      {{ id: 'mk-btc', key: 'btc_usd', digits: 2 }},
      {{ id: 'mk-eth', key: 'eth_usd', digits: 2 }},
      {{ id: 'mk-usdsgd', key: 'usd_sgd', digits: 6 }},
      {{ id: 'mk-sp500', key: 'sp500', digits: 2 }},
    ];

    let previousMarketMetrics = null;

    function setMarketCell(id, text, trend = 'flat') {{
      const node = document.getElementById(id);
      if (node) {{
        node.textContent = text;
        node.classList.remove('market-up', 'market-down', 'market-flat');
        if (trend === 'up') {{
          node.classList.add('market-up');
        }} else if (trend === 'down') {{
          node.classList.add('market-down');
        }} else {{
          node.classList.add('market-flat');
        }}
      }}
    }}

    function marketTrend(previousValue, currentValue) {{
      if (previousValue === null || previousValue === undefined || currentValue === null || currentValue === undefined) {{
        return 'flat';
      }}
      const prev = Number(previousValue);
      const curr = Number(currentValue);
      if (Number.isNaN(prev) || Number.isNaN(curr)) {{
        return 'flat';
      }}
      if (curr > prev) {{
        return 'up';
      }}
      if (curr < prev) {{
        return 'down';
      }}
      return 'flat';
    }}

    function marketDisplayText(value, digits, trend) {{
      if (value === null || value === undefined || Number.isNaN(Number(value))) {{
        return 'n/a';
      }}
      const base = fmt(value, digits);
      if (trend === 'up') {{
        return `▲ ${{base}}`;
      }}
      if (trend === 'down') {{
        return `▼ ${{base}}`;
      }}
      return `• ${{base}}`;
    }}

    function renderMarketSnapshot(payload) {{
      const metrics = (payload && payload.metrics) ? payload.metrics : {{}};
      MARKET_META.forEach(item => {{
        const currentValue = metrics[item.key];
        const previousValue = previousMarketMetrics ? previousMarketMetrics[item.key] : null;
        const trend = marketTrend(previousValue, currentValue);
        setMarketCell(item.id, marketDisplayText(currentValue, item.digits, trend), trend);
      }});
      previousMarketMetrics = metrics;
    }}

    async function loadMarketSnapshot() {{
      try {{
        const payload = await fetchJsonWithTimeout('/api/market-snapshot');
        renderMarketSnapshot(payload);
      }} catch (_error) {{
        MARKET_META.forEach(item => setMarketCell(item.id, 'n/a', 'flat'));
      }}
    }}

    async function loadAll(forceRefresh = false) {{
      let totalSources = 0;
      let totalHeadlines = 0;

      const tasks = Object.entries(SERVICES).map(async ([sectionName, baseUrl]) => {{
        try {{
          const payload = await fetchSectionWithOptions(sectionName, baseUrl, forceRefresh);
          totalSources += payload.sources.length;
          totalHeadlines += payload.total_headlines;
          renderSection(sectionName, payload);
        }} catch (error) {{
          renderSectionError(sectionName, error.message || 'request failed');
        }}
      }});

      await Promise.all(tasks);
      updateStats(totalSources, totalHeadlines);
      await loadMarketSnapshot();
    }}

    const refreshBtn = document.getElementById('refresh-btn');
    const refreshDefaultText = 'Refresh';
    refreshBtn.addEventListener('click', async () => {{
      refreshBtn.disabled = true;
      refreshBtn.textContent = 'Refreshing...';
      try {{
        await loadAll(true);
      }} catch (_error) {{
        window.location.reload();
      }} finally {{
        refreshBtn.disabled = false;
        refreshBtn.textContent = refreshDefaultText;
      }}
    }});

    document.addEventListener('click', async (event) => {{
      const button = event.target.closest('.card-refresh');
      if (!button) {{
        return;
      }}

      const sourceId = button.getAttribute('data-source-id') || '';
      const sectionName = button.getAttribute('data-section') || '';
      const sourceName = button.getAttribute('data-source-name') || 'source';
      if (!sourceId || !sectionName) {{
        return;
      }}

      button.disabled = true;
      const originalText = button.textContent;
      button.textContent = '...';

      try {{
        const payload = await fetchSource(sectionName, sourceId, true);
        renderSingleSourceCard(sectionName, payload);
      }} catch (_error) {{
        const domId = sourceDomId(sourceId, sourceName);
        const card = document.getElementById(`card-${{domId}}`);
        if (card) {{
          card.querySelectorAll('.headline-list').forEach(node => node.remove());
          const empty = card.querySelector('p') || document.createElement('p');
          empty.textContent = 'Unable to refresh this source right now.';
          if (!empty.parentNode) {{
            card.appendChild(empty);
          }}
        }}
      }} finally {{
        button.disabled = false;
        button.textContent = originalText;
      }}
    }});

    loadAll(false).catch(() => {{
      window.location.reload();
    }});

    setInterval(() => {{
      loadMarketSnapshot();
    }}, 15000);
  </script>
</body>
</html>"""


def probe_health(url):
  try:
    with urlopen(url, timeout=HEALTH_TIMEOUT) as response:
      status_code = getattr(response, "status", 200)
      if status_code != 200:
        return {
          "status": "down",
          "detail": f"HTTP {status_code}",
        }

      payload_raw = response.read().decode("utf-8", errors="replace")
      detail = "ok"
      try:
        payload = json.loads(payload_raw)
        if isinstance(payload, dict):
          detail = payload.get("service") or payload.get("section") or payload.get("source_id") or "ok"
      except json.JSONDecodeError:
        detail = "ok"

      return {
        "status": "up",
        "detail": detail,
      }
  except URLError as exc:
    return {
      "status": "down",
      "detail": str(exc.reason),
    }
  except Exception as exc:
    return {
      "status": "down",
      "detail": str(exc),
    }


def _is_authorized(headers):
  if not AUTH_ENABLED:
    return True

  auth_value = headers.get("Authorization", "")
  if not auth_value.startswith("Basic "):
    return False

  encoded = auth_value.split(" ", 1)[1].strip()
  if not encoded:
    return False

  try:
    decoded = base64.b64decode(encoded).decode("utf-8")
  except (ValueError, UnicodeDecodeError, binascii.Error):
    return False

  if ":" not in decoded:
    return False

  username, password = decoded.split(":", 1)
  return hmac.compare_digest(username, AUTH_USERNAME) and hmac.compare_digest(password, AUTH_PASSWORD)


def _send_unauthorized(handler):
  payload = b"Authentication required"
  handler.send_response(401)
  handler.send_header("WWW-Authenticate", 'Basic realm="Traders View"')
  handler.send_header("Content-Type", "text/plain; charset=utf-8")
  handler.send_header("Content-Length", str(len(payload)))
  _set_security_headers(handler)
  handler.end_headers()
  handler.wfile.write(payload)


def build_service_registry():
  topic_services = discover_topic_services(ROOT)

  rows = [
    {
      "name": "ui-gateway",
      "role": "ui-gateway",
      "port": PORT,
      "health_url": f"http://{HOST}:{PORT}/health",
      "status": "up",
      "detail": "self",
    }
  ]

  for service in topic_services:
    section_name = service["section"]
    port = service["port"]
    rows.append(
      {
        "name": f"topic-service::{section_slug(section_name)}",
        "role": "topic-service",
        "section": section_name,
        "port": port,
        "health_url": f"http://{HOST}:{port}/health",
      }
    )

  for row in rows:
    if row["role"] == "ui-gateway":
      continue
    health = probe_health(row["health_url"])
    row["status"] = health["status"]
    row["detail"] = health["detail"]

  return rows


def _service_lookup_by_slug():
  service_urls = build_service_urls(ROOT, HOST)
  return {
    section_slug(section): {"section": section, "url": service_urls[section]}
    for section in service_urls
  }


def _proxy_topic_request(target_url):
  request = Request(target_url, headers={"User-Agent": "TradersViewGateway/1.0"})
  with urlopen(request, timeout=20) as response:
    status_code = getattr(response, "status", 200)
    body = response.read()
    content_type = response.headers.get("Content-Type", "application/json; charset=utf-8")
    return status_code, content_type, body


def _send_proxy_error(handler, status, message):
  payload = json.dumps({"error": message}).encode("utf-8")
  handler.send_response(status)
  handler.send_header("Content-Type", "application/json; charset=utf-8")
  handler.send_header("Content-Length", str(len(payload)))
  handler.send_header("Cache-Control", "no-store")
  _set_security_headers(handler)
  handler.end_headers()
  handler.wfile.write(payload)


def render_services_page(registry):
  up_count = sum(1 for item in registry if item["status"] == "up")
  down_count = len(registry) - up_count

  table_rows = []
  for item in registry:
    status_class = "up" if item["status"] == "up" else "down"
    section = item.get("section", "-")
    source_name = item.get("source_name", "-")
    table_rows.append(
      "<tr>"
      f"<td>{escape(item['name'])}</td>"
      f"<td>{escape(item['role'])}</td>"
      f"<td>{escape(section)}</td>"
      f"<td>{escape(source_name)}</td>"
      f"<td>{item['port']}</td>"
      f"<td><span class='badge {status_class}'>{escape(item['status'])}</span></td>"
      f"<td>{escape(str(item['detail']))}</td>"
      "</tr>"
    )

  rows_html = "".join(table_rows)

  return f"""<!doctype html>
<html lang='en'>
<head>
  <meta charset='utf-8'>
  <meta name='viewport' content='width=device-width, initial-scale=1'>
  <title>Traders View Service Registry</title>
  <style>
  body {{ margin: 0; font-family: Inter, 'Segoe UI', Arial, sans-serif; background: #f5f7fb; color: #1b2439; }}
  .wrap {{ max-width: 1180px; margin: 0 auto; padding: 20px; }}
  .top {{ background: #0f1b3d; color: #fff; border-radius: 12px; padding: 14px 16px; display: flex; align-items: center; justify-content: space-between; gap: 10px; flex-wrap: wrap; }}
  .title {{ margin: 0; font-size: 22px; font-weight: 800; letter-spacing: -0.3px; }}
  .links a {{ color: #cbd7ff; text-decoration: none; margin-left: 10px; font-size: 13px; font-weight: 700; }}
  .links a:hover {{ text-decoration: underline; }}
  .stats {{ margin: 12px 0; display: flex; gap: 10px; flex-wrap: wrap; }}
  .chip {{ background: #fff; border: 1px solid #d8dfef; border-radius: 999px; padding: 6px 10px; font-size: 12px; font-weight: 700; }}
  .chip.up {{ color: #1d7f4e; }}
  .chip.down {{ color: #b11a2b; }}
  table {{ width: 100%; border-collapse: collapse; background: #fff; border: 1px solid #d8dfef; border-radius: 12px; overflow: hidden; }}
  th, td {{ text-align: left; padding: 10px 12px; border-bottom: 1px solid #edf1fa; font-size: 13px; vertical-align: top; }}
  th {{ background: #f2f6ff; font-size: 12px; text-transform: uppercase; letter-spacing: 0.35px; color: #4f5f86; }}
  tr:last-child td {{ border-bottom: 0; }}
  .badge {{ display: inline-block; padding: 3px 8px; border-radius: 999px; text-transform: uppercase; font-size: 10px; font-weight: 800; letter-spacing: 0.4px; }}
  .badge.up {{ background: #e8f8ee; color: #1f7d4e; border: 1px solid #bce6ca; }}
  .badge.down {{ background: #ffecee; color: #b32134; border: 1px solid #f3c0c7; }}
  </style>
</head>
<body>
  <div class='wrap'>
  <div class='top'>
    <h1 class='title'>Service Registry</h1>
    <div class='links'><a href='/'>Dashboard</a><a href='/services.json'>JSON</a></div>
  </div>
  <div class='stats'>
    <span class='chip'>Total: {len(registry)}</span>
    <span class='chip up'>Up: {up_count}</span>
    <span class='chip down'>Down: {down_count}</span>
  </div>
  <table>
    <thead>
    <tr><th>Name</th><th>Role</th><th>Section</th><th>Source</th><th>Port</th><th>Status</th><th>Health Detail</th></tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
  </div>
</body>
</html>"""


class DashboardHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        if not _is_authorized(self.headers):
            _send_unauthorized(self)
            return

        parsed = urlparse(self.path)

        if parsed.path.startswith("/api/section/"):
            parts = parsed.path.split("/")
            if len(parts) < 5:
                _send_proxy_error(self, 404, "Invalid API route")
                return

            slug = parts[3]
            action = parts[4]
            service = _service_lookup_by_slug().get(slug)
            if not service:
                _send_proxy_error(self, 404, "Unknown section")
                return

            target_url = ""
            if action == "data":
                target_url = f"{service['url']}/data"
            elif action == "source" and len(parts) >= 6:
                source_id = unquote("/".join(parts[5:]))
                target_url = f"{service['url']}/source/{quote(source_id, safe='')}"
            else:
                _send_proxy_error(self, 404, "Invalid API action")
                return

            if parsed.query:
                target_url = f"{target_url}?{parsed.query}"

            try:
                status, content_type, body = _proxy_topic_request(target_url)
            except URLError:
                _send_proxy_error(self, 502, "Topic service unavailable")
                return
            except Exception:
                _send_proxy_error(self, 502, "Gateway proxy error")
                return

            self.send_response(status)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(body)
            return

        if parsed.path == "/health":
            payload = json.dumps({"ok": True, "service": "ui-gateway"}).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path.rstrip("/") == "/api/market-snapshot":
            payload = json.dumps(build_market_snapshot()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/services.json":
            payload = json.dumps(build_service_registry()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.send_header("Cache-Control", "no-store")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(payload)
            return

        if parsed.path == "/services":
            html = render_services_page(build_service_registry()).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(html)))
            self.send_header("Cache-Control", "no-store")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(html)
            return

        if parsed.path not in ("/", "/index.html"):
            self.send_response(404)
            self.send_header("Content-Type", "text/plain; charset=utf-8")
            _set_security_headers(self)
            self.end_headers()
            self.wfile.write(b"Not Found")
            return

        html = render_page().encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(html)))
        self.send_header("Cache-Control", "no-store")
        _set_security_headers(self)
        self.end_headers()
        self.wfile.write(html)

    def log_message(self, _format, *_args):
        return


def main():
    server = HTTPServer((HOST, PORT), DashboardHandler)
    print(f"Traders View UI running at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
