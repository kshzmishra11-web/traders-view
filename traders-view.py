from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import base64
import binascii
from datetime import datetime, timezone
import hmac
import json
import os
from pathlib import Path
import threading
import time
from urllib.parse import parse_qs, quote, unquote, urlparse
from urllib.error import URLError
from urllib.request import Request, urlopen
from html import escape

from service_discovery import build_service_urls, discover_topic_services


# --- Security Headers Helper ---
def _set_security_headers(handler):
  handler.send_header("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; object-src 'none';")
  handler.send_header("Referrer-Policy", "strict-origin-when-cross-origin")
  handler.send_header("Strict-Transport-Security", "max-age=63072000; includeSubDomains; preload")
  handler.send_header("X-Content-Type-Options", "nosniff")
  handler.send_header("X-Frame-Options", "DENY")
  handler.send_header("Cross-Origin-Resource-Policy", "same-origin")


HOST = "127.0.0.1"
PORT = 8787
HEALTH_TIMEOUT = 3
ROOT = Path(__file__).resolve().parent

COINGECKO_SIMPLE_PRICE_URL = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,ripple,solana,binancecoin,cardano&vs_currencies=usd&include_24hr_change=true"
FNG_API_URL = "https://api.alternative.me/fng/?limit=1"
COINBASE_BTC_SPOT_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
COINBASE_ETH_SPOT_URL = "https://api.coinbase.com/v2/prices/ETH-USD/spot"
BINANCE_BTC_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=BTCUSDT"
BINANCE_ETH_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=ETHUSDT"
BINANCE_XRP_USDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=XRPUSDT"
BINANCE_SOL_USDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=SOLUSDT"
BINANCE_BNB_USDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BNBUSDT"
BINANCE_ADA_USDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=ADAUSDT"
BINANCE_XRP_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=XRPUSDT"
BINANCE_SOL_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=SOLUSDT"
BINANCE_BNB_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=BNBUSDT"
BINANCE_ADA_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=ADAUSDT"
BINANCE_DOGE_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=DOGEUSDT"
BINANCE_AVAX_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=AVAXUSDT"
BINANCE_LINK_24H_URL = "https://api.binance.com/api/v3/ticker/24hr?symbol=LINKUSDT"
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
  "platinum_spot_usd_oz": None,
  "palladium_spot_usd_oz": None,
  "btc_usd": None,
  "eth_usd": None,
  "xrp_usd": None,
  "sol_usd": None,
  "bnb_usd": None,
  "ada_usd": None,
  "doge_usd": None,
  "avax_usd": None,
  "link_usd": None,
  "btc_chg24": None,
  "eth_chg24": None,
  "xrp_chg24": None,
  "sol_chg24": None,
  "bnb_chg24": None,
  "ada_chg24": None,
  "doge_chg24": None,
  "avax_chg24": None,
  "link_chg24": None,
  "usd_sgd": None,
  "sp500": None,
  "nasdaq": None,
  "nikkei": None,
  "ftse": None,
  "dax": None,
  "hangseng": None,
  "vix": None,
  "wti": None,
  "dxy": None,
  "dow": None,
  "russell2000": None,
  "tnx": None,
  "fiveyr_yield": None,
  "thirtyyr_yield": None,
  "brent": None,
  "eurusd": None,
  "gbpusd": None,
  "audusd": None,
  "usdjpy": None,
  "fear_greed": None,
}
_market_snapshot_cache_lock = threading.Lock()
_market_snapshot_cache = {
  "epoch": 0.0,
  "payload": None,
}
MARKET_SNAPSHOT_TTL_SECONDS = max(
  5,
  int(os.environ.get("TRADERS_VIEW_MARKET_SNAPSHOT_TTL_SECONDS", "15").strip() or "15"),
)
NETWORK_TIMEOUT_SECONDS = max(
  2.0,
  float(os.environ.get("TRADERS_VIEW_NETWORK_TIMEOUT_SECONDS", "4.5").strip() or "4.5"),
)
TOPIC_PROXY_TIMEOUT_SECONDS = max(
  3.0,
  float(os.environ.get("TRADERS_VIEW_TOPIC_PROXY_TIMEOUT_SECONDS", "12").strip() or "12"),
)


AUTH_USERNAME = os.environ.get("TRADERS_VIEW_USER", "").strip()
AUTH_PASSWORD = os.environ.get("TRADERS_VIEW_PASS", "").strip()
AUTH_ENABLED = os.environ.get("TRADERS_VIEW_AUTH_ENABLED", "0").strip() != "0"

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


def _fetch_json(url, timeout=NETWORK_TIMEOUT_SECONDS):
  request = Request(url, headers={"User-Agent": "TradersViewGateway/1.0"})
  with urlopen(request, timeout=timeout) as response:
    return json.loads(response.read().decode("utf-8", errors="replace"))


def _fetch_stooq_close(symbol, timeout=NETWORK_TIMEOUT_SECONDS):
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


def _fetch_yahoo_last_close(symbol, timeout=NETWORK_TIMEOUT_SECONDS):
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


def _fetch_gold_api_spot(symbol, timeout=NETWORK_TIMEOUT_SECONDS):
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


def _fetch_fear_greed():
  try:
    payload = _fetch_json(FNG_API_URL)
    data = payload.get("data")
    if isinstance(data, list) and data:
      value = data[0].get("value")
      if value is not None:
        return int(value)
  except Exception:
    pass
  return None


def _fetch_dow():
  try:
    v = _fetch_yahoo_last_close("^DJI")
    if v is None:
      v = _fetch_stooq_close("^dji")
    return v
  except Exception:
    return None


def _fetch_tnx():
  try:
    return _fetch_yahoo_last_close("^TNX")
  except Exception:
    return None


def _fetch_brent():
  try:
    v = _fetch_yahoo_last_close("BZ=F")
    if v is None:
      v = _fetch_stooq_close("lcousd")
    return v
  except Exception:
    return None


def _fetch_eurusd():
  try:
    v = _fetch_yahoo_last_close("EURUSD=X")
    if v is None:
      v = _fetch_stooq_close("eurusd")
    return v
  except Exception:
    return None


def _fetch_usdjpy():
  try:
    v = _fetch_yahoo_last_close("JPY=X")
    if v is None:
      v = _fetch_stooq_close("usdjpy")
    return v
  except Exception:
    return None


def _fetch_russell2000():
  try:
    v = _fetch_yahoo_last_close("^RUT")
    if v is None:
      v = _fetch_stooq_close("^rut")
    return v
  except Exception:
    return None


def _fetch_platinum():
  try:
    v = _fetch_stooq_close("xptusd")
    if v is None:
      v = _fetch_yahoo_last_close("PL=F")
    return v
  except Exception:
    return None


def _fetch_palladium():
  try:
    v = _fetch_stooq_close("xpdusd")
    if v is None:
      v = _fetch_yahoo_last_close("PA=F")
    return v
  except Exception:
    return None


def _fetch_binance_24h_both(url):
  """Fetch (lastPrice, priceChangePercent) from a Binance 24hr ticker in one call."""
  try:
    payload = _fetch_json(url)
    price = _safe_float((payload or {}).get("lastPrice"))
    chg = _safe_float((payload or {}).get("priceChangePercent"))
    return price, chg
  except Exception:
    return None, None


def _fetch_doge():
  return _fetch_binance_24h_both(BINANCE_DOGE_24H_URL)


def _fetch_avax():
  return _fetch_binance_24h_both(BINANCE_AVAX_24H_URL)


def _fetch_link():
  return _fetch_binance_24h_both(BINANCE_LINK_24H_URL)


def _fetch_global_indices():
  nikkei = ftse = dax = hangseng = None
  try:
    nikkei = _fetch_yahoo_last_close("^N225")
  except Exception:
    pass
  try:
    ftse = _fetch_yahoo_last_close("^FTSE")
  except Exception:
    pass
  try:
    dax = _fetch_yahoo_last_close("^GDAXI")
  except Exception:
    pass
  try:
    hangseng = _fetch_yahoo_last_close("^HSI")
  except Exception:
    pass
  return nikkei, ftse, dax, hangseng


def _fetch_fiveyr_yield():
  try:
    return _fetch_yahoo_last_close("^FVX")
  except Exception:
    return None


def _fetch_thirtyyr_yield():
  try:
    return _fetch_yahoo_last_close("^TYX")
  except Exception:
    return None


def _fetch_gbpusd():
  try:
    v = _fetch_yahoo_last_close("GBPUSD=X")
    if v is None:
      v = _fetch_stooq_close("gbpusd")
    return v
  except Exception:
    return None


def _fetch_audusd():
  try:
    v = _fetch_yahoo_last_close("AUDUSD=X")
    if v is None:
      v = _fetch_stooq_close("audusd")
    return v
  except Exception:
    return None


def _fetch_binance_usdt_price(url):
  try:
    payload = _fetch_json(url)
    return _safe_float((payload or {}).get("price"))
  except Exception:
    return None


def _fetch_binance_24h_change(url):
  try:
    payload = _fetch_json(url)
    return _safe_float((payload or {}).get("priceChangePercent"))
  except Exception:
    return None


def _fetch_crypto_prices():
  try:
    crypto = _fetch_json(COINGECKO_SIMPLE_PRICE_URL)
    btc = _safe_float(((crypto or {}).get("bitcoin") or {}).get("usd"))
    eth = _safe_float(((crypto or {}).get("ethereum") or {}).get("usd"))
    xrp = _safe_float(((crypto or {}).get("ripple") or {}).get("usd"))
    sol = _safe_float(((crypto or {}).get("solana") or {}).get("usd"))
    bnb = _safe_float(((crypto or {}).get("binancecoin") or {}).get("usd"))
    ada = _safe_float(((crypto or {}).get("cardano") or {}).get("usd"))
    btc_chg24 = _safe_float(((crypto or {}).get("bitcoin") or {}).get("usd_24h_change"))
    eth_chg24 = _safe_float(((crypto or {}).get("ethereum") or {}).get("usd_24h_change"))
    xrp_chg24 = _safe_float(((crypto or {}).get("ripple") or {}).get("usd_24h_change"))
    sol_chg24 = _safe_float(((crypto or {}).get("solana") or {}).get("usd_24h_change"))
    bnb_chg24 = _safe_float(((crypto or {}).get("binancecoin") or {}).get("usd_24h_change"))
    ada_chg24 = _safe_float(((crypto or {}).get("cardano") or {}).get("usd_24h_change"))
    if btc is not None and eth is not None:
      return btc, eth, xrp, sol, bnb, ada, btc_chg24, eth_chg24, xrp_chg24, sol_chg24, bnb_chg24, ada_chg24, "CoinGecko"
  except Exception:
    pass

  try:
    btc_payload = _fetch_json(COINBASE_BTC_SPOT_URL)
    eth_payload = _fetch_json(COINBASE_ETH_SPOT_URL)
    btc = _safe_float(((btc_payload or {}).get("data") or {}).get("amount"))
    eth = _safe_float(((eth_payload or {}).get("data") or {}).get("amount"))
    xrp = _fetch_binance_usdt_price(BINANCE_XRP_USDT_URL)
    sol = _fetch_binance_usdt_price(BINANCE_SOL_USDT_URL)
    bnb = _fetch_binance_usdt_price(BINANCE_BNB_USDT_URL)
    ada = _fetch_binance_usdt_price(BINANCE_ADA_USDT_URL)
    btc_chg24 = _fetch_binance_24h_change(BINANCE_BTC_24H_URL)
    eth_chg24 = _fetch_binance_24h_change(BINANCE_ETH_24H_URL)
    xrp_chg24 = _fetch_binance_24h_change(BINANCE_XRP_24H_URL)
    sol_chg24 = _fetch_binance_24h_change(BINANCE_SOL_24H_URL)
    bnb_chg24 = _fetch_binance_24h_change(BINANCE_BNB_24H_URL)
    ada_chg24 = _fetch_binance_24h_change(BINANCE_ADA_24H_URL)
    if btc is not None and eth is not None:
      return btc, eth, xrp, sol, bnb, ada, btc_chg24, eth_chg24, xrp_chg24, sol_chg24, bnb_chg24, ada_chg24, "Coinbase+Binance"
  except Exception:
    pass

  try:
    btc = _fetch_stooq_close("btcusd")
    eth = _fetch_stooq_close("ethusd")
    xrp = _fetch_binance_usdt_price(BINANCE_XRP_USDT_URL)
    sol = _fetch_binance_usdt_price(BINANCE_SOL_USDT_URL)
    bnb = _fetch_binance_usdt_price(BINANCE_BNB_USDT_URL)
    ada = _fetch_binance_usdt_price(BINANCE_ADA_USDT_URL)
    btc_chg24 = _fetch_binance_24h_change(BINANCE_BTC_24H_URL)
    eth_chg24 = _fetch_binance_24h_change(BINANCE_ETH_24H_URL)
    xrp_chg24 = _fetch_binance_24h_change(BINANCE_XRP_24H_URL)
    sol_chg24 = _fetch_binance_24h_change(BINANCE_SOL_24H_URL)
    bnb_chg24 = _fetch_binance_24h_change(BINANCE_BNB_24H_URL)
    ada_chg24 = _fetch_binance_24h_change(BINANCE_ADA_24H_URL)
    if btc is not None and eth is not None:
      return btc, eth, xrp, sol, bnb, ada, btc_chg24, eth_chg24, xrp_chg24, sol_chg24, bnb_chg24, ada_chg24, "Stooq+Binance"
  except Exception:
    pass

  return None, None, None, None, None, None, None, None, None, None, None, None, None


def _fetch_indices_macro_snapshot():
  try:
    sp500_value = _fetch_yahoo_last_close("^GSPC")
    nasdaq_value = _fetch_yahoo_last_close("^IXIC")
    vix_value = _fetch_yahoo_last_close("^VIX")
    wti_value = _fetch_yahoo_last_close("CL=F")
    dxy_value = _fetch_yahoo_last_close("DX-Y.NYB")
    if dxy_value is None:
      dxy_value = _fetch_yahoo_last_close("DX=F")
    source = "Yahoo Finance"

    if nasdaq_value is None:
      nasdaq_value = _fetch_stooq_close("^ndq")
      if nasdaq_value is not None:
        source = "Stooq"
    if vix_value is None:
      vix_value = _fetch_stooq_close("^vix")
      if vix_value is not None:
        source = "Stooq"
    if wti_value is None:
      wti_value = _fetch_stooq_close("cl.f")
      if wti_value is not None:
        source = "Stooq"
    if dxy_value is None:
      dxy_value = _fetch_stooq_close("usdx")

    if sp500_value is None:
      sp500_value = _fetch_stooq_close("^spx")
      if sp500_value is not None:
        source = "Stooq"
    return sp500_value, nasdaq_value, vix_value, wti_value, dxy_value, source
  except Exception as exc:
    print("[ERROR] Index/macro fetch failed:", exc, flush=True)
    return None, None, None, None, None, None


def _build_market_snapshot_uncached():
  snapshot = {
    "updated_at": None,
    "metrics": {
      "silver_spot_usd_oz": None,
      "gold_spot_usd_oz": None,
      "platinum_spot_usd_oz": None,
      "palladium_spot_usd_oz": None,
      "btc_usd": None,
      "eth_usd": None,
      "xrp_usd": None,
      "sol_usd": None,
      "bnb_usd": None,
      "ada_usd": None,
      "doge_usd": None,
      "avax_usd": None,
      "link_usd": None,
      "btc_chg24": None,
      "eth_chg24": None,
      "xrp_chg24": None,
      "sol_chg24": None,
      "bnb_chg24": None,
      "ada_chg24": None,
      "doge_chg24": None,
      "avax_chg24": None,
      "link_chg24": None,
      "usd_sgd": None,
      "sp500": None,
      "nasdaq": None,
      "nikkei": None,
      "ftse": None,
      "dax": None,
      "hangseng": None,
      "vix": None,
      "wti": None,
      "dxy": None,
      "dow": None,
      "russell2000": None,
      "tnx": None,
      "fiveyr_yield": None,
      "thirtyyr_yield": None,
      "brent": None,
      "eurusd": None,
      "gbpusd": None,
      "audusd": None,
      "usdjpy": None,
      "fear_greed": None,
    },
    "sources": {
      "metals": "Stooq",
      "crypto": "CoinGecko",
      "fx": "Frankfurter",
      "indices": "Stooq",
      "fear_greed": "Alternative.me",
    },
  }

  with ThreadPoolExecutor(max_workers=22) as pool:
    futures = {
      "metals": pool.submit(_fetch_metals_prices),
      "indices_macro": pool.submit(_fetch_indices_macro_snapshot),
      "crypto": pool.submit(_fetch_crypto_prices),
      "fx": pool.submit(_fetch_usd_sgd_rate),
      "fng": pool.submit(_fetch_fear_greed),
      "dow": pool.submit(_fetch_dow),
      "russell2000": pool.submit(_fetch_russell2000),
      "tnx": pool.submit(_fetch_tnx),
      "brent": pool.submit(_fetch_brent),
      "eurusd": pool.submit(_fetch_eurusd),
      "usdjpy": pool.submit(_fetch_usdjpy),
      "platinum": pool.submit(_fetch_platinum),
      "palladium": pool.submit(_fetch_palladium),
      "doge": pool.submit(_fetch_doge),
      "avax": pool.submit(_fetch_avax),
      "link": pool.submit(_fetch_link),
      "global_indices": pool.submit(_fetch_global_indices),
      "fiveyr_yield": pool.submit(_fetch_fiveyr_yield),
      "thirtyyr_yield": pool.submit(_fetch_thirtyyr_yield),
      "gbpusd": pool.submit(_fetch_gbpusd),
      "audusd": pool.submit(_fetch_audusd),
    }

    gold_value, silver_value, metals_source = futures["metals"].result()
    sp500_value, nasdaq_value, vix_value, wti_value, dxy_value, indices_source = futures["indices_macro"].result()
    (
      btc_value,
      eth_value,
      xrp_value,
      sol_value,
      bnb_value,
      ada_value,
      btc_chg24,
      eth_chg24,
      xrp_chg24,
      sol_chg24,
      bnb_chg24,
      ada_chg24,
      crypto_source,
    ) = futures["crypto"].result()
    usd_sgd_value, usd_sgd_source = futures["fx"].result()
    fear_greed_value = futures["fng"].result()

  snapshot["metrics"]["gold_spot_usd_oz"] = gold_value
  snapshot["metrics"]["silver_spot_usd_oz"] = silver_value
  snapshot["metrics"]["platinum_spot_usd_oz"] = futures["platinum"].result()
  snapshot["metrics"]["palladium_spot_usd_oz"] = futures["palladium"].result()
  if metals_source:
    snapshot["sources"]["metals"] = metals_source

  snapshot["metrics"]["sp500"] = sp500_value
  snapshot["metrics"]["nasdaq"] = nasdaq_value
  snapshot["metrics"]["vix"] = vix_value
  snapshot["metrics"]["wti"] = wti_value
  snapshot["metrics"]["dxy"] = dxy_value
  if indices_source:
    snapshot["sources"]["indices"] = indices_source

  snapshot["metrics"]["btc_usd"] = btc_value
  snapshot["metrics"]["eth_usd"] = eth_value
  snapshot["metrics"]["xrp_usd"] = xrp_value
  snapshot["metrics"]["sol_usd"] = sol_value
  snapshot["metrics"]["bnb_usd"] = bnb_value
  snapshot["metrics"]["ada_usd"] = ada_value
  snapshot["metrics"]["btc_chg24"] = btc_chg24
  snapshot["metrics"]["eth_chg24"] = eth_chg24
  snapshot["metrics"]["xrp_chg24"] = xrp_chg24
  snapshot["metrics"]["sol_chg24"] = sol_chg24
  snapshot["metrics"]["bnb_chg24"] = bnb_chg24
  snapshot["metrics"]["ada_chg24"] = ada_chg24
  if crypto_source:
    snapshot["sources"]["crypto"] = crypto_source

  snapshot["metrics"]["usd_sgd"] = usd_sgd_value
  if usd_sgd_source:
    snapshot["sources"]["fx"] = usd_sgd_source

  snapshot["metrics"]["fear_greed"] = fear_greed_value
  snapshot["metrics"]["dow"] = futures["dow"].result()
  snapshot["metrics"]["russell2000"] = futures["russell2000"].result()
  snapshot["metrics"]["tnx"] = futures["tnx"].result()
  snapshot["metrics"]["brent"] = futures["brent"].result()
  snapshot["metrics"]["eurusd"] = futures["eurusd"].result()
  snapshot["metrics"]["usdjpy"] = futures["usdjpy"].result()
  doge_v, doge_c = futures["doge"].result()
  avax_v, avax_c = futures["avax"].result()
  link_v, link_c = futures["link"].result()
  snapshot["metrics"]["doge_usd"] = doge_v
  snapshot["metrics"]["doge_chg24"] = doge_c
  snapshot["metrics"]["avax_usd"] = avax_v
  snapshot["metrics"]["avax_chg24"] = avax_c
  snapshot["metrics"]["link_usd"] = link_v
  snapshot["metrics"]["link_chg24"] = link_c
  nikkei_v, ftse_v, dax_v, hangseng_v = futures["global_indices"].result()
  snapshot["metrics"]["nikkei"] = nikkei_v
  snapshot["metrics"]["ftse"] = ftse_v
  snapshot["metrics"]["dax"] = dax_v
  snapshot["metrics"]["hangseng"] = hangseng_v
  snapshot["metrics"]["fiveyr_yield"] = futures["fiveyr_yield"].result()
  snapshot["metrics"]["thirtyyr_yield"] = futures["thirtyyr_yield"].result()
  snapshot["metrics"]["gbpusd"] = futures["gbpusd"].result()
  snapshot["metrics"]["audusd"] = futures["audusd"].result()

  with _market_cache_lock:
    for metric_key, metric_value in snapshot["metrics"].items():
      if metric_value is None and _market_last_good_metrics.get(metric_key) is not None:
        snapshot["metrics"][metric_key] = _market_last_good_metrics[metric_key]
      elif metric_value is not None:
        _market_last_good_metrics[metric_key] = metric_value

  snapshot["updated_at"] = datetime.now(timezone.utc).isoformat()
  return snapshot


def build_market_snapshot(force_refresh=False):
  now = time.time()
  with _market_snapshot_cache_lock:
    cached_payload = _market_snapshot_cache["payload"]
    cached_epoch = _market_snapshot_cache["epoch"]
    if (
      not force_refresh
      and cached_payload is not None
      and (now - cached_epoch) < MARKET_SNAPSHOT_TTL_SECONDS
    ):
      return cached_payload

  payload = _build_market_snapshot_uncached()
  with _market_snapshot_cache_lock:
    _market_snapshot_cache["payload"] = payload
    _market_snapshot_cache["epoch"] = time.time()
  return payload


def render_page():
  service_urls = build_service_urls(ROOT, HOST)
  section_names = list(service_urls.keys())
  service_endpoints = {
    section: f"/api/section/{section_slug(section)}"
    for section in section_names
  }
  services_json = json.dumps(service_endpoints)

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
    html {{ scroll-behavior: smooth; }}
    body {{
      margin: 0;
      background: radial-gradient(circle at top right, #e6edff 0, #f3f5fa 42%, #f3f5fa 100%);
      color: var(--text);
      font-family: Inter, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif;
      display: flex;
      justify-content: center;
    }}
    .shell {{
      width: min(1260px, 100%);
      margin: 0 auto;
      padding: 18px 20px 26px;
    }}
    .header {{
      background: linear-gradient(145deg, #0b1632, #142653);
      color: #fff;
      border-radius: 16px;
      padding: 16px 20px;
      box-shadow: var(--shadow);
    }}
    .header-top {{ display: flex; align-items: center; justify-content: space-between; gap: 16px; flex-wrap: wrap; }}
    .brand-title {{ font-size: 30px; font-weight: 820; letter-spacing: -0.35px; }}
    .brand-sub {{ font-size: 12px; color: #bfcbf5; letter-spacing: 0.22px; text-transform: uppercase; }}
    .refresh {{ border: 1px solid rgba(255,255,255,0.34); background: rgba(255,255,255,0.08); color: #fff; border-radius: 10px; padding: 8px 14px; font-size: 13px; font-weight: 600; cursor: pointer; }}
    .refresh:hover {{ background: rgba(255,255,255,0.2); }}
    .refresh:disabled {{ opacity: 0.65; cursor: not-allowed; }}
    .market-groups {{ margin-top: 12px; display: grid; gap: 10px; }}
    .market-group {{ display: grid; gap: 6px; }}
    .market-group-label {{
      font-size: 10px;
      font-weight: 800;
      letter-spacing: 0.35px;
      text-transform: uppercase;
      color: #f0d060;
      padding-left: 2px;
      text-decoration: none;
      display: inline-block;
    }}
    .market-group-label:hover {{ color: #fff; text-decoration: underline; }}
    .market-strip {{
      display: flex;
      gap: 8px;
      overflow-x: auto;
      overflow-y: hidden;
      padding-bottom: 2px;
      scrollbar-gutter: stable;
    }}
    .market-strip::-webkit-scrollbar {{ height: 8px; }}
    .market-strip::-webkit-scrollbar-track {{ background: rgba(255,255,255,0.08); border-radius: 999px; }}
    .market-strip::-webkit-scrollbar-thumb {{ background: rgba(207,221,255,0.42); border-radius: 999px; }}
    .market-strip::-webkit-scrollbar-thumb:hover {{ background: rgba(227,236,255,0.58); }}
    .market-tile {{
      background: rgba(0,0,0,0.24);
      border: 1px solid rgba(210,224,255,0.24);
      border-radius: 10px;
      padding: 7px 8px;
      min-width: 165px;
      flex: 0 0 165px;
    }}
    .market-label {{ font-size: 10px; letter-spacing: 0.2px; text-transform: uppercase; color: #c8d5ff; font-weight: 700; margin-bottom: 2px; white-space: normal; line-height: 1.15; min-height: 22px; }}
    .market-value {{ font-size: 14px; font-weight: 800; letter-spacing: -0.2px; color: #ffffff; overflow: hidden; }}
    .market-value.market-up .mv-price {{ color: #8cffb9; }}
    .market-value.market-down .mv-price {{ color: #ff9fb0; }}
    .market-value.market-flat .mv-price {{ color: #d8e2ff; }}
    .market-value.market-fng-extreme-fear .mv-price {{ color: #ff7f96; }}
    .market-value.market-fng-fear .mv-price {{ color: #ffb07a; }}
    .market-value.market-fng-neutral .mv-price {{ color: #ffe8a3; }}
    .market-value.market-fng-greed .mv-price {{ color: #9bffd0; }}
    .market-value.market-fng-extreme-greed .mv-price {{ color: #5effa3; }}
    .mv-price {{ display: block; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}
    .mv-chg {{ display: block; font-size: 11px; font-weight: 700; margin-top: 3px; white-space: nowrap; }}
    .mv-chg-up {{ color: #8cffb9; }}
    .mv-chg-down {{ color: #ff9fb0; }}
    .mv-chg-flat {{ color: #d8e2ff; }}
    .stats {{ display: grid; grid-template-columns: repeat(3, minmax(0,1fr)); gap: 12px; margin: 12px 0 14px; }}
    .stat {{ background: var(--surface); border: 1px solid var(--line); border-radius: 12px; padding: 13px 15px; box-shadow: 0 6px 16px rgba(13, 26, 60, 0.04); }}
    .stat-label {{ margin: 0 0 4px; color: var(--muted); font-size: 12px; text-transform: uppercase; letter-spacing: 0.4px; font-weight: 700; }}
    .stat-value {{ margin: 0; font-size: 30px; font-weight: 820; letter-spacing: -0.75px; color: var(--text); }}
    .section-block {{ margin-bottom: 14px; }}
    .section-head {{ display: flex; align-items: baseline; justify-content: space-between; gap: 10px; margin-bottom: 9px; border-bottom: 1px solid #dbe3f1; padding-bottom: 8px; }}
    .section-head h2 {{ margin: 0; font-size: 25px; letter-spacing: -0.55px; font-weight: 820; }}
    .section-head span {{ font-size: 12px; color: var(--muted); text-transform: uppercase; letter-spacing: 0.35px; font-weight: 700; }}
    .card-grid {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0,1fr));
      gap: 10px;
    }}
    .card {{
      background: var(--surface);
      border: 1px solid var(--line);
      border-radius: 14px;
      padding: 12px 13px 10px;
      box-shadow: 0 10px 24px rgba(16, 30, 66, 0.06);
      transition: transform 140ms ease, box-shadow 140ms ease;
      max-height: 360px;
      overflow-y: auto;
      overflow-x: hidden;
      scrollbar-gutter: stable;
    }}
    .card::-webkit-scrollbar {{ width: 8px; }}
    .card::-webkit-scrollbar-track {{ background: #edf2fb; border-radius: 999px; }}
    .card::-webkit-scrollbar-thumb {{ background: #c0cde8; border-radius: 999px; }}
    .card::-webkit-scrollbar-thumb:hover {{ background: #a9b9dd; }}
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
    .headline-list li {{ padding: 6px 0; border-top: 1px solid #edf1fa; }}
    .headline-list li:first-child {{ border-top: 0; padding-top: 2px; }}
    .headline-list a {{ text-decoration: none; color: #1a2442; font-size: 14px; line-height: 1.35; font-weight: 550; display: -webkit-box; -webkit-line-clamp: 2; line-clamp: 2; -webkit-box-orient: vertical; overflow: hidden; }}
    .headline-list a:hover {{ color: var(--accent); }}
    .headline-meta {{ display: flex; align-items: center; justify-content: space-between; gap: 8px; margin-top: 3px; flex-wrap: wrap; }}
    .headline-time {{ color: #6e7b99; font-size: 11px; font-weight: 600; }}
    .impact-wrap {{ display: inline-flex; align-items: center; flex-wrap: wrap; gap: 4px; }}
    .impact-chip {{ display: inline-flex; align-items: center; gap: 3px; font-size: 10px; letter-spacing: 0.2px; border-radius: 999px; padding: 2px 6px; font-weight: 700; border: 1px solid transparent; }}
    .impact-chip.impact-up {{ background: #e8fbef; color: #19633c; border-color: #b7e7c9; }}
    .impact-chip.impact-down {{ background: #ffedf1; color: #8d1e3c; border-color: #f7c1ce; }}
    .placeholder, .service-error {{ display: grid; place-items: center; min-height: 120px; color: var(--muted); }}
    .service-error strong {{ color: #a11; }}
    @media (max-width: 1200px) {{
      .card-grid {{ grid-template-columns: repeat(2, minmax(0,1fr)); }}
      .market-tile {{ min-width: 155px; flex-basis: 155px; }}
    }}
    @media (max-width: 980px) {{
      .stats {{ grid-template-columns: 1fr; }}
      .card-grid {{ grid-template-columns: 1fr; }}
      .card {{ max-height: none; overflow: visible; }}
      .section-head h2 {{ font-size: 23px; }}
      .brand-title {{ font-size: 25px; }}
      .market-group-label {{ padding-left: 2px; }}
      .market-tile {{ min-width: 145px; flex-basis: 145px; }}
    }}
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
      <div class='market-groups'>
        <div class='market-group'>
          <a class='market-group-label' href='#metals'>Metals</a>
          <div class='market-strip market-strip-metals'>
            <div class='market-tile'><div class='market-label'>Gold Spot (USD/oz)</div><div id='mk-gold' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Silver Spot (USD/oz)</div><div id='mk-silver' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Platinum Spot (USD/oz)</div><div id='mk-platinum' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Palladium Spot (USD/oz)</div><div id='mk-palladium' class='market-value'>--</div></div>
          </div>
        </div>
        <div class='market-group'>
          <a class='market-group-label' href='#crypto'>Crypto</a>
          <div class='market-strip market-strip-crypto'>
            <div class='market-tile'><div class='market-label'>Fear &amp; Greed Index</div><div id='mk-fng' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>BTC/USD</div><div id='mk-btc' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>ETH/USD</div><div id='mk-eth' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>XRP/USD</div><div id='mk-xrp' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>SOL/USD</div><div id='mk-sol' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>BNB/USD</div><div id='mk-bnb' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>ADA/USD</div><div id='mk-ada' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>DOGE/USD</div><div id='mk-doge' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>AVAX/USD</div><div id='mk-avax' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>LINK/USD</div><div id='mk-link' class='market-value'>--</div></div>
          </div>
        </div>
        <div class='market-group'>
          <a class='market-group-label' href='#financial'>Financial</a>
          <div class='market-strip market-strip-markets'>
            <div class='market-tile'><div class='market-label'>S&amp;P 500</div><div id='mk-sp500' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Dow Jones</div><div id='mk-dow' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>NASDAQ</div><div id='mk-nasdaq' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Russell 2000</div><div id='mk-russell2000' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Nikkei 225</div><div id='mk-nikkei' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>FTSE 100</div><div id='mk-ftse' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>DAX</div><div id='mk-dax' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Hang Seng</div><div id='mk-hangseng' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>VIX</div><div id='mk-vix' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>US 10Y Yield (%)</div><div id='mk-tnx' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>US 5Y Yield (%)</div><div id='mk-fiveyr' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>US 30Y Yield (%)</div><div id='mk-thirtyyr' class='market-value'>--</div></div>
          </div>
        </div>
        <div class='market-group'>
          <a class='market-group-label' href='#geopolitical'>Geopolitical</a>
          <div class='market-strip market-strip-macro'>
            <div class='market-tile'><div class='market-label'>WTI Crude</div><div id='mk-wti' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>Brent Crude</div><div id='mk-brent' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>EUR/USD</div><div id='mk-eurusd' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>GBP/USD</div><div id='mk-gbpusd' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>AUD/USD</div><div id='mk-audusd' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>USD/JPY</div><div id='mk-usdjpy' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>USD/SGD</div><div id='mk-usdsgd' class='market-value'>--</div></div>
            <div class='market-tile'><div class='market-label'>DXY (USD Index)</div><div id='mk-dxy' class='market-value'>--</div></div>
          </div>
        </div>
      </div>
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
    const MAX_HEADLINES_PER_CARD = 8;

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

    function relativeTime(value) {{
      if (!value) {{
        return 'time n/a';
      }}
      const dt = new Date(value);
      if (Number.isNaN(dt.getTime())) {{
        return 'time n/a';
      }}
      const now = Date.now();
      const deltaSec = Math.max(0, Math.floor((now - dt.getTime()) / 1000));
      if (deltaSec < 60) {{
        return `${{deltaSec}}s ago`;
      }}
      if (deltaSec < 3600) {{
        return `${{Math.floor(deltaSec / 60)}}m ago`;
      }}
      if (deltaSec < 86400) {{
        return `${{Math.floor(deltaSec / 3600)}}h ago`;
      }}
      return `${{Math.floor(deltaSec / 86400)}}d ago`;
    }}

    function inferImpactDirection(text) {{
      const bullish = [
        'surge', 'rally', 'jump', 'breakout', 'gain', 'gains', 'rise', 'rises',
        'bullish', 'inflow', 'approval', 'beats', 'beat', 'upgrade', 'record high', 'all-time high'
      ];
      const bearish = [
        'drop', 'plunge', 'selloff', 'sell-off', 'crash', 'decline', 'falls', 'fall',
        'bearish', 'outflow', 'ban', 'lawsuit', 'hack', 'downgrade', 'recession', 'liquidation',
        'shrink', 'slump', 'weakens', 'weaken', 'cuts'
      ];
      let upHits = 0;
      let downHits = 0;
      bullish.forEach(word => {{
        if (text.includes(word)) upHits += 1;
      }});
      bearish.forEach(word => {{
        if (text.includes(word)) downHits += 1;
      }});
      if (upHits > downHits) return 'up';
      if (downHits > upHits) return 'down';
      return null;
    }}

    function sectionImpactLabel(sectionName) {{
      const key = String(sectionName || '').trim().toLowerCase();
      if (key === 'crypto') return 'Crypto';
      if (key === 'metals') return 'Metals';
      if (key === 'financial') return 'Equities';
      if (key === 'geopolitical') return 'Macro';
      return '';
    }}

    function inferHeadlineImpact(sectionName, title) {{
      const text = String(title || '').toLowerCase();
      const direction = inferImpactDirection(text);
      const label = sectionImpactLabel(sectionName);
      if (!direction || !label) {{
        return null;
      }}
      return {{ label, direction }};
    }}

    function impactChipHtml(impact) {{
      if (!impact || !impact.direction) {{
        return '';
      }}
      const symbol = impact.direction === 'up' ? '▲' : '▼';
      return `<span class="impact-chip impact-${{impact.direction}}">${{symbol}} ${{escapeHtml(impact.label)}}</span>`;
    }}

    function headlineHtmlBlock(sectionName, source) {{
      if (!source.headlines || !source.headlines.length) {{
        return `<p>No headlines available right now.</p>`;
      }}
      const sorted = [...source.headlines].sort((a, b) => {{
        const ta = a.published_at ? new Date(a.published_at).getTime() : 0;
        const tb = b.published_at ? new Date(b.published_at).getTime() : 0;
        return tb - ta;
      }});
      const visible = sorted.slice(0, MAX_HEADLINES_PER_CARD);
      return `<ul class="headline-list">${{visible.map(item => {{
        const rawTime = item.published_at || source.updated_at;
        const headlineTime = relativeTime(rawTime);
        const impact = inferHeadlineImpact(sectionName, item.title);
        const chip = impactChipHtml(impact);
        const impactsHtml = chip ? `<span class="impact-wrap">${{chip}}</span>` : '';
        return `<li><a href="${{escapeHtml(item.link)}}" target="_blank" rel="noopener noreferrer">${{escapeHtml(item.title)}}</a><div class="headline-meta"><span class="headline-time">${{escapeHtml(headlineTime)}}</span>${{impactsHtml}}</div></li>`;
      }}).join('')}}</ul>`;
    }}

    function sourceCardHtml(sectionName, source) {{
      const badgeClass = source.mode === 'page' ? 'mode-page' : 'mode-feed';
      const badgeText = source.mode === 'page' ? 'Page Fallback' : 'Live Feed';
      const sourceId = source.source_id || '';
      const domId = sourceDomId(sourceId, source.source_name || 'source');
      const headlineHtml = headlineHtmlBlock(sectionName, source);

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

    const REQUEST_TIMEOUT_MS = 9000;

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

      const sources = payload.sources || [];
      const headlineCount = sources.reduce((acc, src) => acc + ((src.headlines || []).length), 0);
      grid.innerHTML = sources.map(source => sourceCardHtml(sectionName, source)).join('');
      count.textContent = `${{sources.length}} source${{sources.length !== 1 ? 's' : ''}} · ${{headlineCount}} headline${{headlineCount !== 1 ? 's' : ''}}`;
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
      {{ id: 'mk-platinum', key: 'platinum_spot_usd_oz', digits: 2 }},
      {{ id: 'mk-palladium', key: 'palladium_spot_usd_oz', digits: 2 }},
      {{ id: 'mk-fng', key: 'fear_greed', digits: 0 }},
      {{ id: 'mk-btc', key: 'btc_usd', chg24Key: 'btc_chg24', digits: 2 }},
      {{ id: 'mk-eth', key: 'eth_usd', chg24Key: 'eth_chg24', digits: 2 }},
      {{ id: 'mk-xrp', key: 'xrp_usd', chg24Key: 'xrp_chg24', digits: 4 }},
      {{ id: 'mk-sol', key: 'sol_usd', chg24Key: 'sol_chg24', digits: 2 }},
      {{ id: 'mk-bnb', key: 'bnb_usd', chg24Key: 'bnb_chg24', digits: 2 }},
      {{ id: 'mk-ada', key: 'ada_usd', chg24Key: 'ada_chg24', digits: 4 }},
      {{ id: 'mk-doge', key: 'doge_usd', chg24Key: 'doge_chg24', digits: 4 }},
      {{ id: 'mk-avax', key: 'avax_usd', chg24Key: 'avax_chg24', digits: 2 }},
      {{ id: 'mk-link', key: 'link_usd', chg24Key: 'link_chg24', digits: 2 }},
      {{ id: 'mk-sp500', key: 'sp500', digits: 2 }},
      {{ id: 'mk-dow', key: 'dow', digits: 2 }},
      {{ id: 'mk-nasdaq', key: 'nasdaq', digits: 2 }},
      {{ id: 'mk-russell2000', key: 'russell2000', digits: 2 }},
      {{ id: 'mk-nikkei', key: 'nikkei', digits: 2 }},
      {{ id: 'mk-ftse', key: 'ftse', digits: 2 }},
      {{ id: 'mk-dax', key: 'dax', digits: 2 }},
      {{ id: 'mk-hangseng', key: 'hangseng', digits: 2 }},
      {{ id: 'mk-vix', key: 'vix', digits: 2 }},
      {{ id: 'mk-tnx', key: 'tnx', digits: 3 }},
      {{ id: 'mk-fiveyr', key: 'fiveyr_yield', digits: 3 }},
      {{ id: 'mk-thirtyyr', key: 'thirtyyr_yield', digits: 3 }},
      {{ id: 'mk-wti', key: 'wti', digits: 2 }},
      {{ id: 'mk-brent', key: 'brent', digits: 2 }},
      {{ id: 'mk-eurusd', key: 'eurusd', digits: 4 }},
      {{ id: 'mk-gbpusd', key: 'gbpusd', digits: 4 }},
      {{ id: 'mk-audusd', key: 'audusd', digits: 4 }},
      {{ id: 'mk-usdjpy', key: 'usdjpy', digits: 3 }},
      {{ id: 'mk-usdsgd', key: 'usd_sgd', digits: 4 }},
      {{ id: 'mk-dxy', key: 'dxy', digits: 2 }},
    ];

    let previousMarketMetrics = null;
    let isLoadingAll = false;

    function setMarketCell(id, text, trend = 'flat', extraClass = '') {{
      const node = document.getElementById(id);
      if (node) {{
        node.innerHTML = text;
        node.classList.remove(
          'market-up',
          'market-down',
          'market-flat',
          'market-fng-extreme-fear',
          'market-fng-fear',
          'market-fng-neutral',
          'market-fng-greed',
          'market-fng-extreme-greed'
        );
        if (trend === 'up') {{
          node.classList.add('market-up');
        }} else if (trend === 'down') {{
          node.classList.add('market-down');
        }} else {{
          node.classList.add('market-flat');
        }}
        if (extraClass) {{
          node.classList.add(extraClass);
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

    function marketDisplayText(value, digits, trend, change24 = null) {{
      if (value === null || value === undefined || Number.isNaN(Number(value))) {{
        return '<span class="mv-price">n/a</span>';
      }}
      const base = fmt(value, digits);
      const arrow = trend === 'up' ? '▲' : trend === 'down' ? '▼' : '•';
      const chg = Number(change24);
      let chgHtml = '';
      if (Number.isFinite(chg)) {{
        const sign = chg >= 0 ? '+' : '';
        const chgCls = chg > 0 ? 'mv-chg-up' : chg < 0 ? 'mv-chg-down' : 'mv-chg-flat';
        chgHtml = `<span class="mv-chg ${{chgCls}}">${{sign}}${{fmt(chg, 2)}}%</span>`;
      }}
      return `<span class="mv-price">${{arrow}} ${{base}}</span>${{chgHtml}}`;
    }}

    function fearGreedBand(value) {{
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {{
        return 'N/A';
      }}
      if (numeric <= 24) {{
        return 'Extreme Fear';
      }}
      if (numeric <= 44) {{
        return 'Fear';
      }}
      if (numeric <= 54) {{
        return 'Neutral';
      }}
      if (numeric <= 74) {{
        return 'Greed';
      }}
      return 'Extreme Greed';
    }}

    function fearGreedBandClass(band) {{
      if (band === 'Extreme Fear') {{
        return 'market-fng-extreme-fear';
      }}
      if (band === 'Fear') {{
        return 'market-fng-fear';
      }}
      if (band === 'Neutral') {{
        return 'market-fng-neutral';
      }}
      if (band === 'Greed') {{
        return 'market-fng-greed';
      }}
      if (band === 'Extreme Greed') {{
        return 'market-fng-extreme-greed';
      }}
      return '';
    }}

    function fearGreedDisplay(value, band) {{
      const numeric = Number(value);
      if (!Number.isFinite(numeric)) {{
        return '<span class="mv-price">n/a</span>';
      }}
      return `<span class="mv-price">• ${{Math.round(numeric)}}</span><span class="mv-chg">${{band}}</span>`;
    }}

    function renderMarketSnapshot(payload) {{
      const metrics = (payload && payload.metrics) ? payload.metrics : {{}};
      MARKET_META.forEach(item => {{
        const currentValue = metrics[item.key];
        if (item.key === 'fear_greed') {{
          const band = fearGreedBand(currentValue);
          const bandClass = fearGreedBandClass(band);
          setMarketCell(item.id, fearGreedDisplay(currentValue, band), 'flat', bandClass);
          return;
        }}
        const previousValue = previousMarketMetrics ? previousMarketMetrics[item.key] : null;
        const trend = marketTrend(previousValue, currentValue);
        const change24 = item.chg24Key ? metrics[item.chg24Key] : null;
        setMarketCell(item.id, marketDisplayText(currentValue, item.digits, trend, change24), trend);
      }});
      previousMarketMetrics = metrics;
    }}

    async function loadMarketSnapshot(forceRefresh = false) {{
      try {{
        const url = withRefreshFlag('/api/market-snapshot', forceRefresh);
        const payload = await fetchJsonWithTimeout(url);
        renderMarketSnapshot(payload);
      }} catch (_error) {{
        MARKET_META.forEach(item => setMarketCell(item.id, 'n/a', 'flat'));
      }}
    }}

    async function loadAll(forceRefresh = false) {{
      if (isLoadingAll) {{
        return;
      }}
      isLoadingAll = true;
      let totalSources = 0;
      let totalHeadlines = 0;
      const marketTask = loadMarketSnapshot(forceRefresh).catch(() => {{
        MARKET_META.forEach(item => setMarketCell(item.id, 'n/a', 'flat'));
      }});

      try {{
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
        await marketTask;
      }} finally {{
        isLoadingAll = false;
      }}
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

    const MARKET_REFRESH_INTERVAL_MS = 15000;
    const SECTION_AUTO_REFRESH_MS = 300000;

    setInterval(() => {{
      loadMarketSnapshot(false);
    }}, MARKET_REFRESH_INTERVAL_MS);

    setInterval(() => {{
      loadAll(false);
    }}, SECTION_AUTO_REFRESH_MS);
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
  with urlopen(request, timeout=TOPIC_PROXY_TIMEOUT_SECONDS) as response:
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
    table_rows.append(
      "<tr>"
      f"<td>{escape(item['name'])}</td>"
      f"<td>{escape(item['role'])}</td>"
      f"<td>{escape(section)}</td>"
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
    <tr><th>Name</th><th>Role</th><th>Section</th><th>Port</th><th>Status</th><th>Health Detail</th></tr>
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
            refresh_flag = parse_qs(parsed.query).get("refresh", ["0"])[0].strip().lower() in ("1", "true", "yes")
            payload = json.dumps(build_market_snapshot(force_refresh=refresh_flag)).encode("utf-8")
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
    server = ThreadingHTTPServer((HOST, PORT), DashboardHandler)
    print(f"Traders View UI running at http://{HOST}:{PORT}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
