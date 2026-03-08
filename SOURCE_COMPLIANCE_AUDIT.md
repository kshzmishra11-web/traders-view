# Source Compliance Audit (2026-02-21)

This is a practical compliance review of currently configured sources in `topic_services/*_service.py`.

## Scope & method
- Reviewed configured `feed_url` and `page_url` values.
- Checked `robots.txt` availability/signals for each source domain.
- Reviewed accessible Terms pages for high-risk providers (Google, Bloomberg, CryptoNews).
- Focused on your current behavior: **RSS fetch + page-scrape fallback**.

## Important caveat
- This is not legal advice. For production/public commercial use, have counsel review final source list and usage terms.

## Current source inventory
- Geopolitical Futures (`geopoliticalfutures.com`)
- Stratfor Worldview (`worldview.stratfor.com`)
- Geopolitical Watch via Google News RSS (`news.google.com`)
- Geopolitical Monitor (`geopoliticalmonitor.com`)
- Financial Times (`ft.com`)
- Bloomberg ETF Report via Google News RSS + Bloomberg page fallback (`news.google.com`, `bloomberg.com`)
- CoinDesk (`coindesk.com`)
- CryptoNews (`cryptonews.com`)
- Kitco via Google News RSS + Kitco page fallback (`news.google.com`, `kitco.com`)
- Sharps Pixley via Google News RSS + Sharps page fallback (`news.google.com`, `sharpspixley.com`)

## Findings by domain

### 1) `bloomberg.com` — **High risk**
- Terms explicitly prohibit scraper/robot/bot/data-mining access without written consent.
- Terms limit use and redistribution; strongly protective of content rights.
- **Recommendation:** Do not page-scrape Bloomberg. Keep only licensed/official feeds or remove source.

### 2) `news.google.com` — **Medium to high risk for page scraping**
- `robots.txt` indicates broad disallow for generic crawling.
- Google Terms prohibit automated access that violates machine-readable rules.
- **Recommendation:** Avoid scraping Google News HTML pages. RSS usage is lower risk than HTML scraping, but still monitor Google policy changes.

### 3) `ft.com` — **High risk for page scraping/reuse**
- `robots.txt` available; Terms page was blocked in this fetch session (403), but FT content is typically subscription/proprietary.
- **Recommendation:** Treat FT as feed-only at most; avoid HTML scraping and avoid redistributing beyond headline/link attribution.

### 4) `cryptonews.com` — **Medium risk**
- Terms state content is proprietary; copying/distribution/publication beyond personal/noncommercial use needs permission.
- **Recommendation:** Keep to headline + link + attribution only; no full-text reuse. Prefer feed-only mode.

### 5) `coindesk.com` — **Medium risk (insufficient ToS retrieval in this pass)**
- `robots.txt` fetch hit redirect behavior in this check; terms endpoint tested returned 404.
- **Recommendation:** Keep RSS-only until explicit ToS review confirms automation permissions.

### 6) `geopoliticalfutures.com`, `worldview.stratfor.com`, `geopoliticalmonitor.com`, `kitco.com`, `sharpspixley.com` — **Low to medium risk (robots appear accessible, no blanket disallow observed in this pass)**
- Still subject to each site terms/copyright policies.
- **Recommendation:** Keep conservative usage (headline + link + attribution), low request rate, and monitor policy changes.

## Project-level risk in current implementation
Current topic services use:
- RSS fetch first
- If empty/error, fallback to HTML page scraping

That fallback creates your main compliance exposure for restricted publishers.

## Recommended policy before wider public launch
1. **Disable HTML fallback** for high-risk domains (`bloomberg.com`, `ft.com`, `news.google.com`, and optionally `cryptonews.com`).
2. Prefer **feed-only** sources (official RSS/API) where possible.
3. Keep output limited to **title + link + source attribution** (already true).
4. Keep rate conservative (already partially true via cache), and avoid aggressive forced refresh for public users.
5. Maintain a source register with permission status (`approved`, `feed-only`, `blocked`, `needs-license`).

## Suggested safe rollout decision
- **Keep now:** Geopolitical Futures, Stratfor, Geopolitical Monitor, Kitco, Sharps Pixley (with conservative usage).
- **Constrain to feed-only:** FT, CoinDesk, CryptoNews, Google News RSS sources.
- **Disable immediately unless licensed:** Bloomberg HTML scraping.

---
If you want, next step is I can enforce this in code by adding a per-source `feed_only` flag and disabling page fallback for restricted domains.
