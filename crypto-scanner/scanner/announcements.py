"""Monitor Binance announcements for new Alpha token listings.

Binance publishes listing announcements via their CMS API. This module
polls that API, detects new Alpha-related announcements, extracts token
symbols, and fires alerts so you can act before the listing goes live.
"""

from __future__ import annotations

import json
import logging
import re
import time
from dataclasses import dataclass, field
from pathlib import Path

import aiohttp

from scanner.config import BASE_DIR
from scanner.rate_limiter import get_limiter

logger = logging.getLogger(__name__)

# Binance announcement API endpoints
# Primary: public CMS API (may require browser-like headers)
ANNOUNCEMENT_URL = "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
# Fallback: Binance public announcement API
ANNOUNCEMENT_URL_V2 = "https://www.binance.com/bapi/composite/v1/public/cms/article/catalog/list/query"

# File to track which announcements we've already seen
SEEN_ANNOUNCEMENTS_PATH = BASE_DIR / "seen_announcements.json"

# Patterns that indicate Alpha/listing announcements
ALPHA_PATTERNS = [
    r"binance\s+alpha",
    r"alpha\s+(?:alert|listing|add)",
    r"new\s+(?:token|listing).*alpha",
    r"will\s+list\b",
    r"adds?\b.*\bto\s+(?:alpha|spot)",
]
ALPHA_RE = re.compile("|".join(ALPHA_PATTERNS), re.IGNORECASE)

# Extract token symbols from announcement titles
# Matches patterns like "(TOKEN)", "TOKEN/USDT", "lists TOKEN"
SYMBOL_PATTERNS = [
    r"\(([A-Z]{2,10})\)",                    # (TOKEN)
    r"\b([A-Z]{2,10})/USDT\b",              # TOKEN/USDT
    r"(?:lists?|adds?)\s+([A-Z]{2,10})\b",  # lists TOKEN
    r"\b([A-Z]{2,10})\s+(?:Token|Coin)\b",  # TOKEN Token
]
SYMBOL_RE = re.compile("|".join(SYMBOL_PATTERNS))

# Symbols to ignore (common words that match patterns)
IGNORE_SYMBOLS = {
    "THE", "AND", "FOR", "NEW", "ALL", "HOW", "GET", "VIA", "USD",
    "BTC", "ETH", "BNB", "USDT", "USDC", "BUSD", "FDUSD",
    "API", "NFT", "CEO", "CTO", "AMA", "FAQ", "KYC",
}


@dataclass
class Announcement:
    id: str
    title: str
    url: str
    publish_time: int          # Unix ms
    symbols: list[str]         # Extracted token symbols
    is_alpha: bool             # Matches Alpha patterns
    raw_data: dict = field(default_factory=dict, repr=False)


async def fetch_announcements(
    session: aiohttp.ClientSession,
    catalog_id: int = 48,      # 48 = "New Cryptocurrency Listing"
    page_size: int = 20,
) -> list[dict]:
    """Fetch recent Binance announcements from CMS API.

    Uses browser-like headers to avoid 403 blocks.
    Falls back to the Binance RSS feed if the CMS API is unavailable.
    """
    limiter = get_limiter("binance")
    await limiter.acquire()

    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        ),
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Origin": "https://www.binance.com",
        "Referer": "https://www.binance.com/en/support/announcement/new-cryptocurrency-listing",
    }

    payload = {
        "type": 1,
        "catalogId": catalog_id,
        "pageNo": 1,
        "pageSize": page_size,
    }

    # Try CMS API with browser headers
    try:
        async with session.post(
            ANNOUNCEMENT_URL, json=payload, headers=headers
        ) as resp:
            if resp.status == 200:
                data = await resp.json()
                catalogs = data.get("data", {}).get("catalogs", [])
                articles = []
                for cat in catalogs:
                    articles.extend(cat.get("articles", []))
                if articles:
                    return articles
    except Exception as e:
        logger.debug(f"CMS API failed: {e}")

    # Fallback: Binance RSS feed
    try:
        rss_url = "https://www.binance.com/en/support/announcement/rss"
        async with session.get(rss_url, headers=headers) as resp:
            if resp.status == 200:
                text = await resp.text()
                return _parse_rss(text)
    except Exception as e:
        logger.debug(f"RSS feed failed: {e}")

    # Fallback: Binance support page API (different endpoint)
    try:
        api_url = (
            "https://www.binance.com/bapi/composite/v1/public/cms/article/list/query"
            f"?type=1&pageNo=1&pageSize={page_size}"
        )
        async with session.get(api_url, headers=headers) as resp:
            if resp.status == 200:
                data = await resp.json()
                catalogs = data.get("data", {}).get("catalogs", [])
                articles = []
                for cat in catalogs:
                    articles.extend(cat.get("articles", []))
                return articles
    except Exception as e:
        logger.debug(f"Fallback API failed: {e}")

    logger.warning(
        "Could not fetch Binance announcements. "
        "The API may be geo-blocked. Consider using a VPN or "
        "checking announcements manually at "
        "https://www.binance.com/en/support/announcement/new-cryptocurrency-listing"
    )
    return []


def _parse_rss(xml_text: str) -> list[dict]:
    """Parse Binance RSS feed XML into article dicts."""
    import xml.etree.ElementTree as ET

    articles = []
    try:
        root = ET.fromstring(xml_text)
        for item in root.findall(".//item"):
            title = item.findtext("title", "")
            link = item.findtext("link", "")
            pub_date = item.findtext("pubDate", "")

            # Extract code from link
            code = link.split("/")[-1] if link else ""

            articles.append({
                "title": title,
                "code": code,
                "releaseDate": 0,  # RSS doesn't give Unix timestamp easily
            })
    except ET.ParseError as e:
        logger.debug(f"RSS parse error: {e}")

    return articles


def extract_symbols(title: str) -> list[str]:
    """Extract token symbols from an announcement title."""
    symbols = set()
    for match in SYMBOL_RE.finditer(title):
        # Each group in the alternation
        for group in match.groups():
            if group and group not in IGNORE_SYMBOLS:
                symbols.add(group)
    return sorted(symbols)


def is_alpha_announcement(title: str) -> bool:
    """Check if an announcement is related to Binance Alpha."""
    return bool(ALPHA_RE.search(title))


def parse_announcement(article: dict) -> Announcement:
    """Parse a raw API article into an Announcement."""
    title = article.get("title", "")
    code = article.get("code", "")
    release_date = article.get("releaseDate", 0)

    return Announcement(
        id=code,
        title=title,
        url=f"https://www.binance.com/en/support/announcement/{code}",
        publish_time=release_date,
        symbols=extract_symbols(title),
        is_alpha=is_alpha_announcement(title),
        raw_data=article,
    )


def load_seen_ids() -> set[str]:
    """Load previously seen announcement IDs from disk."""
    if not SEEN_ANNOUNCEMENTS_PATH.exists():
        return set()
    try:
        with open(SEEN_ANNOUNCEMENTS_PATH) as f:
            data = json.load(f)
            return set(data.get("seen_ids", []))
    except Exception:
        return set()


def save_seen_ids(seen_ids: set[str]):
    """Persist seen announcement IDs to disk."""
    with open(SEEN_ANNOUNCEMENTS_PATH, "w") as f:
        json.dump({"seen_ids": sorted(seen_ids), "updated": time.time()}, f, indent=2)


async def check_new_announcements(
    session: aiohttp.ClientSession,
    alpha_only: bool = True,
) -> list[Announcement]:
    """Check for new Binance announcements and return unseen ones.

    Args:
        session: aiohttp session
        alpha_only: If True, only return announcements matching Alpha patterns.
                    If False, return all new listing announcements.
    """
    articles = await fetch_announcements(session)
    if not articles:
        return []

    seen_ids = load_seen_ids()
    new_announcements = []

    for article in articles:
        ann = parse_announcement(article)

        if ann.id in seen_ids:
            continue

        seen_ids.add(ann.id)

        # Filter
        if alpha_only and not ann.is_alpha:
            continue

        if ann.symbols:
            new_announcements.append(ann)
            logger.info(
                f"NEW LISTING: {ann.title} | Symbols: {', '.join(ann.symbols)} | "
                f"Alpha: {ann.is_alpha}"
            )

    save_seen_ids(seen_ids)
    return new_announcements


async def get_all_recent_announcements(
    session: aiohttp.ClientSession,
) -> list[Announcement]:
    """Get all recent announcements (for initial display / debugging)."""
    articles = await fetch_announcements(session, page_size=50)
    announcements = []
    for article in articles:
        ann = parse_announcement(article)
        announcements.append(ann)
    return announcements
