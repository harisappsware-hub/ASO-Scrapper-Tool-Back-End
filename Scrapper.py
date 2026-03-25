"""
Google Play Store Scraper
Uses requests + BeautifulSoup with multiple extraction strategies.
"""
import re
import json
import time
import random
import asyncio
import hashlib
import logging
from typing import Optional, List, Dict, Any
from urllib.parse import urlparse, parse_qs, urlencode, quote

import requests
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

COUNTRY_TLD = {
    "us": "com", "gb": "co.uk", "pk": "com.pk",
    "in": "co.in", "ca": "ca", "au": "com.au"
}


def _random_delay(min_s=1.0, max_s=2.5):
    time.sleep(random.uniform(min_s, max_s))


def _extract_app_id(url: str) -> Optional[str]:
    """Extract app ID from Play Store URL."""
    patterns = [
        r"id=([a-zA-Z][a-zA-Z0-9._]+)",
        r"apps/details\?.*id=([a-zA-Z][a-zA-Z0-9._]+)",
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None


class GooglePlayScraper:
    BASE_URL = "https://play.google.com"
    SEARCH_URL = "https://play.google.com/store/search"

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(HEADERS)
        self._cache: Dict[str, Any] = {}

    def _get(self, url: str, params: dict = None) -> Optional[requests.Response]:
        cache_key = hashlib.md5((url + str(params)).encode()).hexdigest()
        if cache_key in self._cache:
            return self._cache[cache_key]

        try:
            _random_delay(0.8, 2.0)
            resp = self.session.get(url, params=params, timeout=15)
            resp.raise_for_status()
            self._cache[cache_key] = resp
            return resp
        except Exception as e:
            logger.error(f"GET {url} failed: {e}")
            return None

    async def scrape_app(self, url: str, country: str = "us") -> Optional[Dict]:
        """Scrape app metadata from Google Play Store page."""
        app_id = _extract_app_id(url)
        if not app_id:
            raise ValueError(f"Cannot extract app ID from URL: {url}")

        app_url = f"{self.BASE_URL}/store/apps/details"
        params = {"id": app_id, "hl": "en", "gl": country.upper()}

        loop = asyncio.get_event_loop()
        resp = await loop.run_in_executor(None, lambda: self._get(app_url, params))

        if not resp:
            return self._mock_app_data(app_id)  # fallback for demo

        soup = BeautifulSoup(resp.text, "html.parser")

        # Multiple extraction strategies
        data = {
            "app_id": app_id,
            "url": url,
            "country": country,
        }

        # Title
        data["title"] = self._extract_title(soup)
        # Short description
        data["short_description"] = self._extract_short_desc(soup)
        # Long description
        data["long_description"] = self._extract_long_desc(soup)
        # Developer
        data["developer"] = self._extract_developer(soup)
        # Category
        data["category"] = self._extract_category(soup)
        # Rating
        data["rating"] = self._extract_rating(soup)
        # Installs
        data["installs"] = self._extract_installs(soup)
        # Version
        data["version"] = self._extract_version(soup)
        # Last updated
        data["last_updated"] = self._extract_last_updated(soup)
        # Icon
        data["icon_url"] = self._extract_icon(soup)

        # Ensure we have at least title
        if not data["title"]:
            return self._mock_app_data(app_id)

        return data

    def _extract_title(self, soup: BeautifulSoup) -> str:
        selectors = [
            ("h1", {"itemprop": "name"}),
            ("h1", {"class": re.compile(r"Fd93Bb")}),
        ]
        for tag, attrs in selectors:
            el = soup.find(tag, attrs)
            if el:
                return el.get_text(strip=True)

        # Try JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(script.string)
                if isinstance(d, dict) and "name" in d:
                    return d["name"]
            except:
                pass

        # Try meta
        meta = soup.find("meta", {"property": "og:title"})
        if meta:
            return meta.get("content", "").split(" - ")[0].strip()

        return ""

    def _extract_short_desc(self, soup: BeautifulSoup) -> str:
        meta = soup.find("meta", {"name": "description"})
        if meta:
            content = meta.get("content", "")
            # Usually the first sentence or 80 chars
            return content[:200] if content else ""

        el = soup.find("div", {"data-g-id": "description"})
        if el:
            text = el.get_text(" ", strip=True)
            return text[:200]

        return ""

    def _extract_long_desc(self, soup: BeautifulSoup) -> str:
        # Try multiple selectors
        selectors = [
            {"data-g-id": "description"},
            {"class": re.compile(r"bARER")},
            {"itemprop": "description"},
        ]
        for attrs in selectors:
            el = soup.find("div", attrs)
            if el:
                return el.get_text(" ", strip=True)

        # JSON-LD
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                d = json.loads(script.string)
                if isinstance(d, dict) and "description" in d:
                    return d["description"]
            except:
                pass

        return ""

    def _extract_developer(self, soup: BeautifulSoup) -> str:
        el = soup.find("a", {"href": re.compile(r"/store/apps/developer")})
        if el:
            return el.get_text(strip=True)
        meta = soup.find("meta", {"property": "og:description"})
        return ""

    def _extract_category(self, soup: BeautifulSoup) -> str:
        el = soup.find("a", {"itemprop": "genre"})
        if el:
            return el.get_text(strip=True)
        return ""

    def _extract_rating(self, soup: BeautifulSoup) -> Optional[float]:
        el = soup.find("div", {"itemprop": "starRating"})
        if el:
            meta = el.find("meta", {"itemprop": "ratingValue"})
            if meta:
                try:
                    return float(meta.get("content", 0))
                except:
                    pass

        # Try aria-label
        el = soup.find("div", {"aria-label": re.compile(r"Rated \d")})
        if el:
            m = re.search(r"[\d.]+", el.get("aria-label", ""))
            if m:
                try:
                    return float(m.group())
                except:
                    pass
        return None

    def _extract_installs(self, soup: BeautifulSoup) -> str:
        patterns = [
            re.compile(r"[\d,]+\+?\s*downloads?", re.I),
            re.compile(r"[\d,]+[MBK]+\+?\s*installs?", re.I),
        ]
        text = soup.get_text()
        for p in patterns:
            m = p.search(text)
            if m:
                return m.group().strip()
        return ""

    def _extract_version(self, soup: BeautifulSoup) -> str:
        el = soup.find(string=re.compile(r"Version"))
        if el:
            parent = el.parent
            if parent:
                sibling = parent.find_next_sibling()
                if sibling:
                    return sibling.get_text(strip=True)
        return ""

    def _extract_last_updated(self, soup: BeautifulSoup) -> str:
        el = soup.find(string=re.compile(r"Updated"))
        if el:
            parent = el.parent
            if parent:
                nxt = parent.find_next_sibling()
                if nxt:
                    return nxt.get_text(strip=True)
        return ""

    def _extract_icon(self, soup: BeautifulSoup) -> str:
        img = soup.find("img", {"itemprop": "image"})
        if img:
            return img.get("src", "")
        meta = soup.find("meta", {"property": "og:image"})
        if meta:
            return meta.get("content", "")
        return ""

    async def estimate_rankings(
        self, keywords: List[str], app_id: str, country: str = "us"
    ) -> List[Dict]:
        """Estimate app ranking for each keyword via Play Store search."""
        results = []
        loop = asyncio.get_event_loop()

        for kw in keywords:
            rank_data = await loop.run_in_executor(
                None, lambda k=kw: self._search_rank(k, app_id, country)
            )
            results.append(rank_data)
            await asyncio.sleep(random.uniform(0.5, 1.2))

        return results

    def _search_rank(self, keyword: str, app_id: str, country: str) -> Dict:
        params = {
            "q": keyword,
            "c": "apps",
            "hl": "en",
            "gl": country.upper()
        }
        resp = self._get(self.SEARCH_URL, params)
        if not resp:
            return {"keyword": keyword, "rank": None, "status": "Error"}

        soup = BeautifulSoup(resp.text, "html.parser")

        # Find app links in results
        links = soup.find_all("a", href=re.compile(r"/store/apps/details"))
        rank = None
        for i, link in enumerate(links, 1):
            href = link.get("href", "")
            if app_id in href:
                rank = i
                break
            if i >= 50:
                break

        return {
            "keyword": keyword,
            "rank": rank,
            "status": "Ranking" if rank else "Not Ranking",
            "results_count": len(links)
        }

    async def estimate_difficulty(
        self, keywords: List[str], country: str = "us"
    ) -> List[Dict]:
        """Estimate keyword difficulty based on competition."""
        results = []
        loop = asyncio.get_event_loop()

        for kw in keywords:
            diff_data = await loop.run_in_executor(
                None, lambda k=kw: self._calc_difficulty(k, country)
            )
            results.append(diff_data)
            await asyncio.sleep(random.uniform(0.3, 0.8))

        return results

    def _calc_difficulty(self, keyword: str, country: str) -> Dict:
        params = {"q": keyword, "c": "apps", "hl": "en", "gl": country.upper()}
        resp = self._get(self.SEARCH_URL, params)
        if not resp:
            return {"keyword": keyword, "score": 50, "competitor_count": 0}

        soup = BeautifulSoup(resp.text, "html.parser")
        links = soup.find_all("a", href=re.compile(r"/store/apps/details"))
        competitor_count = min(len(links), 50)

        # Big players = apps with high install counts mentioned
        text = soup.get_text().lower()
        big_player_signals = len(re.findall(r'\d+[mb]\+\s*(installs?|downloads?)', text))

        # Score: 0-100
        # Base: competitor count (up to 50 points)
        base_score = (competitor_count / 50) * 50
        # Big player penalty
        big_score = min(big_player_signals * 10, 50)
        score = int(min(100, base_score + big_score))

        return {
            "keyword": keyword,
            "score": score,
            "competitor_count": competitor_count,
            "big_players": big_player_signals
        }

    def _mock_app_data(self, app_id: str) -> Dict:
        """Return demo data when scraping is blocked (for development)."""
        return {
            "app_id": app_id,
            "title": f"App ({app_id})",
            "short_description": (
                "A powerful app for productivity and task management. "
                "Organize your work, manage projects, and boost efficiency."
            ),
            "long_description": (
                "Welcome to our comprehensive productivity application. "
                "This app provides powerful tools for task management, "
                "project organization, team collaboration, and workflow automation. "
                "\n\nKey Features:\n"
                "- Smart task manager with priority sorting\n"
                "- Project templates and workflow automation\n"
                "- Team collaboration and real-time sync\n"
                "- Calendar integration and deadline tracking\n"
                "- Offline support and cloud backup\n"
                "- Analytics dashboard and productivity insights\n"
                "\nWhether you're a student, freelancer, or business professional, "
                "our app helps you stay organized, focused, and productive every day. "
                "Manage your tasks efficiently, track your progress, and achieve your goals.\n"
                "\nWith over 5 million downloads and a 4.8 star rating, "
                "join millions of users who trust this app for their daily workflow."
            ),
            "developer": "Demo Developer",
            "category": "Productivity",
            "rating": 4.8,
            "installs": "5,000,000+",
            "version": "3.2.1",
            "last_updated": "January 2025",
            "icon_url": "",
            "url": f"https://play.google.com/store/apps/details?id={app_id}",
            "country": "us",
            "is_demo": True
        }

    def search_apps(self, keyword: str, country: str = "us", limit: int = 5) -> List[Dict]:
        """Search for apps by keyword."""
        params = {"q": keyword, "c": "apps", "hl": "en", "gl": country.upper()}
        resp = self._get(self.SEARCH_URL, params)
        if not resp:
            return []

        soup = BeautifulSoup(resp.text, "html.parser")
        apps = []

        links = soup.find_all("a", href=re.compile(r"/store/apps/details\?id="))
        seen = set()

        for link in links:
            href = link.get("href", "")
            m = re.search(r"id=([a-zA-Z][a-zA-Z0-9._]+)", href)
            if m:
                aid = m.group(1)
                if aid not in seen:
                    seen.add(aid)
                    apps.append({
                        "app_id": aid,
                        "url": f"https://play.google.com{href}",
                        "title": link.get_text(strip=True) or aid
                    })
            if len(apps) >= limit:
                break

        return apps
