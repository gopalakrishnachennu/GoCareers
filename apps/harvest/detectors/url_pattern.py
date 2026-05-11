from typing import Optional
from urllib.parse import urlparse

from . import get_url_patterns

BOT_HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; GoCareers-Bot/1.0; +https://gocareers.io)",
}


class URLPatternDetector:
    """Step 1: Direct URL substring match against known platform patterns."""

    def detect(self, company) -> tuple[Optional[str], str, str]:
        urls = []
        if getattr(company, "career_site_url", ""):
            urls.append(company.career_site_url.lower())
        if getattr(company, "website", ""):
            urls.append(company.website.lower())

        patterns_by_slug = get_url_patterns()
        for url in urls:
            for slug, patterns in patterns_by_slug.items():
                for pattern in patterns:
                    if pattern_matches_url(pattern, url):
                        return slug, "HIGH", "URL_PATTERN"

        return None, "UNKNOWN", "UNDETECTED"


def pattern_matches_url(pattern: str, url: str) -> bool:
    """Match registry URL patterns with host/path boundaries, not raw substrings."""
    pattern = (pattern or "").strip().lower()
    url = (url or "").strip().lower()
    if not pattern or not url:
        return False

    candidate = url if "://" in url else f"https://{url}"
    parsed = urlparse(candidate)
    host = (parsed.netloc or parsed.path.split("/", 1)[0]).lower()
    path = (parsed.path or "").lower()
    host_path = f"{host}{path}"

    if not host:
        return pattern in url

    if pattern.startswith(".") and "/" not in pattern:
        return host.endswith(pattern)

    if "/" in pattern:
        return pattern in host_path

    return host == pattern or host.endswith(f".{pattern}")
