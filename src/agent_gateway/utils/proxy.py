"""
Proxy utilities for platform adapters that need to route through
HTTP/SOCKS proxies.

Provides a unified interface to resolve proxy URLs from environment
variables and build platform-specific proxy kwargs.
"""

from __future__ import annotations

import logging
import os
from typing import Optional
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)


def resolve_proxy_url(
    platform_env_var: Optional[str] = None,
    target_hosts: Optional[str] = None,
) -> Optional[str]:
    """Resolve a proxy URL from environment variables.

    Check order:
      0. *platform_env_var* (e.g. ``DISCORD_PROXY``) — highest priority
      1. HTTPS_PROXY / HTTP_PROXY / ALL_PROXY (and lowercase variants)

    Returns ``None`` if no proxy is configured.
    """
    if platform_env_var:
        value = os.environ.get(platform_env_var, "").strip()
        if value:
            return _normalise_proxy_url(value)

    for key in (
        "HTTPS_PROXY", "HTTP_PROXY", "ALL_PROXY",
        "https_proxy", "http_proxy", "all_proxy",
    ):
        value = os.environ.get(key, "").strip()
        if value:
            return _normalise_proxy_url(value)

    return None


def _normalise_proxy_url(url: str) -> str:
    """Normalise a proxy URL (strip whitespace, add scheme if missing)."""
    url = url.strip()
    if not url:
        return ""
    if "://" not in url:
        url = f"http://{url}"
    return url


def proxy_kwargs_for_httpx(proxy_url: Optional[str]) -> dict:
    """Build kwargs for ``httpx.Client`` with proxy support.

    Returns ``{"proxy": url}`` or ``{}``.
    """
    if not proxy_url:
        return {}
    return {"proxy": proxy_url}


def proxy_kwargs_for_aiohttp(proxy_url: Optional[str]) -> tuple[dict, dict]:
    """Build kwargs for ``aiohttp.ClientSession`` with proxy support.

    Returns ``(session_kwargs, request_kwargs)``.
    """
    if not proxy_url:
        return {}, {}

    try:
        from aiohttp_socks import ProxyConnector
        connector = ProxyConnector.from_url(proxy_url, rdns=True)
        return {"connector": connector}, {}
    except ImportError:
        if proxy_url.lower().startswith("socks"):
            logger.warning(
                "aiohttp_socks not installed — SOCKS proxy ignored. "
                "Run: pip install aiohttp-socks"
            )
            return {}, {}
        return {}, {"proxy": proxy_url}
