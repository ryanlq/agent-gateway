"""
Media caching utilities.

Downloads and caches images, audio, video, and documents from platform
attachments so the agent can access them by local file path.

Design:
  - Each media type gets its own cache subdirectory
  - Files are named with a UUID prefix to avoid collisions
  - Optional cleanup of files older than a configurable age
  - Safety checks (magic-byte validation, SSRF protection)
"""

from __future__ import annotations

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Optional

from agent_gateway.utils.paths import resolve_home

logger = logging.getLogger(__name__)

# Default cache root
_DEFAULT_CACHE_ROOT = resolve_home() / "cache"

# Recognised file extensions
SUPPORTED_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".tiff", ".svg"}
SUPPORTED_VIDEO_EXTS = {".mp4", ".mov", ".avi", ".mkv", ".webm"}
SUPPORTED_AUDIO_EXTS = {".ogg", ".opus", ".mp3", ".wav", ".m4a", ".flac"}
SUPPORTED_DOCUMENT_EXTS = {
    ".pdf", ".docx", ".doc", ".odt", ".rtf", ".txt", ".md", ".epub",
    ".xlsx", ".xls", ".ods", ".csv", ".tsv", ".json", ".xml", ".yaml", ".yml",
    ".pptx", ".ppt", ".odp", ".zip", ".tar", ".gz",
}

# Magic byte signatures
_IMAGE_SIGNATURES = {
    b"\x89PNG\r\n\x1a\n": "png",
    b"\xff\xd8\xff": "jpg",
    b"GIF87a": "gif",
    b"GIF89a": "gif",
    b"BM": "bmp",
}


class MediaCache:
    """
    Manages local caching of media files.

    Usage::

        cache = MediaCache()
        path = cache.save_bytes(image_bytes, ext=".jpg")
        cache.cleanup(max_age_hours=24)
    """

    def __init__(self, cache_root: Optional[Path] = None) -> None:
        self.root = cache_root or _DEFAULT_CACHE_ROOT
        self.images_dir = self.root / "images"
        self.audio_dir = self.root / "audio"
        self.video_dir = self.root / "videos"
        self.documents_dir = self.root / "documents"

    # -- Save from bytes -----------------------------------------------------

    def save_bytes(
        self,
        data: bytes,
        *,
        ext: str = ".bin",
        kind: str = "document",
        filename: str = "",
    ) -> str:
        """Save raw bytes to the appropriate cache directory.

        Args:
            data: Raw file bytes.
            ext: File extension including dot (e.g. ``".jpg"``).
            kind: ``"image"`` / ``"audio"`` / ``"video"`` / ``"document"``.
            filename: Optional original filename (preserved in cache name).

        Returns:
            Absolute path to the cached file.
        """
        target_dir = self._dir_for_kind(kind)
        target_dir.mkdir(parents=True, exist_ok=True)

        # Build cache filename
        safe_name = Path(filename).name if filename else ""
        safe_name = safe_name.replace("\x00", "").strip()
        if not safe_name or safe_name in {".", ".."}:
            safe_name = f"file{ext}"

        cached_name = f"{kind}_{uuid.uuid4().hex[:12]}_{safe_name}"
        filepath = target_dir / cached_name

        # Safety: ensure path stays inside cache dir
        try:
            filepath.resolve().relative_to(target_dir.resolve())
        except ValueError:
            cached_name = f"{kind}_{uuid.uuid4().hex[:12]}{ext}"
            filepath = target_dir / cached_name

        filepath.write_bytes(data)
        logger.debug("Cached %s (%d bytes): %s", kind, len(data), filepath)
        return str(filepath)

    def save_image(self, data: bytes, ext: str = ".jpg", filename: str = "") -> str:
        """Save image bytes.  Raises ValueError if data doesn't look like an image."""
        if not self._looks_like_image(data):
            raise ValueError("Data does not appear to be a valid image")
        return self.save_bytes(data, ext=ext, kind="image", filename=filename)

    def save_audio(self, data: bytes, ext: str = ".ogg") -> str:
        return self.save_bytes(data, ext=ext, kind="audio")

    def save_video(self, data: bytes, ext: str = ".mp4") -> str:
        return self.save_bytes(data, ext=ext, kind="video")

    def save_document(self, data: bytes, filename: str = "document") -> str:
        ext = Path(filename).suffix or ".bin"
        return self.save_bytes(data, ext=ext, kind="document", filename=filename)

    # -- Download from URL ---------------------------------------------------

    async def download(self, url: str, ext: str = ".bin", kind: str = "document",
                       retries: int = 2) -> str:
        """Download a file from a URL and cache it.

        Args:
            url: HTTP/HTTPS URL to download.
            ext: Expected file extension.
            kind: Media kind for cache routing.
            retries: Number of retry attempts on transient failures.

        Returns:
            Absolute path to the cached file.
        """
        import asyncio
        try:
            import httpx
        except ImportError:
            raise ImportError("httpx is required for URL downloads: pip install httpx")

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for attempt in range(retries + 1):
                try:
                    response = await client.get(url, headers={
                        "User-Agent": "AgentGateway/0.1",
                    })
                    response.raise_for_status()
                    return self.save_bytes(response.content, ext=ext, kind=kind)
                except (httpx.TimeoutException, httpx.HTTPStatusError) as exc:
                    if isinstance(exc, httpx.HTTPStatusError) and exc.response.status_code < 429:
                        raise
                    if attempt < retries:
                        await asyncio.sleep(1.5 * (attempt + 1))
                        continue
                    raise

    # -- Cleanup -------------------------------------------------------------

    def cleanup(self, max_age_hours: int = 24) -> dict[str, int]:
        """Delete cached files older than *max_age_hours*.

        Returns a dict of {kind: count_removed}.
        """
        cutoff = time.time() - (max_age_hours * 3600)
        result: dict[str, int] = {}

        for kind, directory in [
            ("images", self.images_dir),
            ("audio", self.audio_dir),
            ("videos", self.video_dir),
            ("documents", self.documents_dir),
        ]:
            if not directory.exists():
                continue
            removed = 0
            for f in directory.iterdir():
                if f.is_file() and f.stat().st_mtime < cutoff:
                    try:
                        f.unlink()
                        removed += 1
                    except OSError:
                        pass
            result[kind] = removed

        total = sum(result.values())
        if total:
            logger.info("Cleaned up %d cached media files", total)
        return result

    # -- Helpers -------------------------------------------------------------

    def _dir_for_kind(self, kind: str) -> Path:
        mapping = {
            "image": self.images_dir,
            "audio": self.audio_dir,
            "video": self.video_dir,
            "document": self.documents_dir,
        }
        return mapping.get(kind, self.documents_dir)

    @staticmethod
    def _looks_like_image(data: bytes) -> bool:
        """Check if data starts with a known image magic byte sequence."""
        if len(data) < 4:
            return False
        for sig in _IMAGE_SIGNATURES:
            if data[:len(sig)] == sig:
                return True
        # Check WEBP
        if len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return True
        return False

    @staticmethod
    def classify_ext(filename: str, mime_type: str = "") -> str:
        """Best-effort file extension from filename, then MIME fallback."""
        if filename:
            ext = os.path.splitext(filename)[1].lower()
            if ext:
                return ext
        return ""
