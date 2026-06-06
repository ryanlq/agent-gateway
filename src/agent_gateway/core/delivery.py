"""
Delivery routing for agent responses and scheduled outputs.

Routes messages to the appropriate destination based on:
  - Explicit targets (e.g. ``"telegram:123456789"``)
  - Platform home channels (e.g. ``"telegram"`` → default chat)
  - Origin (back to where the message came from)
  - Local (save to file)

Also includes:
  - Adaptive truncation for oversized outputs
  - Anti-loop silence-narration filtering
  - Thread / topic resolution
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

MAX_PLATFORM_OUTPUT = 4000
"""Soft cap for outbound messages (characters)."""

TRUNCATED_VISIBLE = 3800
"""Characters to keep when truncating (remainder goes to file)."""

# Regex that matches pure silence-narration tokens — anchored so legitimate
# prose containing the word "silent" is never filtered.
_SILENCE_NARRATION = re.compile(
    r"^[\s*_~`]*\(?\s*(silent|silence|no\s+response|no\s+reply)\s*\.?\)?[\s*_~`]*$"
    r"|^[\s*_~`]*[\U0001F507\.…]+[\s*_~`]*$",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _is_silence_narration(content: Optional[str]) -> bool:
    """Return True when *content* is only a silence-narration token."""
    if not content:
        return False
    stripped = content.strip()
    if not stripped or len(stripped) > 64:
        return False
    return bool(_SILENCE_NARRATION.match(stripped))


def _looks_like_int(value: Optional[str]) -> bool:
    try:
        int(value)  # type: ignore[arg-type]
        return True
    except (TypeError, ValueError):
        return False


# ---------------------------------------------------------------------------
# DeliveryTarget
# ---------------------------------------------------------------------------

@dataclass
class DeliveryTarget:
    """
    A single delivery destination.

    Formats accepted by ``parse()``::

        "origin"              → back to the message source
        "local"               → save to local file
        "telegram"            → Telegram home channel
        "telegram:123456"     → specific Telegram chat
        "telegram:123:thread" → specific thread/topic
    """

    platform: str
    """Platform name (``"telegram"``, ``"discord"``, ``"local"``)."""

    chat_id: Optional[str] = None
    """Chat / channel ID.  ``None`` means use the home channel."""

    thread_id: Optional[str] = None
    """Thread / topic ID."""

    is_origin: bool = False
    """True when routing back to the message source."""

    is_explicit: bool = False
    """True when the chat_id was explicitly specified (not auto-resolved)."""

    @classmethod
    def parse(cls, target: str, origin: Any = None) -> DeliveryTarget:
        """Parse a target string into a ``DeliveryTarget``.

        *origin* should be a ``MessageSource`` when ``target == "origin"``.
        """
        target_stripped = target.strip()
        target_lower = target_stripped.lower()

        # "origin" → route back to source
        if target_lower == "origin":
            if origin is not None:
                return cls(
                    platform=getattr(origin, "platform", "local"),
                    chat_id=getattr(origin, "chat_id", None),
                    thread_id=getattr(origin, "thread_id", None),
                    is_origin=True,
                )
            return cls(platform="local", is_origin=True)

        # "local" → file only
        if target_lower == "local":
            return cls(platform="local")

        # "platform:chat_id[:thread_id]"
        if ":" in target_stripped:
            parts = target_stripped.split(":", 2)
            platform = parts[0].lower()
            chat_id = parts[1] if len(parts) > 1 else None
            thread_id = parts[2] if len(parts) > 2 else None
            return cls(platform=platform, chat_id=chat_id, thread_id=thread_id, is_explicit=True)

        # Just a platform name (use home channel)
        return cls(platform=target_lower)

    def to_string(self) -> str:
        """Convert back to string format."""
        if self.is_origin:
            return "origin"
        if self.platform == "local":
            return "local"
        if self.chat_id and self.thread_id:
            return f"{self.platform}:{self.chat_id}:{self.thread_id}"
        if self.chat_id:
            return f"{self.platform}:{self.chat_id}"
        return self.platform


# ---------------------------------------------------------------------------
# DeliveryRouter
# ---------------------------------------------------------------------------

class DeliveryRouter:
    """
    Routes messages to appropriate destinations.

    Handles:
      - Resolving delivery targets to concrete adapters + chat IDs
      - Truncating oversized output
      - Filtering silence-narration to prevent bot-to-bot loops
      - Saving full output to disk when truncation is needed
    """

    def __init__(
        self,
        adapters: dict[str, Any],
        output_dir: Optional[Path] = None,
        filter_silence: bool = True,
    ) -> None:
        """
        Args:
            adapters: Dict mapping platform names to adapter instances.
            output_dir: Directory for local delivery / truncated output.
            filter_silence: Whether to drop silence-narration outbound.
        """
        self.adapters = adapters
        self.output_dir = output_dir or Path.home() / ".agent_gateway" / "output"
        self.filter_silence = filter_silence

    async def deliver(
        self,
        content: str,
        target: DeliveryTarget,
        *,
        job_id: Optional[str] = None,
        job_name: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        """Deliver *content* to a single target.

        Returns a dict with ``success`` and delivery details.
        """
        try:
            if target.platform == "local":
                result = self._deliver_local(content, job_id, job_name, metadata)
                return {"success": True, "target": target.to_string(), "result": result}

            return await self._deliver_to_platform(target, content, metadata)

        except Exception as exc:
            logger.error("Delivery to %s failed: %s", target.to_string(), exc)
            return {"success": False, "target": target.to_string(), "error": str(exc)}

    async def deliver_multi(
        self,
        content: str,
        targets: list[DeliveryTarget],
        **kwargs: Any,
    ) -> dict[str, dict[str, Any]]:
        """Deliver *content* to multiple targets.

        Returns a dict mapping target strings to result dicts.
        """
        results: dict[str, dict[str, Any]] = {}
        for target in targets:
            results[target.to_string()] = await self.deliver(content, target, **kwargs)
        return results

    # -- Platform delivery ---------------------------------------------------

    async def _deliver_to_platform(
        self,
        target: DeliveryTarget,
        content: str,
        metadata: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Deliver content to a messaging platform."""
        adapter = self.adapters.get(target.platform)
        if not adapter:
            raise ValueError(f"No adapter configured for '{target.platform}'")

        if not target.chat_id:
            raise ValueError(f"No chat ID for '{target.platform}' delivery")

        # Truncate oversized output
        outbound = content
        if len(content) > MAX_PLATFORM_OUTPUT:
            job_id = (metadata or {}).get("job_id", "unknown")
            saved_path = self._save_full_output(content, job_id)
            logger.info("Output truncated (%d chars) — full: %s", len(content), saved_path)
            outbound = (
                content[:TRUNCATED_VISIBLE]
                + f"\n\n... [truncated, full output saved to {saved_path}]"
            )

        # Anti-loop: drop silence narration
        if self.filter_silence and _is_silence_narration(outbound):
            logger.warning("Dropped silence-narration to %s/%s", target.platform, target.chat_id)
            return {"success": True, "filtered": "silence_narration", "delivered": False}

        # Build send metadata
        send_meta = dict(metadata or {})
        if target.thread_id:
            send_meta["thread_id"] = target.thread_id

        result = await adapter.send(target.chat_id, outbound, metadata=send_meta or None)

        if getattr(result, "success", False):
            return {"success": True, "message_id": getattr(result, "message_id", None)}
        else:
            error = getattr(result, "error", "Unknown error")
            raise RuntimeError(error)

    # -- Local delivery ------------------------------------------------------

    def _deliver_local(
        self,
        content: str,
        job_id: Optional[str],
        job_name: Optional[str],
        metadata: Optional[dict[str, Any]],
    ) -> dict[str, Any]:
        """Save content to a local file."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        if job_id:
            output_path = self.output_dir / job_id / f"{timestamp}.md"
        else:
            output_path = self.output_dir / "misc" / f"{timestamp}.md"

        output_path.parent.mkdir(parents=True, exist_ok=True)

        lines = [
            f"# {job_name or 'Delivery Output'}",
            "",
            f"**Timestamp:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        ]
        if job_id:
            lines.append(f"**Job ID:** {job_id}")
        if metadata:
            for key, value in metadata.items():
                lines.append(f"**{key}:** {value}")
        lines.extend(["", "---", "", content])

        output_path.write_text("\n".join(lines), encoding="utf-8")
        return {"path": str(output_path), "timestamp": timestamp}

    def _save_full_output(self, content: str, job_id: str) -> Path:
        """Save full (untruncated) output to disk."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        path = self.output_dir / f"{job_id}_{timestamp}.txt"
        path.write_text(content, encoding="utf-8")
        return path
