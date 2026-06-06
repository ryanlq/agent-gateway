"""
Unified message types for cross-platform agent communication.

All platform adapters produce ``MessageEvent`` on the inbound path and
consume ``SendResult`` on the outbound path.  This module defines those
shared data structures so the gateway core is never coupled to any
specific messaging platform.

Design principles (drawn from the Hermes agent gateway):
  - Inbound messages are normalised into a single ``MessageEvent`` struct.
  - Outbound results carry a ``retryable`` flag so the runner can decide
    whether to re-queue on transient failures.
  - Media is represented as local file paths (downloaded by the adapter)
    rather than ephemeral platform URLs, so downstream tools (vision, STT)
    can access them reliably.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Optional


# ---------------------------------------------------------------------------
# Message types
# ---------------------------------------------------------------------------

class MessageType(Enum):
    """Categories of inbound messages every adapter must classify."""

    TEXT = "text"
    PHOTO = "photo"
    VIDEO = "video"
    AUDIO = "audio"
    VOICE = "voice"
    DOCUMENT = "document"
    STICKER = "sticker"
    LOCATION = "location"
    COMMAND = "command"          # /command style messages


class ChatType(Enum):
    """Kind of conversation the message arrived in."""

    DM = "dm"
    GROUP = "group"
    CHANNEL = "channel"
    THREAD = "thread"


# ---------------------------------------------------------------------------
# Message source — uniquely identifies where a message came from
# ---------------------------------------------------------------------------

@dataclass
class MessageSource:
    """
    Four-dimensional identity tuple that isolates conversations:

        (platform, user_id, chat_id, thread_id)

    This mirrors the Hermes gateway's session isolation model — two users
    on the same platform get independent sessions, and a single user in
    two different group chats also gets independent sessions.
    """

    platform: str
    """Platform name, e.g. ``"telegram"``, ``"discord"``."""

    user_id: str
    """Sender's platform-specific user ID."""

    chat_id: str
    """Conversation / channel / group ID."""

    thread_id: Optional[str] = None
    """Thread / topic / forum ID (``None`` for flat chats)."""

    chat_type: ChatType = ChatType.DM
    """Kind of conversation."""

    display_name: str = ""
    """Human-readable sender name (for system prompt injection)."""

    def session_key(self) -> str:
        """Return a stable key for session lookup.

        Format: ``platform:user_id:chat_id[:thread_id]``
        """
        parts = [self.platform, self.user_id, self.chat_id]
        if self.thread_id:
            parts.append(self.thread_id)
        return ":".join(parts)


# ---------------------------------------------------------------------------
# Inbound message event
# ---------------------------------------------------------------------------

@dataclass
class MessageEvent:
    """
    Normalised inbound message produced by every platform adapter.

    Adapters translate platform-specific updates into this structure so the
    gateway core can process them uniformly.
    """

    text: str
    """Message text content (captions for media become text)."""

    message_type: MessageType = MessageType.TEXT
    """Category of the message."""

    source: Optional[MessageSource] = None
    """Origin metadata (who sent it, from where)."""

    # -- Media ---------------------------------------------------------------
    media_urls: list[str] = field(default_factory=list)
    """Local file paths of downloaded media (images, videos, audio, docs)."""

    media_types: list[str] = field(default_factory=list)
    """MIME types corresponding to *media_urls*."""

    # -- Reply context -------------------------------------------------------
    message_id: Optional[str] = None
    """Platform-specific message ID of this event."""

    reply_to_message_id: Optional[str] = None
    """If this message is a reply, the ID of the parent message."""

    reply_to_text: Optional[str] = None
    """Text content of the replied-to message (for context injection)."""

    # -- Metadata ------------------------------------------------------------
    raw_message: Any = None
    """Original platform update object (for adapter-internal use)."""

    timestamp: datetime = field(default_factory=datetime.now)
    """When the event was created."""

    internal: bool = False
    """True for synthetic events that bypass authorisation checks."""

    # -- Skill / prompt bindings (advanced) ----------------------------------
    auto_skill: Optional[str | list[str]] = None
    """Skill(s) to auto-load for this conversation (e.g. channel bindings)."""

    channel_prompt: Optional[str] = None
    """Per-channel ephemeral system prompt (never persisted to history)."""

    channel_context: Optional[str] = None
    """Recovered context from missed messages between bot turns."""

    # -- Helpers -------------------------------------------------------------

    def is_command(self) -> bool:
        """Return True if this is a ``/command`` message."""
        return self.text.startswith("/")

    def get_command(self) -> Optional[str]:
        """Extract the command name (without leading ``/`` and ``@bot`` suffix)."""
        if not self.is_command():
            return None
        parts = self.text.split(maxsplit=1)
        raw = parts[0][1:].lower() if parts else None
        if raw and "@" in raw:
            raw = raw.split("@", 1)[0]
        # Reject file paths — valid command names never contain /
        if raw and "/" in raw:
            return None
        return raw

    def get_command_args(self) -> str:
        """Return everything after the command word."""
        if not self.is_command():
            return self.text
        parts = self.text.split(maxsplit=1)
        return parts[1] if len(parts) > 1 else ""


# ---------------------------------------------------------------------------
# Outbound send result
# ---------------------------------------------------------------------------

@dataclass
class SendResult:
    """
    Result of an outbound ``send`` / ``edit`` / ``media`` call.

    Carries enough information for the runner to decide whether to retry
    and how to address follow-up edits.
    """

    success: bool
    """Whether the send succeeded."""

    message_id: Optional[str] = None
    """Platform-assigned message ID (for subsequent edits / deletes)."""

    error: Optional[str] = None
    """Error description on failure."""

    retryable: bool = False
    """True for transient errors (network, rate-limit) — runner may retry."""

    continuation_message_ids: tuple[str, ...] = ()
    """Extra message IDs when the payload was split across multiple messages."""

    raw_response: Any = None
    """Original platform API response (for adapter-internal use)."""


# ---------------------------------------------------------------------------
# Ephemeral reply — auto-delete after TTL
# ---------------------------------------------------------------------------

class EphemeralReply(str):
    """
    A reply string that requests auto-deletion after ``ttl_seconds``.

    Slash-command handlers return this wrapper instead of a plain ``str``
    to request that the reply be deleted on platforms that support it.

    Usage::

        return EphemeralReply("✅ Done", ttl_seconds=10)

    Subclassing ``str`` keeps the wrapper transparent to existing code that
    treats handler return values as plain text.
    """

    ttl_seconds: Optional[int]

    def __new__(cls, text: str, ttl_seconds: Optional[int] = None) -> EphemeralReply:
        instance = super().__new__(cls, text)
        instance.ttl_seconds = ttl_seconds
        return instance

    @property
    def text(self) -> str:
        return str.__str__(self)


# ---------------------------------------------------------------------------
# Processing outcome (for lifecycle hooks)
# ---------------------------------------------------------------------------

class ProcessingOutcome(Enum):
    """Result classification for message-processing lifecycle hooks."""

    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"


# ---------------------------------------------------------------------------
# Command coercion helper
# ---------------------------------------------------------------------------

_PLAINTEXT_RESTART_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^(?:please\s+)?restart\s+(?:the\s+)?gateway[.!?\s]*$", re.IGNORECASE),
    re.compile(r"^(?:please\s+)?restart\s+hermes[.!?\s]*$", re.IGNORECASE),
)


def coerce_plaintext_gateway_command(event: MessageEvent) -> None:
    """Rewrite common plaintext admin phrases into slash commands.

    Keeps high-impact operational phrases (``restart gateway``) out of the
    LLM / tool path.  Scope is intentionally narrow: DM text only.
    """
    if event is None or event.message_type != MessageType.TEXT:
        return
    text = (event.text or "").strip()
    if not text or text.startswith("/"):
        return
    source = getattr(event, "source", None)
    if source is None or source.chat_type != ChatType.DM:
        return
    for pattern in _PLAINTEXT_RESTART_PATTERNS:
        if pattern.match(text):
            event.text = "/restart"
            return
