"""
Base platform adapter interface.

All platform adapters inherit from ``BasePlatformAdapter`` and implement
the required abstract methods (``connect``, ``disconnect``, ``send``).
Everything else has a sensible default implementation that degrades
gracefully — for example ``send_image`` falls back to sending the URL
as plain text.

The minimal adapter contract is deliberately tiny so new platforms can
be onboarded quickly.  Advanced features (streaming, editing, typing
indicators, inline buttons) are opt-in overrides.
"""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Awaitable, Callable, Optional

from agent_gateway.core.message import (
    ChatType,
    MessageEvent,
    SendResult,
)

logger = logging.getLogger(__name__)

# Type alias for the message handler callback that the runner injects.
MessageHandler = Callable[[MessageEvent], Awaitable[Optional[str]]]

# Type alias for the busy-session handler.
BusySessionHandler = Callable[[MessageEvent, str], Awaitable[bool]]


@dataclass
class TextDebounceState:
    """Tracks in-flight debounce for rapid text bursts."""

    event: MessageEvent
    task: asyncio.Task | None
    first_ts: float
    last_ts: float


class BasePlatformAdapter(ABC):
    """
    Abstract base class for all platform adapters.

    Subclasses implement platform-specific logic for:
      - Connecting and authenticating
      - Receiving messages (via the injected ``_message_handler``)
      - Sending responses, media, and streaming updates

    **Minimal implementation**: override ``connect``, ``disconnect``, ``send``.

    **Optional overrides**: ``edit_message``, ``delete_message``,
    ``send_image``, ``send_voice``, ``send_document``, ``send_typing``,
    ``render_message_event``, ``format_tool_event``, etc.
    """

    # Subclass class-level attribute: set True when the adapter manages
    # its own access policy (e.g. WeChat dm_policy) so the runner skips
    # the env-based default-deny.
    ENFORCES_OWN_ACCESS_POLICY: bool = False

    # Subclass class-level attribute: set True if ``edit_message`` needs
    # an explicit ``finalize=True`` call to close a streaming lifecycle
    # (e.g. rich-card AI assistant surfaces).
    REQUIRES_EDIT_FINALIZE: bool = False

    # ------------------------------------------------------------------
    # Construction
    # ------------------------------------------------------------------

    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self._message_handler: Optional[MessageHandler] = None
        self._running: bool = False
        self._name: str = self.__class__.__name__

        # Session-level concurrency guard: only one message processed per
        # session at a time.  Incoming messages during active processing
        # are queued or rejected based on ``_busy_text_mode``.
        self._active_sessions: dict[str, asyncio.Event] = {}
        self._pending_messages: dict[str, MessageEvent] = {}
        self._session_tasks: dict[str, asyncio.Task] = {}

        # Busy-mode configuration
        self._busy_text_mode: str = "queue"     # "queue" | "reject"
        self._busy_text_debounce_seconds: float = 0.35

        self._text_debounce: dict[str, TextDebounceState] = {}
        self._background_tasks: set[asyncio.Task] = set()

        # Post-delivery callbacks keyed by session_key
        self._post_delivery_callbacks: dict[str, Any] = {}

        # Busy session handler — injected by the runner
        self._busy_session_handler: Optional[BusySessionHandler] = None

        # Typing pause set
        self._typing_paused: set[str] = set()

        # Fatal error state
        self._fatal_error_code: Optional[str] = None
        self._fatal_error_message: Optional[str] = None
        self._fatal_error_retryable: bool = True

        # Backlog recovery state
        self._backlog_recovered: bool = False
        self._last_disconnect_time: Optional[float] = None

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def name(self) -> str:
        """Human-readable adapter name."""
        return self._name

    @property
    def is_connected(self) -> bool:
        """Whether the adapter is currently connected."""
        return self._running

    @property
    def has_fatal_error(self) -> bool:
        return self._fatal_error_message is not None

    @property
    def fatal_error_message(self) -> Optional[str]:
        return self._fatal_error_message

    @property
    def fatal_error_code(self) -> Optional[str]:
        return self._fatal_error_code

    @property
    def fatal_error_retryable(self) -> bool:
        return self._fatal_error_retryable

    @property
    def message_len_fn(self) -> Callable[[str], int]:
        """Length function for this platform.

        Override for platforms that measure message size differently
        (e.g. Telegram counts UTF-16 code units).
        """
        return len

    @property
    def max_message_length(self) -> int:
        """Maximum message length in platform-native units. 0 = unlimited."""
        return 0

    # ------------------------------------------------------------------
    # Abstract methods — MUST implement
    # ------------------------------------------------------------------

    @abstractmethod
    async def connect(self) -> bool:
        """Connect to the platform and start receiving messages.

        Returns ``True`` on success.
        """

    @abstractmethod
    async def disconnect(self) -> None:
        """Disconnect from the platform and clean up resources."""

    async def recover_backlog(self) -> int:
        """Recover messages missed during the offline period.

        Called by the runner once after ``connect()`` succeeds.
        Subclasses override this to fetch and replay missed messages
        through ``handle_message()``.

        Returns the number of recovered messages (0 = no-op).
        """
        return 0

    @abstractmethod
    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message to a chat.

        Args:
            chat_id: Target chat / channel / conversation ID.
            content: Message text (may contain markdown).
            reply_to: Optional message ID to reply to.
            metadata: Platform-specific options (thread_id, parse_mode, etc.).

        Returns:
            ``SendResult`` with success status and platform message ID.
        """

    # ------------------------------------------------------------------
    # Optional methods — override for richer platform support
    # ------------------------------------------------------------------

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit a previously sent message.

        Platforms that don't support editing leave this default
        (returns ``success=False``) and callers fall back to sending
        a new message.

        ``finalize=True`` signals the last edit in a streaming sequence.
        Most platforms treat it as a no-op.
        """
        return SendResult(success=False, error="Not supported")

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a previously sent message.  Return ``True`` on success."""
        return False

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        """Send a typing indicator (no-op by default)."""

    async def stop_typing(self, chat_id: str) -> None:
        """Stop a persistent typing indicator (no-op by default)."""

    # -- Draft streaming (advanced) -----------------------------------------

    def supports_edit(self) -> bool:
        """Whether this adapter supports editing previously sent messages."""
        return False

    def supports_draft_streaming(
        self,
        chat_type: Optional[ChatType] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> bool:
        """Whether this adapter supports native streaming-draft updates."""
        return False

    async def send_draft(
        self,
        chat_id: str,
        draft_id: int,
        content: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send or update an animated streaming-draft preview."""
        return SendResult(success=False, error="Not supported")

    # -- Media delivery -----------------------------------------------------

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image natively.  Default: send URL as text."""
        text = f"{caption}\n{image_url}" if caption else image_url
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    async def send_image_file(
        self,
        chat_id: str,
        image_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a local image file.  Default: send path as text."""
        text = f"🖼️ Image: {image_path}"
        if caption:
            text = f"{caption}\n{text}"
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send audio as a native voice message.  Default: send path as text."""
        text = f"🔊 Audio: {audio_path}"
        if caption:
            text = f"{caption}\n{text}"
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    async def send_video(
        self,
        chat_id: str,
        video_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a video natively.  Default: send path as text."""
        text = f"🎬 Video: {video_path}"
        if caption:
            text = f"{caption}\n{text}"
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a document / file attachment.  Default: send path as text."""
        text = f"📎 File: {file_path}"
        if caption:
            text = f"{caption}\n{text}"
        return await self.send(chat_id=chat_id, content=text, reply_to=reply_to, metadata=metadata)

    async def send_animation(
        self,
        chat_id: str,
        animation_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send an animated GIF natively.  Default: delegate to send_image."""
        return await self.send_image(
            chat_id=chat_id, image_url=animation_url,
            caption=caption, reply_to=reply_to, metadata=metadata,
        )

    async def send_multiple_images(
        self,
        chat_id: str,
        images: list[tuple[str, str]],
        metadata: Optional[dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        """Send a batch of images (URL, alt_text)."""
        for image_url, alt_text in images:
            if human_delay > 0:
                await asyncio.sleep(human_delay)
            try:
                if image_url.lower().split("?")[0].endswith(".gif"):
                    await self.send_animation(chat_id, image_url, alt_text, metadata=metadata)
                else:
                    await self.send_image(chat_id, image_url, alt_text, metadata=metadata)
            except Exception as exc:
                logger.error("[%s] Failed to send image %s: %s", self.name, image_url, exc)

    # -- Structured stream-event rendering ----------------------------------

    def render_message_event(self, event: Any, sink: Any) -> None:
        """Render a structured stream event onto *sink* (a ``StreamConsumer``).

        Override to customise how your platform presents streaming events.
        """
        from agent_gateway.core.stream_events import MessageChunk, MessageStop

        if isinstance(event, MessageChunk):
            if event.text:
                sink.on_delta(event.text)
        elif isinstance(event, MessageStop):
            if not event.final:
                sink.on_segment_break()

    def format_tool_event(self, event: Any, *, mode: str = "all",
                          preview_max_len: int = 40) -> Optional[str]:
        """Return rendered tool chrome for a ToolCallChunk, or None to hide it."""
        from agent_gateway.core.stream_events import ToolCallChunk
        if not isinstance(event, ToolCallChunk):
            return None

        emoji = "⚙️"  # default tool emoji
        if mode == "verbose":
            if event.args:
                import json
                args_str = json.dumps(event.args, ensure_ascii=False, default=str)
                if preview_max_len > 0 and len(args_str) > preview_max_len:
                    args_str = args_str[:preview_max_len - 3] + "..."
                return f"{emoji} {event.tool_name}({list(event.args.keys())})\n{args_str}"
            if event.preview:
                return f'{emoji} {event.tool_name}: "{event.preview}"'
            return f"{emoji} {event.tool_name}..."

        preview = event.preview
        if preview:
            cap = preview_max_len if preview_max_len > 0 else 40
            if len(preview) > cap:
                preview = preview[:cap - 3] + "..."
            return f'{emoji} {event.tool_name}: "{preview}"'
        return f"{emoji} {event.tool_name}..."

    # -- Interactive prompts -------------------------------------------------

    async def send_clarify(
        self,
        chat_id: str,
        question: str,
        choices: Optional[list[str]] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a clarification prompt.  Default: numbered text list."""
        if choices:
            lines = [f"❓ {question}", ""]
            for i, choice in enumerate(choices, 1):
                lines.append(f"  {i}. {choice}")
            lines.append("")
            lines.append("Reply with the number, the option text, or your own answer.")
            text = "\n".join(lines)
        else:
            text = f"❓ {question}"
        return await self.send(chat_id=chat_id, content=text, metadata=metadata)

    async def send_slash_confirm(
        self,
        chat_id: str,
        title: str,
        message: str,
        session_key: str,
        confirm_id: str,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a three-option confirmation prompt.  Default: plain text."""
        text = f"⚠️ {title}\n\n{message}\n\nReply: /approve, /always, or /cancel"
        return await self.send(chat_id=chat_id, content=text, metadata=metadata)

    async def send_private_notice(
        self,
        chat_id: str,
        user_id: Optional[str],
        content: str,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a private notice.  Default: normal send."""
        return await self.send(chat_id=chat_id, content=content, reply_to=reply_to, metadata=metadata)

    # -- Thread / handoff ----------------------------------------------------

    async def create_handoff_thread(
        self,
        parent_chat_id: str,
        name: str,
    ) -> Optional[str]:
        """Create a new thread for session handoff.  Return thread ID or None."""
        return None

    # -- TTS -----------------------------------------------------------------

    def prepare_tts_text(self, text: str) -> str:
        """Prepare text for TTS.  Default: strip markdown, truncate to 4000."""
        import re
        return re.sub(r"[*_`#\[\]()]", "", text)[:4000].strip()

    async def play_tts(
        self,
        chat_id: str,
        audio_path: str,
        **kwargs,
    ) -> SendResult:
        """Play auto-TTS audio.  Default: delegate to send_voice."""
        return await self.send_voice(chat_id=chat_id, audio_path=audio_path, **kwargs)

    # ------------------------------------------------------------------
    # Handler injection — called by the runner
    # ------------------------------------------------------------------

    def set_message_handler(self, handler: MessageHandler) -> None:
        """Set the callback that processes inbound messages."""
        self._message_handler = handler

    def set_busy_session_handler(self, handler: Optional[BusySessionHandler]) -> None:
        """Set the handler for messages arriving during active processing."""
        self._busy_session_handler = handler

    def set_session_store(self, session_store: Any) -> None:
        """Inject the session store for checking active sessions."""
        self._session_store = session_store

    # ------------------------------------------------------------------
    # Message dispatch — called by adapter subclasses when a platform
    # message arrives
    # ------------------------------------------------------------------

    async def handle_message(self, event: MessageEvent) -> None:
        """Entry point for inbound messages from platform adapters.

        Subclasses call this when they receive a message from the platform.
        It manages session-level concurrency and dispatches to the runner's
        message handler.
        """
        if not self._message_handler:
            logger.warning("[%s] No message handler set, dropping message", self.name)
            return

        source = event.source
        if source is None:
            logger.warning("[%s] Message without source, dropping", self.name)
            return

        session_key = source.session_key()

        # Check if this session is already being processed
        active = self._active_sessions.get(session_key)
        if active is not None and not active.is_set():
            # Session is busy
            if self._busy_text_mode == "queue":
                self._pending_messages[session_key] = event
                logger.debug("[%s] Queuing message for busy session %s", self.name, session_key)
            elif self._busy_session_handler:
                await self._busy_session_handler(event, session_key)
            return

        # Spawn background task for processing
        task = asyncio.create_task(
            self._process_message_background(event, session_key)
        )
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def _process_message_background(self, event: MessageEvent, session_key: str) -> None:
        """Process a message in the background with session-level locking."""
        # Acquire session lock
        lock = asyncio.Event()
        self._active_sessions[session_key] = lock

        try:
            # Send typing indicator
            if event.source:
                await self.send_typing(event.source.chat_id)

            # Dispatch to the runner's handler
            response = await self._message_handler(event)  # type: ignore[misc]

            # Send response if handler returned text
            if response and event.source:
                from agent_gateway.core.message import EphemeralReply
                result = await self.send(
                    chat_id=event.source.chat_id,
                    content=str(response),
                    reply_to=event.reply_to_message_id,
                )

                # Handle ephemeral replies
                if isinstance(response, EphemeralReply) and result.success and result.message_id:
                    ttl = response.ttl_seconds
                    if ttl:
                        await self._schedule_ephemeral_delete(
                            event.source.chat_id, result.message_id, ttl
                        )

        except Exception as exc:
            logger.exception("[%s] Error processing message for %s: %s", self.name, session_key, exc)
        finally:
            # Release session lock
            lock.set()
            self._active_sessions.pop(session_key, None)

            # Process queued message if any
            pending = self._pending_messages.pop(session_key, None)
            if pending:
                asyncio.create_task(self._process_message_background(pending, session_key))

    async def _schedule_ephemeral_delete(self, chat_id: str, message_id: str, ttl: int) -> None:
        """Delete a message after *ttl* seconds."""
        async def _delete():
            try:
                await asyncio.sleep(max(1, ttl))
                await self.delete_message(chat_id=chat_id, message_id=message_id)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.debug("[%s] Ephemeral delete failed: %s", self.name, exc)

        try:
            asyncio.create_task(_delete())
        except RuntimeError:
            pass

    # ------------------------------------------------------------------
    # State management
    # ------------------------------------------------------------------

    def _mark_connected(self) -> None:
        self._running = True
        self._fatal_error_code = None
        self._fatal_error_message = None

    def _mark_disconnected(self) -> None:
        import time as _time
        self._last_disconnect_time = _time.time()
        self._running = False

    def _set_fatal_error(self, code: str, message: str, *, retryable: bool = True) -> None:
        self._running = False
        self._fatal_error_code = code
        self._fatal_error_message = message
        self._fatal_error_retryable = retryable

    # ------------------------------------------------------------------
    # Message helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _merge_caption(existing: Optional[str], incoming: Optional[str]) -> str:
        """Merge two caption strings."""
        parts = [s for s in (existing, incoming) if s]
        return "\n".join(parts)

    @staticmethod
    def extract_images(content: str) -> tuple[list[tuple[str, str]], str]:
        """Extract image URLs from markdown and HTML image tags.

        Returns (list of (url, alt_text) pairs, cleaned content).
        """
        import re

        images: list[tuple[str, str]] = []
        cleaned = content

        # Markdown: ![alt](url)
        md_pattern = r"!\[([^\]]*)\]\((https?://[^\s\)]+)\)"
        for match in re.finditer(md_pattern, content):
            images.append((match.group(2), match.group(1)))

        # HTML: <img src="url">
        html_pattern = r'<img\s+src=["\']?(https?://[^\s"\'<>]+)["\']?\s*/?>'
        for match in re.finditer(html_pattern, content):
            images.append((match.group(1), ""))

        if images:
            extracted_urls = {url for url, _ in images}

            def _remove(match: re.Match) -> str:
                url = match.group(2) if match.lastindex and match.lastindex >= 2 else match.group(1)
                return "" if url in extracted_urls else match.group(0)

            cleaned = re.sub(md_pattern, _remove, cleaned)
            cleaned = re.sub(html_pattern, _remove, cleaned)
            cleaned = re.sub(r"\n{3,}", "\n\n", cleaned).strip()

        return images, cleaned
