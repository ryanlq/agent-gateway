"""
Telegram platform adapter.

Uses the ``python-telegram-bot`` library for long-polling or webhook-based
message reception.  Supports:
  - Text, photo, video, voice, document, sticker messages
  - Message editing (streaming)
  - Inline keyboards (clarify / confirm)
  - Draft streaming (Bot API 9.5+)
  - Typing indicators

Requirements::

    pip install agent-gateway[telegram]
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Optional

from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.message import (
    ChatType,
    MessageEvent,
    MessageSource,
    MessageType,
    SendResult,
)
from agent_gateway.core.registry import PlatformEntry, registry
from agent_gateway.media.cache import MediaCache

logger = logging.getLogger(__name__)


def _check_telegram_deps() -> bool:
    """Check if python-telegram-bot is available."""
    try:
        import telegram  # noqa: F401
        return True
    except ImportError:
        return False


class TelegramAdapter(BasePlatformAdapter):
    """Telegram Bot API adapter."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._token = config.get("token", "")
        self._bot: Any = None
        self._application: Any = None
        self._polling_task: Optional[asyncio.Task] = None
        self._media_cache = MediaCache()
        self._name = "Telegram"

    # -- Abstract method implementations -------------------------------------

    async def connect(self) -> bool:
        """Connect to Telegram via long-polling."""
        if not self._token:
            self._set_fatal_error("no_token", "TELEGRAM_TOKEN not set", retryable=False)
            return False

        try:
            from telegram import Bot
            from telegram.ext import ApplicationBuilder, MessageHandler, filters

            self._bot = Bot(token=self._token)
            me = await self._bot.get_me()
            self._name = f"Telegram (@{me.username})"

            # Build application
            app = (
                ApplicationBuilder()
                .token(self._token)
                .build()
            )

            # Register message handlers
            app.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, self._on_update))

            self._application = app

            # Start polling in background
            await app.initialize()
            await app.start()
            self._polling_task = asyncio.create_task(self._run_polling(app))

            self._mark_connected()
            return True

        except Exception as exc:
            self._set_fatal_error("connect_failed", str(exc))
            logger.error("Telegram connect failed: %s", exc)
            return False

    async def disconnect(self) -> None:
        """Stop polling and disconnect."""
        if self._application:
            try:
                await self._application.stop()
                await self._application.shutdown()
            except Exception:
                pass
        if self._polling_task:
            self._polling_task.cancel()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a text message via the Telegram Bot API."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            # Truncate to Telegram's 4096 UTF-16 code unit limit
            text = self._truncate_utf16(content, 4096)

            kwargs: dict[str, Any] = {
                "chat_id": int(chat_id),
                "text": text,
                "parse_mode": metadata.get("parse_mode", "Markdown") if metadata else None,
            }
            if reply_to:
                kwargs["reply_to_message_id"] = int(reply_to)

            # Thread/topic support
            if metadata and "thread_id" in metadata:
                kwargs["message_thread_id"] = int(metadata["thread_id"])

            msg = await self._bot.send_message(**kwargs)
            return SendResult(success=True, message_id=str(msg.message_id))

        except Exception as exc:
            error_str = str(exc)
            retryable = any(s in error_str.lower() for s in ("retry", "flood", "429", "timeout"))
            return SendResult(success=False, error=error_str, retryable=retryable)

    # -- Optional overrides --------------------------------------------------

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        """Edit an existing message."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")

        try:
            text = self._truncate_utf16(content, 4096)
            await self._bot.edit_message_text(
                chat_id=int(chat_id),
                message_id=int(message_id),
                text=text,
            )
            return SendResult(success=True, message_id=message_id)
        except Exception as exc:
            error_str = str(exc)
            # "message is not modified" is not a real error
            if "not modified" in error_str.lower():
                return SendResult(success=True, message_id=message_id)
            return SendResult(success=False, error=error_str)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        """Delete a message."""
        if not self._bot:
            return False
        try:
            await self._bot.delete_message(chat_id=int(chat_id), message_id=int(message_id))
            return True
        except Exception:
            return False

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        """Send a typing indicator."""
        if self._bot:
            try:
                await self._bot.send_chat_action(chat_id=int(chat_id), action="typing")
            except Exception:
                pass

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a photo."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")
        try:
            msg = await self._bot.send_photo(
                chat_id=int(chat_id),
                photo=image_url,
                caption=caption[:1024] if caption else None,
                reply_to_message_id=int(reply_to) if reply_to else None,
            )
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_voice(
        self,
        chat_id: str,
        audio_path: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a voice message."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")
        try:
            with open(audio_path, "rb") as f:
                msg = await self._bot.send_voice(
                    chat_id=int(chat_id),
                    voice=f,
                    caption=caption[:1024] if caption else None,
                )
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a document / file attachment."""
        if not self._bot:
            return SendResult(success=False, error="Not connected")
        try:
            with open(file_path, "rb") as f:
                msg = await self._bot.send_document(
                    chat_id=int(chat_id),
                    document=f,
                    filename=file_name,
                    caption=caption[:1024] if caption else None,
                )
            return SendResult(success=True, message_id=str(msg.message_id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    # -- Properties ----------------------------------------------------------

    @property
    def max_message_length(self) -> int:
        return 4096

    # -- Internal ------------------------------------------------------------

    async def _run_polling(self, app: Any) -> None:
        """Run the polling loop."""
        try:
            await app.updater.start_polling(drop_pending_updates=True)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Telegram polling error: %s", exc)
            self._set_fatal_error("polling_error", str(exc))

    async def _on_update(self, update: Any, context: Any) -> None:
        """Handle an incoming Telegram update."""
        message = update.effective_message
        if not message:
            return

        # Build MessageSource
        chat = message.chat
        chat_type = ChatType.DM if chat.type == "private" else ChatType.GROUP
        thread_id = str(message.message_thread_id) if message.message_thread_id else None

        source = MessageSource(
            platform="telegram",
            user_id=str(message.from_user.id),
            chat_id=str(chat.id),
            thread_id=thread_id,
            chat_type=chat_type,
            display_name=message.from_user.full_name,
        )

        # Determine message type and content
        text = message.text or message.caption or ""
        media_urls: list[str] = []
        media_types: list[str] = []
        msg_type = MessageType.TEXT

        if message.photo:
            msg_type = MessageType.PHOTO
            # Download the highest-resolution photo
            try:
                photo = message.photo[-1]  # Largest size
                file = await photo.get_file()
                data = await file.download_as_bytearray()
                path = self._media_cache.save_image(bytes(data), ext=".jpg")
                media_urls.append(path)
                media_types.append("image/jpeg")
            except Exception as exc:
                logger.warning("Failed to download photo: %s", exc)

        elif message.voice:
            msg_type = MessageType.VOICE
            try:
                file = await message.voice.get_file()
                data = await file.download_as_bytearray()
                path = self._media_cache.save_audio(bytes(data), ext=".ogg")
                media_urls.append(path)
                media_types.append("audio/ogg")
            except Exception as exc:
                logger.warning("Failed to download voice: %s", exc)

        elif message.document:
            msg_type = MessageType.DOCUMENT

        elif message.sticker:
            msg_type = MessageType.STICKER

        elif message.video:
            msg_type = MessageType.VIDEO

        # Build and dispatch the event
        event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            media_urls=media_urls,
            media_types=media_types,
            message_id=str(message.message_id),
            reply_to_message_id=str(message.reply_to_message.message_id) if message.reply_to_message else None,
            raw_message=message,
        )

        await self.handle_message(event)

    @staticmethod
    def _truncate_utf16(text: str, limit: int) -> str:
        """Truncate text to fit within Telegram's UTF-16 code unit limit."""
        encoded = text.encode("utf-16-le")
        code_units = len(encoded) // 2
        if code_units <= limit:
            return text
        # Binary search for the longest safe prefix
        lo, hi = 0, len(text)
        while lo < hi:
            mid = (lo + hi + 1) // 2
            if len(text[:mid].encode("utf-16-le")) // 2 <= limit:
                lo = mid
            else:
                hi = mid - 1
        return text[:lo]


def register_telegram() -> None:
    """Register the Telegram adapter with the global registry."""
    from agent_gateway.core.registry import EnvVarDef

    registry.register(PlatformEntry(
        name="telegram",
        label="Telegram",
        adapter_factory=lambda cfg: TelegramAdapter(cfg),
        check_fn=_check_telegram_deps,
        install_hint="pip install agent-gateway[telegram]",
        required_env=["TELEGRAM_TOKEN"],
        max_message_length=4096,
        emoji="💬",
        platform_hint="You are on Telegram. Prefer concise responses. Markdown is supported.",
        source="builtin",
        env_var_defs=[
            EnvVarDef(
                key="TELEGRAM_TOKEN",
                description="Bot token from @BotFather",
                prompt="123456:ABC-DEF...",
                is_password=True,
                required=True,
                url="https://core.telegram.org/bots#how-do-i-create-a-bot",
            ),
        ],
    ))
