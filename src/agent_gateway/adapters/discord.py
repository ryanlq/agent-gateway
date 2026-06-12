"""
Discord platform adapter.

Uses ``discord.py`` for gateway-based message reception.  Supports:
  - Text, image, file attachments
  - Message editing (streaming)
  - Typing indicators
  - Thread support

Requirements::

    pip install agent-gateway[discord]
"""

from __future__ import annotations

import asyncio
import logging
import os
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

logger = logging.getLogger(__name__)


def _check_discord_deps() -> bool:
    try:
        import discord  # noqa: F401
        return True
    except ImportError:
        return False


class DiscordAdapter(BasePlatformAdapter):
    """Discord bot adapter using discord.py."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        self._token = config.get("token") or os.getenv("DISCORD_TOKEN", "")
        self._client: Any = None
        self._name = "Discord"

    async def connect(self) -> bool:
        if not self._token:
            self._set_fatal_error("no_token", "DISCORD_TOKEN not set", retryable=False)
            return False

        try:
            import discord

            intents = discord.Intents.default()
            intents.message_content = True

            client = discord.Client(intents=intents)

            @client.event
            async def on_message(message: discord.Message) -> None:
                await self._on_discord_message(message)

            @client.event
            async def on_ready() -> None:
                self._name = f"Discord ({client.user})"
                logger.info("Discord bot ready: %s", client.user)

            self._client = client

            # Start the Discord gateway in a background task
            asyncio.create_task(client.start(self._token))

            # Wait briefly for connection
            for _ in range(10):
                if client.is_ready():
                    self._mark_connected()
                    return True
                await asyncio.sleep(0.5)

            self._set_fatal_error("timeout", "Discord connection timed out")
            return False

        except Exception as exc:
            self._set_fatal_error("connect_failed", str(exc))
            return False

    async def disconnect(self) -> None:
        if self._client:
            await self._client.close()
        self._mark_disconnected()

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        if not self._client or not self._client.is_ready():
            return SendResult(success=False, error="Not connected")

        try:
            channel = self._client.get_channel(int(chat_id))
            if channel is None:
                channel = await self._client.fetch_channel(int(chat_id))

            # Discord message limit: 2000 characters
            text = content[:2000] if len(content) > 2000 else content

            reference = None
            if reply_to:
                # Fetch the message to reply to
                try:
                    ref_msg = await channel.fetch_message(int(reply_to))
                    reference = ref_msg
                except Exception:
                    pass

            msg = await channel.send(text, reference=reference)
            return SendResult(success=True, message_id=str(msg.id))

        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def edit_message(
        self,
        chat_id: str,
        message_id: str,
        content: str,
        *,
        finalize: bool = False,
    ) -> SendResult:
        if not self._client or not self._client.is_ready():
            return SendResult(success=False, error="Not connected")

        try:
            channel = self._client.get_channel(int(chat_id))
            if channel is None:
                channel = await self._client.fetch_channel(int(chat_id))

            msg = await channel.fetch_message(int(message_id))
            text = content[:2000] if len(content) > 2000 else content
            await msg.edit(content=text)
            return SendResult(success=True, message_id=message_id)

        except Exception as exc:
            error_str = str(exc)
            return SendResult(success=False, error=error_str)

    async def delete_message(self, chat_id: str, message_id: str) -> bool:
        try:
            channel = self._client.get_channel(int(chat_id))
            if channel:
                msg = await channel.fetch_message(int(message_id))
                await msg.delete()
                return True
        except Exception:
            pass
        return False

    async def send_typing(self, chat_id: str, metadata: Any = None) -> None:
        if self._client and self._client.is_ready():
            try:
                channel = self._client.get_channel(int(chat_id))
                if channel:
                    await channel.trigger_typing()
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
        if not self._client:
            return SendResult(success=False, error="Not connected")
        try:
            channel = self._client.get_channel(int(chat_id))
            if channel is None:
                channel = await self._client.fetch_channel(int(chat_id))
            msg = await channel.send(caption or "", embed=None, files=None)
            # Discord supports embed images
            import discord
            embed = discord.Embed()
            embed.set_image(url=image_url)
            if caption:
                embed.description = caption[:2048]
            msg = await channel.send(embed=embed)
            return SendResult(success=True, message_id=str(msg.id))
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
        if not self._client:
            return SendResult(success=False, error="Not connected")
        try:
            import discord
            channel = self._client.get_channel(int(chat_id))
            if channel is None:
                channel = await self._client.fetch_channel(int(chat_id))
            msg = await channel.send(
                caption or "",
                file=discord.File(file_path, filename=file_name),
            )
            return SendResult(success=True, message_id=str(msg.id))
        except Exception as exc:
            return SendResult(success=False, error=str(exc))

    @property
    def max_message_length(self) -> int:
        return 2000

    # -- Internal ------------------------------------------------------------

    async def _on_discord_message(self, message: Any) -> None:
        """Handle an incoming Discord message."""
        # Ignore bot's own messages
        if message.author == self._client.user:
            return

        chat_type = ChatType.DM if isinstance(message.channel, type(message.channel).__class__) else ChatType.GROUP

        # More specific chat type detection
        import discord
        if isinstance(message.channel, discord.DMChannel):
            chat_type = ChatType.DM
        elif isinstance(message.channel, discord.TextChannel):
            chat_type = ChatType.GROUP
        elif isinstance(message.channel, discord.Thread):
            chat_type = ChatType.THREAD

        source = MessageSource(
            platform="discord",
            user_id=str(message.author.id),
            chat_id=str(message.channel.id),
            thread_id=str(message.channel.id) if isinstance(message.channel, discord.Thread) else None,
            chat_type=chat_type,
            display_name=message.author.display_name,
        )

        # Process attachments
        media_urls: list[str] = []
        media_types: list[str] = []
        text = message.content or ""
        msg_type = MessageType.TEXT

        if message.attachments:
            from agent_gateway.media.cache import MediaCache
            cache = MediaCache()
            for attachment in message.attachments:
                try:
                    data = await attachment.read()
                    if attachment.content_type and attachment.content_type.startswith("image/"):
                        path = cache.save_image(data, ext=".png")
                        media_urls.append(path)
                        media_types.append(attachment.content_type)
                        msg_type = MessageType.PHOTO
                    elif attachment.content_type and attachment.content_type.startswith("audio/"):
                        path = cache.save_audio(data, ext=".ogg")
                        media_urls.append(path)
                        media_types.append(attachment.content_type)
                        msg_type = MessageType.AUDIO
                    else:
                        path = cache.save_document(data, filename=attachment.filename)
                        media_urls.append(path)
                        media_types.append(attachment.content_type or "application/octet-stream")
                        msg_type = MessageType.DOCUMENT
                except Exception as exc:
                    logger.warning("Failed to cache Discord attachment: %s", exc)

        event = MessageEvent(
            text=text,
            message_type=msg_type,
            source=source,
            media_urls=media_urls,
            media_types=media_types,
            message_id=str(message.id),
            reply_to_message_id=str(message.reference.message_id) if message.reference else None,
            raw_message=message,
        )

        await self.handle_message(event)


def register_discord() -> None:
    """Register the Discord adapter with the global registry."""
    from agent_gateway.core.registry import EnvVarDef

    registry.register(PlatformEntry(
        name="discord",
        label="Discord",
        adapter_factory=lambda cfg: DiscordAdapter(cfg),
        check_fn=_check_discord_deps,
        install_hint="pip install agent-gateway[discord]",
        required_env=["DISCORD_TOKEN"],
        max_message_length=2000,
        emoji="🎮",
        platform_hint="You are on Discord. Use markdown for formatting. Keep messages under 2000 chars.",
        source="builtin",
        env_var_defs=[
            EnvVarDef(
                key="DISCORD_TOKEN",
                description="Bot token from Discord Developer Portal",
                prompt="Enter bot token",
                is_password=True,
                required=True,
                url="https://discord.com/developers/applications",
            ),
            EnvVarDef(
                key="DISCORD_ALLOWED_USERS",
                description="Comma-separated Discord user IDs",
                prompt="123456789,987654321",
                required=False,
            ),
            EnvVarDef(
                key="DISCORD_ALLOW_ALL_USERS",
                description='Set to "true" to allow all users (development only)',
                prompt="true",
                required=False,
                advanced=True,
            ),
            EnvVarDef(
                key="DISCORD_HOME_CHANNEL",
                description="Channel ID for proactive messages (cron output, reminders)",
                prompt="1234567890",
                required=False,
                advanced=True,
            ),
        ],
    ))
