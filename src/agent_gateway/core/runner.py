"""
Gateway runner — orchestrates all platform adapters.

The ``GatewayRunner`` is the top-level controller that:

  1. Loads configuration and creates platform adapters via the registry
  2. Starts all adapters and injects the message handler
  3. Routes inbound messages to the AI agent core
  4. Routes outbound responses back to the originating platform
  5. Manages session lifecycle and streaming delivery
  6. Handles graceful shutdown

Usage::

    from agent_gateway import GatewayRunner, GatewayConfig

    config = GatewayConfig.load("gateway.yaml")
    runner = GatewayRunner(config, my_agent)

    # Or with a simple callback:
    runner = GatewayRunner(config, agent_callback=my_llm_function)

    await runner.start()
    await runner.wait_for_shutdown()
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import time
import signal
from typing import Any, AsyncIterator, Awaitable, Callable, Optional, Union

from agent_gateway.adapters.email import _normalize_subject
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.config import GatewayConfig
from agent_gateway.core.delivery import DeliveryRouter, DeliveryTarget
from agent_gateway.core.message import (
    EphemeralReply,
    MessageEvent,
    MessageSource,
    SendResult,
)
from agent_gateway.core.registry import registry
from agent_gateway.core.session import Session, SessionStore, build_session_context_prompt
from agent_gateway.server.session_store import PersistedSession as DesktopSession
from agent_gateway.core.stream import StreamConsumer, StreamConsumerConfig

logger = logging.getLogger(__name__)

# Type for the agent callback — can be either:
#   - An object with a ``chat()`` method
#   - A bare async function ``(session_key, message, history, **kw) -> str``
AgentCore = Any
AgentCallback = Callable[..., Awaitable[str]]


class GatewayRunner:
    """
    Gateway orchestrator — manages adapter lifecycles and message flow.

    Args:
        config: Gateway configuration.
        agent: An AI agent object with a ``chat()`` method, **or** pass
            ``agent_callback`` instead for a simpler function-based interface.
        agent_callback: Async function called for each message.
            Signature: ``(session_key, message, history, **kw) -> str``
    """

    def __init__(
        self,
        config: GatewayConfig,
        agent: AgentCore = None,
        *,
        agent_callback: AgentCallback | None = None,
        desktop_store: Any = None,
    ) -> None:
        self.config = config
        self.agent = agent
        self._agent_callback = agent_callback
        self._desktop_store = desktop_store
        # Optional callback to push streaming events to the desktop WebSocket.
        # Signature: async (event_type: str, payload: dict, session_id: str) -> None
        self.desktop_emit: Any = None

        # Runtime state
        self.adapters: dict[str, BasePlatformAdapter] = {}
        self.session_store = SessionStore(
            max_idle_seconds=config.session.max_idle_seconds,
            max_history=config.session.max_history,
        )
        self.delivery_router: Optional[DeliveryRouter] = None
        self._shutdown_event = asyncio.Event()
        self._cleanup_task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start all configured platform adapters.

        Creates adapters via the registry, injects the message handler,
        and connects to each platform.  Adapters that fail to connect
        are skipped with a warning.
        """
        logger.info("Starting gateway...")

        for name, pcfg in self.config.enabled_platforms().items():
            try:
                adapter = registry.create_adapter(name, pcfg.__dict__)
            except Exception as exc:
                logger.error("Failed to create adapter '%s': %s", name, exc)
                continue

            if adapter is None:
                logger.warning("Platform '%s' unavailable — skipping", name)
                continue

            # Wire up the adapter
            adapter.set_message_handler(self._on_message)
            adapter.set_session_store(self.session_store)
            adapter.set_busy_session_handler(self._on_busy_session)

            try:
                connected = await adapter.connect()
                if connected:
                    self.adapters[name] = adapter
                    logger.info("✅ %s connected", adapter.name)
                else:
                    logger.error("❌ %s failed to connect", adapter.name)
            except Exception as exc:
                logger.error("❌ %s connection error: %s", name, exc)

        # Build the delivery router
        self.delivery_router = DeliveryRouter(
            adapters=self.adapters,
            filter_silence=self.config.filter_silence_narration,
        )

        # Start periodic cleanup
        self._cleanup_task = asyncio.create_task(self._periodic_cleanup())

        self._running = True
        logger.info(
            "Gateway started with %d platform(s): %s",
            len(self.adapters),
            ", ".join(self.adapters.keys()),
        )

    # ------------------------------------------------------------------
    # Main message handler — injected into every adapter
    # ------------------------------------------------------------------

    async def _on_message(self, event: MessageEvent) -> Optional[str]:
        """Process an inbound message from any platform.

        This is the central message-processing pipeline:
          1. Authorise the user
          2. Resolve / create the session
          3. Process media attachments
          4. Build context and call the agent
          5. Deliver the response (with optional streaming)
          6. Update session history
        """
        source = event.source
        if source is None:
            return None

        session_key = source.session_key()
        logger.info(
            "Message from %s@%s: %s",
            source.user_id, source.platform,
            event.text[:100] if event.text else "<media>",
        )

        # 1. Authorisation check
        if not event.internal and not self._is_user_authorized(source):
            logger.warning("Unauthorized user: %s@%s", source.user_id, source.platform)
            return "⛔ You are not authorized to use this agent."

        # 2. Handle slash commands
        if event.is_command():
            return await self._handle_command(event)

        # 3. Resolve session
        session = self.session_store.get_or_create(source)

        # 4. Process media
        user_input = event.text
        media_context = self._process_media(event)
        if media_context:
            user_input = f"{user_input}\n\n{media_context}" if user_input else media_context

        # Inject channel context
        if event.channel_context:
            user_input = f"{event.channel_context}\n\n{user_input}"

        # 5. Build agent context
        context_extra = build_session_context_prompt(session)
        if event.channel_prompt:
            context_extra += f"\n\n{event.channel_prompt}"

        # 6. Call the agent
        try:
            if self.config.streaming.enabled:
                response = await self._call_agent_streaming(event, session, user_input, context_extra)
            else:
                response = await self._call_agent(event, session, user_input, context_extra)
        except Exception as exc:
            logger.exception("Agent error for %s: %s", session_key, exc)
            return f"⚠️ Agent error: {exc}"

        # 7. Update history
        session.add_message("user", user_input)
        if response:
            session.add_message("assistant", response)

        # 8. Sync to desktop session store so conversations appear in UI
        if self._desktop_store and source:
            self._sync_to_desktop(source, user_input, response, event)

        return None  # Response already delivered via streaming or direct send

    # ------------------------------------------------------------------
    # Agent calling
    # ------------------------------------------------------------------

    async def _call_agent(
        self,
        event: MessageEvent,
        session: Session,
        user_input: str,
        context_extra: str,
    ) -> Optional[str]:
        """Call the agent (non-streaming) and send the response."""
        response = await self._invoke_agent(session.key, user_input, session.history, context_extra)

        if response and event.source:
            result = await self._send_response(event, response)

            # Register the outgoing Message-ID for email session threading
            if result and getattr(result, "success", False):
                self._register_outgoing_email_msg_id(
                    event, getattr(result, "message_id", None)
                )

            # Handle ephemeral replies
            if isinstance(response, EphemeralReply) and result and result.success and result.message_id:
                ttl = response.ttl_seconds or 0
                if ttl:
                    adapter = self.adapters.get(event.source.platform)
                    if adapter:
                        await adapter._schedule_ephemeral_delete(
                            event.source.chat_id, result.message_id, ttl
                        )

        return response if not isinstance(response, EphemeralReply) else str(response)

    async def _call_agent_streaming(
        self,
        event: MessageEvent,
        session: Session,
        user_input: str,
        context_extra: str,
    ) -> Optional[str]:
        """Call the agent with streaming response delivery.

        Streams to both the platform adapter (via StreamConsumer) and the
        desktop client (via ``self.desktop_emit`` if set).
        """
        source = event.source
        if not source:
            return None

        adapter = self.adapters.get(source.platform)
        if not adapter:
            return await self._call_agent(event, session, user_input, context_extra)

        # Create stream consumer for the platform adapter
        stream_config = StreamConsumerConfig(
            min_edit_interval=self.config.streaming.min_edit_interval,
            use_draft=self.config.streaming.use_draft,
            tool_progress_mode=self.config.streaming.tool_progress,
            tool_preview_length=self.config.streaming.tool_preview_length,
            # Platforms that can't edit messages (email, etc.) should buffer
            # all output and send a single final message — intermediate sends
            # would create duplicate, un-editable messages.
            send_final_only=not adapter.supports_edit(),
        )
        consumer_metadata = {"thread_id": source.thread_id} if source.thread_id else None
        consumer = StreamConsumer(
            adapter, source.chat_id, stream_config,
            reply_to=event.reply_to_message_id,
            metadata=consumer_metadata,
        )

        # Determine the session ID for desktop events (if applicable)
        desktop_sid: str | None = None
        if self._desktop_store:
            raw = event.raw_message or {}
            in_reply_to = raw.get("in_reply_to") or event.reply_to_message_id
            references_raw = raw.get("references", "")
            sender = source.user_id.replace("@", "-").replace(".", "-")
            # Prefer adapter-resolved thread_id (already walked In-Reply-To)
            # over re-normalizing the raw subject which may have localized
            # prefixes like "回复:", "Aw:", "Réf :" etc.
            subject = source.thread_id or _normalize_subject(str(raw.get("subject", "")).strip())
            # Try to find existing session via In-Reply-To
            if in_reply_to:
                parent = self._desktop_store.find_by_email_message_id(in_reply_to)
                if parent:
                    desktop_sid = parent.session_id
            # Walk References header chain as fallback
            if not desktop_sid and references_raw:
                for ref_id in references_raw.split():
                    ref_id = ref_id.strip("<> \t\n")
                    if ref_id:
                        parent = self._desktop_store.find_by_email_message_id(ref_id)
                        if parent:
                            desktop_sid = parent.session_id
                            break
            if not desktop_sid:
                if subject:
                    subject_hash = hashlib.sha256(subject.encode()).hexdigest()[:8]
                    desktop_sid = f"email-{sender}-{subject_hash}"
                else:
                    desktop_sid = f"email-{sender}"
                existing = self._desktop_store.get(desktop_sid)
                if not existing:
                    desktop_sid = None  # Will be created in _sync_to_desktop later

        # Notify desktop: stream starting
        if desktop_sid and self.desktop_emit:
            await self.desktop_emit("message.start", {}, desktop_sid)

        # Obtain an async iterator of text chunks from the agent
        full_text = ""
        try:
            if self.agent and hasattr(self.agent, "stream"):
                # Direct agent object with stream() method
                chunk_iter = self.agent.stream(
                    session_key=session.key,
                    message=user_input,
                    history=session.history,
                    system_extra=context_extra,
                )
            else:
                # Use the dynamic bridge (same logic as make_agent_callback)
                chunk_iter = self._stream_via_bridge(
                    session.key, user_input, session.history, context_extra,
                )

            async for chunk in chunk_iter:
                if isinstance(chunk, str):
                    full_text += chunk
                    consumer.on_delta(chunk)
                    # Push delta to desktop
                    if desktop_sid and self.desktop_emit:
                        await self.desktop_emit("message.delta", {"text": chunk}, desktop_sid)
                elif hasattr(chunk, "tool_name"):
                    await consumer.on_tool_call(
                        chunk.tool_name,
                        getattr(chunk, "preview", ""),
                        getattr(chunk, "args", None),
                    )

            result = await consumer.finish(full_text)

            # Register the outgoing Message-ID so future In-Reply-To lookups
            # from the user's email client can find this session.
            if result and getattr(result, "success", False):
                self._register_outgoing_email_msg_id(
                    event, getattr(result, "message_id", None)
                )

            # Notify desktop: stream complete
            if desktop_sid and self.desktop_emit:
                await self.desktop_emit("message.complete", {"text": full_text}, desktop_sid)

            return full_text
        except Exception as exc:
            error_text = f"\n\n⚠️ Stream error: {exc}"
            full_text += error_text
            await consumer.finish(full_text)
            if desktop_sid and self.desktop_emit:
                await self.desktop_emit("message.delta", {"text": error_text}, desktop_sid)
                await self.desktop_emit("message.complete", {"text": full_text}, desktop_sid)
            # Don't raise — let _on_message complete normally with what we have
            return full_text

    async def _stream_via_bridge(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        system_extra: str,
    ) -> AsyncIterator[str]:
        """Create a bridge on-the-fly and stream from it.

        Reads the current ``default_agent`` and ``default_agent_params``
        from the desktop store config so agent switching takes effect
        immediately.
        """
        from agent_gateway.server.agent_factory import create_bridge

        if self._desktop_store:
            agent_type = self._desktop_store.get_config("default_agent", "claude-code")
            # Per-agent params: { "claude-code": {...}, "pi": {...} }
            all_params: dict = self._desktop_store.get_config("agent_params") or {}
            agent_params = all_params.get(agent_type, {}) if isinstance(all_params, dict) else {}
            # Include global reasoning default if set
            default_reasoning = self._desktop_store.get_config("default_reasoning")
            if default_reasoning and "reasoning" not in agent_params:
                agent_params["reasoning"] = default_reasoning
        else:
            agent_type = "claude-code"
            agent_params = {}

        bridge = create_bridge(agent_type, timeout=self.config.agent_timeout, **agent_params)
        # Set default cwd from hermes_config.terminal.cwd if available
        if self._desktop_store and not bridge.config.cwd:
            hermes_cfg = self._desktop_store.get_config("hermes_config", {})
            if isinstance(hermes_cfg, dict):
                default_cwd = hermes_cfg.get("terminal", {}).get("cwd") if isinstance(hermes_cfg.get("terminal"), dict) else None
                if default_cwd:
                    bridge.config.cwd = default_cwd
        try:
            async for chunk in bridge.stream(
                session_key=session_key,
                message=message,
                history=history,
                system_extra=system_extra,
            ):
                yield chunk
        finally:
            try:
                await asyncio.wait_for(bridge.shutdown(), timeout=5.0)
            except Exception:
                pass

    async def _invoke_agent(
        self,
        session_key: str,
        message: str,
        history: list[dict[str, Any]],
        context_extra: str,
    ) -> Optional[str]:
        """Invoke the agent core (object or callback)."""
        if self._agent_callback:
            return await self._agent_callback(
                session_key=session_key,
                message=message,
                history=history,
                system_extra=context_extra,
            )

        if self.agent and hasattr(self.agent, "chat"):
            return await self.agent.chat(
                session_key=session_key,
                message=message,
                history=history,
                system_extra=context_extra,
            )

        raise RuntimeError("No agent or agent_callback configured")

    # ------------------------------------------------------------------
    # Command handling
    # ------------------------------------------------------------------

    async def _handle_command(self, event: MessageEvent) -> Optional[str]:
        """Handle slash commands."""
        command = event.get_command()
        if not command:
            return None

        handlers = {
            "new": self._cmd_new_session,
            "reset": self._cmd_new_session,
            "status": self._cmd_status,
            "help": self._cmd_help,
            "sessions": self._cmd_sessions,
        }

        handler = handlers.get(command)
        if handler:
            return await handler(event)

        return None  # Let the agent handle unknown commands

    async def _cmd_new_session(self, event: MessageEvent) -> str:
        if event.source:
            self.session_store.reset(event.source.session_key())
        return "🆕 Session reset. Starting fresh!"

    async def _cmd_status(self, event: MessageEvent) -> str:
        lines = ["📊 **Gateway Status**", ""]
        for name, adapter in self.adapters.items():
            status = "✅ connected" if adapter.is_connected else "❌ disconnected"
            lines.append(f"- {name}: {status}")
        lines.append(f"\nSessions: {self.session_store.active_count()}")
        return "\n".join(lines)

    async def _cmd_help(self, event: MessageEvent) -> str:
        return (
            "🤖 **Agent Gateway Commands**\n\n"
            "/new — Reset conversation\n"
            "/status — Gateway status\n"
            "/sessions — Active sessions\n"
            "/help — This message"
        )

    async def _cmd_sessions(self, event: MessageEvent) -> str:
        sessions = self.session_store.all_sessions()
        if not sessions:
            return "No active sessions."
        lines = [f"📋 **{len(sessions)} active session(s)**:", ""]
        for s in sessions[:10]:
            from datetime import datetime
            last = datetime.fromtimestamp(s.last_active).strftime("%H:%M:%S")
            lines.append(f"- `{s.platform}:{s.user_id}` (last {last}, {len(s.history)} msgs)")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _send_response(self, event: MessageEvent, response: str) -> Optional[SendResult]:
        """Send a response back to the originating platform."""
        if not event.source or not self.delivery_router:
            return None

        target = DeliveryTarget(
            platform=event.source.platform,
            chat_id=event.source.chat_id,
            thread_id=event.source.thread_id,
        )
        result = await self.delivery_router.deliver(
            content=str(response),
            target=target,
            reply_to=event.reply_to_message_id,
        )
        # Convert delivery dict to SendResult so callers can access message_id
        if result.get("success"):
            return SendResult(
                success=True,
                message_id=result.get("message_id"),
            )
        return None

    def _is_user_authorized(self, source: MessageSource) -> bool:
        """Check if the sender is authorised.

        Default: allow all.  Override in subclasses or configure per-platform.
        """
        # Check platform-specific allowlist
        pcfg = self.config.get_platform(source.platform)
        if pcfg and pcfg.allow_from:
            return source.user_id in pcfg.allow_from or source.chat_id in pcfg.allow_from

        return True  # Default: open access

    async def _on_busy_session(self, event: MessageEvent, session_key: str) -> bool:
        """Handle a message that arrived while the session was busy."""
        if event.source:
            meta = {"thread_id": event.source.thread_id} if event.source.thread_id else None
            await self.adapters[event.source.platform].send(
                event.source.chat_id,
                "⏳ I'm still processing your previous message. Please wait...",
                metadata=meta,
            )
        return False

    # ------------------------------------------------------------------
    # Desktop session sync
    # ------------------------------------------------------------------

    def _sync_to_desktop(
        self,
        source: MessageSource,
        user_input: str,
        response: Optional[str],
        event: MessageEvent,
    ) -> None:
        """Sync platform messages to the desktop session store.

        Writes user messages and agent responses into the persistent
        ``~/.nexus-agent/sessions.json`` so they appear in the desktop
        client sidebar.
        """
        store = self._desktop_store
        if store is None:
            return

        # --- Session routing strategy ---
        # 1. If the email has an In-Reply-To / References header, find the
        #    session that contains any ancestor message and append there.
        # 2. Otherwise, derive a deterministic session ID from sender + subject
        #    so new topics get their own session.
        raw = event.raw_message or {}
        email_message_id = raw.get("message_id") or event.message_id
        in_reply_to = raw.get("in_reply_to") or event.reply_to_message_id
        references_raw = raw.get("references", "")
        # Prefer adapter-resolved thread_id (already walked In-Reply-To via
        # _msg_id_to_thread) over re-normalizing the raw subject which may
        # have localized prefixes like "回复:", "Aw:", "Réf :" etc.
        subject = source.thread_id or _normalize_subject(str(raw.get("subject", "")).strip())
        sender = source.user_id.replace("@", "-").replace(".", "-")

        desktop_sid: str | None = None
        existing: DesktopSession | None = None

        # Strategy 1: In-Reply-To → thread into parent session
        if in_reply_to:
            parent = store.find_by_email_message_id(in_reply_to)
            if parent is not None:
                desktop_sid = parent.session_id
                existing = parent

        # Strategy 1b: Walk References header chain (ancestor Message-IDs).
        # Covers the case where In-Reply-To points to the gateway's own
        # outgoing message (registered via _register_outgoing_email_msg_id)
        # or when multiple hops exist between the original message and the
        # current reply.  Works across gateway restarts because _email_msg_ids
        # is persisted to sessions.json.
        if desktop_sid is None and references_raw:
            for ref_id in references_raw.split():
                ref_id = ref_id.strip("<> \t\n")
                if ref_id:
                    parent = store.find_by_email_message_id(ref_id)
                    if parent is not None:
                        desktop_sid = parent.session_id
                        existing = parent
                        break

        # Strategy 2: subject-based deterministic session ID
        if desktop_sid is None:
            # Use an 8-char hash of the subject — works for any language
            subject_hash = hashlib.sha256(subject.encode()).hexdigest()[:8]
            desktop_sid = f"email-{sender}-{subject_hash}" if subject else f"email-{sender}"
            existing = store.get(desktop_sid)

        if existing is None:
            # First message in this thread — create desktop session
            title = (
                subject
                or (user_input or "")[:60].split("\n")[0]
                or f"Email: {source.display_name}"
            )
            # Use the actual agent type, not "email" — the desktop client
            # needs a valid agent_type to create a bridge when resuming.
            agent_type = store.get_config("default_agent", "claude-code")
            store.create(
                session_id=desktop_sid,
                agent_type=agent_type,
                title=title,
            )
            history: list[dict[str, Any]] = []
        else:
            history = list(existing.history)

        # Append messages
        history.append({"role": "user", "content": user_input})
        if response:
            history.append({"role": "assistant", "content": str(response)})

        # Persist
        store.update_history(desktop_sid, history)

        # Track email Message-ID → session mapping for In-Reply-To threading
        update_fields: dict[str, Any] = {"last_active": time.time()}
        if email_message_id:
            msg_ids: list[str] = list(
                (existing._email_msg_ids if existing else None) or []
            )
            if email_message_id not in msg_ids:
                msg_ids.append(email_message_id)
            update_fields["_email_msg_ids"] = msg_ids
        store.update(desktop_sid, **update_fields)
        logger.debug("Synced %d messages to desktop session %s", len(history), desktop_sid)

    def _register_outgoing_email_msg_id(
        self, event: MessageEvent, message_id: str | None
    ) -> None:
        """Register the gateway's outgoing Message-ID in the desktop session.

        When the user later replies to our email, their In-Reply-To will point
        to this Message-ID.  Without registration, ``find_by_email_message_id``
        cannot resolve it and the session-linking fallback produces a *new*
        session (e.g. "回复:xxx" becomes a separate chat).
        """
        store = self._desktop_store
        if not store or not message_id:
            return
        raw = event.raw_message or {}
        in_reply_to = raw.get("in_reply_to") or event.reply_to_message_id
        sender = (event.source.user_id if event.source else "").replace("@", "-").replace(".", "-")
        # Use adapter-resolved thread_id if available
        subject = (event.source.thread_id if event.source else None) or _normalize_subject(str(raw.get("subject", "")).strip())
        # Find the desktop session the same way _sync_to_desktop does
        desktop_sid: str | None = None
        if in_reply_to:
            parent = store.find_by_email_message_id(in_reply_to)
            if parent is not None:
                desktop_sid = parent.session_id
        if not desktop_sid:
            if subject:
                subject_hash = hashlib.sha256(subject.encode()).hexdigest()[:8]
                desktop_sid = f"email-{sender}-{subject_hash}"
            else:
                desktop_sid = f"email-{sender}"
        existing = store.get(desktop_sid)
        if not existing:
            return
        msg_ids: list[str] = list(existing._email_msg_ids or [])
        if message_id not in msg_ids:
            msg_ids.append(message_id)
            store.update(desktop_sid, _email_msg_ids=msg_ids)
            logger.debug("Registered outgoing Message-ID %s in session %s", message_id, desktop_sid)

    def _process_media(self, event: MessageEvent) -> Optional[str]:
        """Build a media description for the agent."""
        if not event.media_urls:
            return None

        parts = []
        for url, mtype in zip(event.media_urls, event.media_types or []):
            kind = "image" if mtype.startswith("image/") else \
                   "video" if mtype.startswith("video/") else \
                   "audio" if mtype.startswith("audio/") else "file"
            parts.append(f"[{kind}: {url}]")

        return "\n".join(parts)

    # ------------------------------------------------------------------
    # Periodic maintenance
    # ------------------------------------------------------------------

    async def _periodic_cleanup(self) -> None:
        """Background task for session cleanup and history trimming."""
        while not self._shutdown_event.is_set():
            try:
                await asyncio.sleep(self.config.session.cleanup_interval)
                removed = self.session_store.cleanup_idle()
                self.session_store.trim_histories()
                self.session_store.check_reset_policy()
                if removed:
                    logger.info("Periodic cleanup: removed %d idle sessions", removed)
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Cleanup error: %s", exc)

    # ------------------------------------------------------------------
    # Shutdown
    # ------------------------------------------------------------------

    async def wait_for_shutdown(self) -> None:
        """Block until ``shutdown()`` is called or a signal is received."""
        await self._shutdown_event.wait()

    async def shutdown(self) -> None:
        """Gracefully shut down all adapters and stop the gateway."""
        if not self._running:
            return

        logger.info("Shutting down gateway...")
        self._running = False

        # Cancel cleanup task
        if self._cleanup_task and not self._cleanup_task.done():
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

        # Disconnect all adapters
        for name, adapter in self.adapters.items():
            try:
                await adapter.disconnect()
                logger.info("✅ %s disconnected", adapter.name)
            except Exception as exc:
                logger.error("Error disconnecting %s: %s", name, exc)

        self.adapters.clear()
        self._shutdown_event.set()
        logger.info("Gateway shutdown complete")

    def install_signal_handlers(self) -> None:
        """Install SIGINT / SIGTERM handlers for graceful shutdown."""
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, lambda: asyncio.create_task(self.shutdown()))
            except NotImplementedError:
                pass  # Windows doesn't support add_signal_handler
