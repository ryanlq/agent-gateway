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
from typing import Any, AsyncIterator, Awaitable, Callable, Optional

from agent_gateway.adapters.email import _normalize_subject
from agent_gateway.core.adapter import BasePlatformAdapter
from agent_gateway.core.config import GatewayConfig
from agent_gateway.core.delivery import DeliveryRouter, DeliveryTarget
from agent_gateway.core.message import (
    ChatType,
    EphemeralReply,
    MessageEvent,
    MessageSource,
    SendResult,
)
from agent_gateway.core.registry import registry
from agent_gateway.core.session import Session, SessionStore, build_session_context_prompt
from agent_gateway.server.session_store import PersistedSession as DesktopSession
from agent_gateway.core.stream import StreamConsumer, StreamConsumerConfig
from agent_gateway.agents.events import AgentEvent

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

        # Cron manager — set externally via runner.cron_manager = ...
        # When present, enables agent-driven cron job creation and /cron commands.
        self.cron_manager: Any = None

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
                    if not adapter._backlog_recovered:
                        try:
                            recovered = await adapter.recover_backlog()
                            if recovered:
                                logger.info("📬 %s recovered %d missed message(s)", adapter.name, recovered)
                            adapter._backlog_recovered = True
                        except Exception as exc:
                            logger.warning("⚠️ %s backlog recovery failed: %s", adapter.name, exc)
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
    # Adapter lifecycle management
    # ------------------------------------------------------------------

    async def start_adapter(self, name: str, config_dict: dict[str, Any] | None = None) -> bool:
        """Create and connect a single adapter by name.

        If the adapter is already running, it is stopped first.
        Returns True if the adapter connected successfully.
        """
        # Stop existing adapter if running
        if name in self.adapters:
            await self.stop_adapter(name)

        entry = registry.get(name)
        if entry is None:
            logger.warning("Platform '%s' not registered — cannot start", name)
            return False

        if not entry.check_fn():
            logger.warning("Platform '%s' dependencies not met", entry.label)
            return False

        adapter = registry.create_adapter(name, config_dict or {})
        if adapter is None:
            return False

        # Wire up the adapter
        adapter.set_message_handler(self._on_message)
        adapter.set_session_store(self.session_store)
        adapter.set_busy_session_handler(self._on_busy_session)

        try:
            connected = await adapter.connect()
            if connected:
                self.adapters[name] = adapter
                # Rebuild delivery router
                self.delivery_router = DeliveryRouter(
                    adapters=self.adapters,
                    filter_silence=self.config.filter_silence_narration,
                )
                logger.info("✅ %s started", adapter.name)
                if not adapter._backlog_recovered:
                    try:
                        recovered = await adapter.recover_backlog()
                        if recovered:
                            logger.info("📬 %s recovered %d missed message(s)", adapter.name, recovered)
                        adapter._backlog_recovered = True
                    except Exception as exc:
                        logger.warning("⚠️ %s backlog recovery failed: %s", adapter.name, exc)
                return True
            else:
                logger.error("❌ %s failed to connect", adapter.name)
                return False
        except Exception as exc:
            logger.error("❌ %s connection error: %s", name, exc)
            return False

    async def stop_adapter(self, name: str) -> bool:
        """Stop and remove a running adapter by name.

        Returns True if the adapter was running and stopped successfully.
        """
        adapter = self.adapters.pop(name, None)
        if adapter is None:
            return False

        try:
            await adapter.disconnect()
            logger.info("⏹️ %s stopped", adapter.name)
        except Exception as exc:
            logger.warning("Error stopping %s: %s", name, exc)

        # Rebuild delivery router
        if self.adapters:
            self.delivery_router = DeliveryRouter(
                adapters=self.adapters,
                filter_silence=self.config.filter_silence_narration,
            )
        else:
            self.delivery_router = None
        return True

    async def restart_adapter(self, name: str, config_dict: dict[str, Any] | None = None) -> bool:
        """Stop and restart a single adapter.

        Returns True if the adapter restarted and connected successfully.
        """
        return await self.start_adapter(name, config_dict)

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
        context_extra = build_session_context_prompt(
            session, cron_enabled=bool(self.cron_manager),
        )
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

        # 7. Post-process: execute any cron operations embedded in agent response
        if self.cron_manager and response:
            try:
                from agent_gateway.core.cron_tool import CronToolParser, CronToolExecutor
                ops = CronToolParser.extract_operations(response)
                if ops:
                    origin_info = {
                        "platform": source.platform,
                        "user_id": source.user_id,
                        "chat_id": source.chat_id,
                        "thread_id": source.thread_id,
                    }
                    executor = CronToolExecutor(self.cron_manager)
                    results = await executor.execute_all(
                        ops, origin=origin_info, session_key=session_key,
                    )
                    response = CronToolParser.replace_operations(response, results)
                    logger.info(
                        "Processed %d cron operation(s) for session %s",
                        len(ops), session_key,
                    )
            except Exception as exc:
                logger.warning("Cron tool post-processing failed: %s", exc)

        # 8. Update history
        session.add_message("user", user_input)
        if response:
            session.add_message("assistant", response)

        # 9. Sync to desktop session store so conversations appear in UI
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

        # Opt-in tool-call card (e.g. Feishu): when the adapter supports it,
        # a single streaming summary card replaces the per-tool progress
        # messages that StreamConsumer.on_tool_call would otherwise send.
        tool_handle: Any = None
        if adapter.supports_tool_card():
            tool_handle = await adapter.begin_tool_round(
                source.chat_id,
                reply_to=event.reply_to_message_id,
                metadata=consumer_metadata,
            )

        # Determine the session ID for desktop events (if applicable)
        desktop_sid: str | None = None
        if self._desktop_store:
            # IM platforms (feishu, telegram, ...): one deterministic session id
            # per chat (+ topic). Email keeps its In-Reply-To / subject threading.
            if source.platform != "email":
                desktop_sid = self._chat_desktop_session_id(source)
            else:
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

            _chunk_count = 0
            async for chunk in chunk_iter:
                _chunk_count += 1
                if _chunk_count == 1:
                    logger.info(
                        "[runner] First chunk received: type=%s kind=%s",
                        type(chunk).__name__,
                        getattr(chunk, "kind", "-"),
                    )
                if isinstance(chunk, AgentEvent):
                    # Structured event protocol (new bridges, e.g. claude-code-sdk)
                    if chunk.kind == "text_delta":
                        if chunk.text:
                            full_text += chunk.text
                            consumer.on_delta(chunk.text)
                            if desktop_sid and self.desktop_emit:
                                await self.desktop_emit(
                                    "message.delta", {"text": chunk.text}, desktop_sid
                                )
                    elif chunk.kind == "reasoning_delta":
                        if chunk.text and desktop_sid and self.desktop_emit:
                            # Reasoning flows ONLY to the desktop reasoning panel.
                            # Platforms (chat apps) don't get this — it would
                            # spam the thread with the model's inner monologue.
                            await self.desktop_emit(
                                "reasoning.delta", {"text": chunk.text}, desktop_sid
                            )
                    elif chunk.kind == "tool_start":
                        # Desktop: structured tool card (start)
                        if desktop_sid and self.desktop_emit:
                            await self.desktop_emit(
                                "tool.start",
                                {
                                    "name": chunk.tool_name,
                                    "tool_id": chunk.tool_id,
                                    "input": chunk.tool_input,
                                },
                                desktop_sid,
                            )
                        # Platform: either the streaming tool card (opt-in) or
                        # the legacy terse progress hint. When a tool card is
                        # active we skip on_tool_call — it would send a separate
                        # message per tool, which the card replaces.
                        if tool_handle is not None:
                            await adapter.tool_round_start(tool_handle, {
                                "name": chunk.tool_name,
                                "tool_id": chunk.tool_id,
                                "input": chunk.tool_input,
                            })
                        else:
                            await consumer.on_tool_call(
                                chunk.tool_name,
                                preview="",
                                args=chunk.tool_input,
                            )
                    elif chunk.kind == "tool_complete":
                        if desktop_sid and self.desktop_emit:
                            payload = {
                                "name": chunk.tool_name,
                                "tool_id": chunk.tool_id,
                                "result": chunk.tool_result,
                            }
                            if chunk.is_error:
                                payload["error"] = chunk.error_message or "tool failed"
                            await self.desktop_emit(
                                "tool.complete", payload, desktop_sid
                            )
                        if tool_handle is not None:
                            await adapter.tool_round_complete(tool_handle, {
                                "name": chunk.tool_name,
                                "tool_id": chunk.tool_id,
                                "result": chunk.tool_result,
                                "is_error": chunk.is_error,
                                "error_message": chunk.error_message,
                            })

                elif isinstance(chunk, str):
                    # Legacy bridge: raw string chunk → text_delta
                    full_text += chunk
                    consumer.on_delta(chunk)
                    if desktop_sid and self.desktop_emit:
                        await self.desktop_emit("message.delta", {"text": chunk}, desktop_sid)

                elif hasattr(chunk, "tool_name"):
                    # Legacy duck-typed tool call (e.g. ToolCallChunk)
                    await consumer.on_tool_call(
                        chunk.tool_name,
                        getattr(chunk, "preview", ""),
                        getattr(chunk, "args", None),
                    )

            result = await consumer.finish(full_text)

            if tool_handle is not None:
                await adapter.end_tool_round(tool_handle, success=True)

            # Post-process cron operations in streaming response
            # Note: the raw block may have been streamed to the platform already,
            # but the confirmation will be sent as a follow-up message.
            if self.cron_manager and full_text:
                try:
                    from agent_gateway.core.cron_tool import CronToolParser, CronToolExecutor
                    ops = CronToolParser.extract_operations(full_text)
                    if ops:
                        origin_info = {
                            "platform": source.platform,
                            "user_id": source.user_id,
                            "chat_id": source.chat_id,
                            "thread_id": source.thread_id,
                        }
                        executor = CronToolExecutor(self.cron_manager)
                        cron_results = await executor.execute_all(
                            ops, origin=origin_info, session_key=session.key,
                        )
                        # Send confirmation as a follow-up message
                        for cr in cron_results:
                            icon = "✅" if cr.success else "❌"
                            confirm_text = f"{icon} {cr.message}"
                            try:
                                await self._send_response(event, confirm_text)
                            except Exception as send_err:
                                logger.warning("Failed to send cron confirmation: %s", send_err)
                        # Clean up full_text for history
                        full_text = CronToolParser.replace_operations(full_text, cron_results)
                        logger.info(
                            "Processed %d cron operation(s) in streaming response",
                            len(ops),
                        )
                except Exception as exc:
                    logger.warning("Cron tool streaming post-processing failed: %s", exc)

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
            if tool_handle is not None:
                await adapter.end_tool_round(tool_handle, success=False)
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
            agent_type = self._desktop_store.get_config("default_agent", "claude-code-sdk")
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
            "cron": self._cmd_cron,
            "jobs": self._cmd_jobs,
            "schedule": self._cmd_schedule,
            "loop": self._cmd_loop,
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
            "/cron list — List cron jobs\n"
            "/cron create <schedule> <prompt> — Create cron job\n"
            "/cron delete <id> — Delete cron job\n"
            "/cron pause <id> — Pause cron job\n"
            "/cron resume <id> — Resume cron job\n"
            "/cron trigger <id> — Trigger immediate run\n"
            "/jobs — List cron jobs (alias)\n"
            "/schedule <schedule> <prompt> — Quick create (one-shot) (alias)\n"
            "/loop <interval> <prompt> — Recurring task (e.g. /loop 10m check deploy)\n"
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
    # Cron commands
    # ------------------------------------------------------------------

    async def _cmd_cron(self, event: MessageEvent) -> str:
        """Handle ``/cron`` subcommands: list, create, delete, pause, resume, trigger."""
        if not self.cron_manager:
            return "⚠️ Cron system is not available."

        args = event.get_command_args().strip()
        parts = args.split(maxsplit=1)
        sub = parts[0].lower() if parts else ""
        rest = parts[1].strip() if len(parts) > 1 else ""

        if sub in ("", "list", "ls"):
            return await self._cron_list()
        elif sub == "create":
            return await self._cron_create(rest, event)
        elif sub in ("delete", "del", "rm"):
            return await self._cron_delete(rest)
        elif sub == "pause":
            return await self._cron_pause(rest)
        elif sub == "resume":
            return await self._cron_resume(rest)
        elif sub in ("trigger", "run", "exec"):
            return await self._cron_trigger(rest)
        else:
            return (
                "⚠️ Unknown cron subcommand. Usage:\n"
                "/cron list\n"
                "/cron create <schedule> <prompt>\n"
                "/cron delete <job_id>\n"
                "/cron pause <job_id>\n"
                "/cron resume <job_id>\n"
                "/cron trigger <job_id>"
            )

    async def _cmd_jobs(self, event: MessageEvent) -> str:
        """Alias for ``/cron list``."""
        if not self.cron_manager:
            return "⚠️ Cron system is not available."
        return await self._cron_list()

    async def _cmd_schedule(self, event: MessageEvent) -> str:
        """Alias for ``/cron create``."""
        if not self.cron_manager:
            return "⚠️ Cron system is not available."
        args = event.get_command_args().strip()
        return await self._cron_create(args, event)

    async def _cmd_loop(self, event: MessageEvent) -> str:
        """``/loop <interval> <prompt>`` — recurring cron create.

        Unlike ``/schedule``, /loop forces RECURRING semantics: a bare ``10m``
        becomes ``every 10m``. Shares ``parse_loop_args`` with the desktop path
        (server/methods.py). Calls ``create_job`` directly rather than delegating
        to ``_cron_create``, whose ``split(maxsplit=1)`` mangles ``every 10m``.
        """
        if not self.cron_manager:
            return "⚠️ Cron system is not available."
        from agent_gateway.core.commands import parse_loop_args

        try:
            schedule, prompt, max_runs = parse_loop_args(event.get_command_args())
        except ValueError as exc:
            return f"⚠️ {exc}"

        origin = self._cron_origin(event)
        deliver = "local"
        if origin:
            parts = [origin["platform"]]
            if origin.get("chat_id"):
                parts.append(str(origin["chat_id"]))
            if origin.get("thread_id"):
                parts.append(str(origin["thread_id"]))
            deliver = ":".join(parts)

        try:
            job = self.cron_manager.create_job(
                prompt=prompt,
                schedule=schedule,
                deliver=deliver,
                origin=origin,
                max_runs=max_runs,
            )
        except ValueError as exc:
            return f"❌ 创建失败: {exc}"
        except Exception as exc:
            return f"❌ 创建失败: {exc}"

        cap = f"• 迭代上限: {max_runs} 次\n" if max_runs else ""
        return (
            f"🔁 已创建循环任务 \"{job.get('name', 'cron job')}\"\n"
            f"• ID: {job.get('id', '?')}\n"
            f"• 计划: {job.get('schedule_display', schedule)}\n"
            f"{cap}"
            f"• 下次执行: {job.get('next_run_at', '?')}"
        )

    # -- Cron helpers -------------------------------------------------------

    def _cron_origin(self, event: MessageEvent) -> Optional[dict]:
        """Build origin dict from the event's source."""
        source = event.source
        if not source:
            return None
        return {
            "platform": source.platform,
            "user_id": source.user_id,
            "chat_id": source.chat_id,
            "thread_id": source.thread_id,
        }

    async def _cron_list(self) -> str:
        jobs = self.cron_manager.list_jobs()
        if not jobs:
            return "📋 当前没有任何定时任务。"
        lines = [f"📋 **定时任务列表** ({len(jobs)} 个):", ""]
        for j in jobs:
            state_icon = {
                "scheduled": "🟢", "paused": "⏸️",
                "completed": "✅", "error": "❌",
            }.get(j.get("state", ""), "❓")
            job_id = j.get("id", "?")
            name = j.get("name", "?")
            schedule = j.get("schedule_display", "?")
            next_run = j.get("next_run_at", "?")
            lines.append(
                f"{state_icon} **{name}** (ID: `{job_id}`)\n"
                f"   计划: {schedule} | 下次: {next_run}"
            )
        return "\n".join(lines)

    async def _cron_create(self, args: str, event: MessageEvent) -> str:
        """Parse ``<schedule> <prompt>`` and create a cron job."""
        if not args:
            return "⚠️ Usage: /cron create <schedule> <prompt>\n示例: /cron create \"0 9 * * *\" 检查服务器状态"
        parts = args.split(maxsplit=1)
        if len(parts) < 2:
            return "⚠️ 需要提供 schedule 和 prompt。示例: /cron create \"every 30m\" 检查磁盘空间"
        schedule = parts[0].strip().strip('"').strip("'")
        prompt = parts[1].strip()

        origin = self._cron_origin(event)
        # Resolve deliver target from origin
        deliver = "local"
        if origin:
            p = [origin["platform"]]
            if origin.get("chat_id"):
                p.append(str(origin["chat_id"]))
            if origin.get("thread_id"):
                p.append(str(origin["thread_id"]))
            deliver = ":".join(p)

        try:
            job = self.cron_manager.create_job(
                prompt=prompt,
                schedule=schedule,
                deliver=deliver,
                origin=origin,
            )
        except ValueError as e:
            return f"❌ 创建失败: {e}"
        except Exception as e:
            return f"❌ 创建失败: {e}"

        return (
            f"✅ 已创建定时任务 \"{job.get('name', 'cron job')}\"\n"
            f"• ID: {job.get('id', '?')}\n"
            f"• 计划: {job.get('schedule_display', schedule)}\n"
            f"• 下次执行: {job.get('next_run_at', '?')}"
        )

    async def _cron_delete(self, job_id: str) -> str:
        job_id = job_id.strip()
        if not job_id:
            return "⚠️ Usage: /cron delete <job_id>"
        ok = self.cron_manager.delete_job(job_id)
        if ok:
            return f"✅ 已删除定时任务 (ID: {job_id})"
        return f"❌ 未找到任务 {job_id}"

    async def _cron_pause(self, job_id: str) -> str:
        job_id = job_id.strip()
        if not job_id:
            return "⚠️ Usage: /cron pause <job_id>"
        job = self.cron_manager.pause_job(job_id)
        if job:
            return f"⏸️ 已暂停 \"{job.get('name', job_id)}\" (ID: {job_id})"
        return f"❌ 未找到任务 {job_id}"

    async def _cron_resume(self, job_id: str) -> str:
        job_id = job_id.strip()
        if not job_id:
            return "⚠️ Usage: /cron resume <job_id>"
        job = self.cron_manager.resume_job(job_id)
        if job:
            return (
                f"▶️ 已恢复 \"{job.get('name', job_id)}\" (ID: {job_id})\n"
                f"• 下次执行: {job.get('next_run_at', '?')}"
            )
        return f"❌ 未找到任务 {job_id}"

    async def _cron_trigger(self, job_id: str) -> str:
        job_id = job_id.strip()
        if not job_id:
            return "⚠️ Usage: /cron trigger <job_id>"
        job = self.cron_manager.trigger_job(job_id)
        if job:
            return f"⚡ 已触发 \"{job.get('name', job_id)}\" (ID: {job_id})，将在下次 tick 执行"
        return f"❌ 未找到任务 {job_id}"

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

    # ------------------------------------------------------------------
    # IM platform desktop sync (feishu, telegram, discord, ...)
    # ------------------------------------------------------------------

    _PLATFORM_LABELS: dict[str, str] = {
        "feishu": "飞书",
        "lark": "飞书",
        "telegram": "Telegram",
        "discord": "Discord",
        "slack": "Slack",
        "matrix": "Matrix",
        "whatsapp": "WhatsApp",
        "signal": "Signal",
        "qqbot": "QQ",
        "weixin": "微信",
        "wecom": "企业微信",
        "dingtalk": "钉钉",
    }

    @staticmethod
    def _chat_desktop_session_id(source: MessageSource) -> str:
        """Deterministic desktop session id for IM platforms.

        ``{platform}-{chat_id}[-{thread_id}]``. A topic/thread reply gets its
        own session; plain group or DM messages share one session per chat.
        """
        parts = [source.platform, source.chat_id]
        if source.thread_id:
            parts.append(source.thread_id)
        return "-".join(parts)

    def _platform_display(self, source: MessageSource) -> str:
        """Human-readable origin for the sidebar, e.g. "飞书·群聊"."""
        label = self._PLATFORM_LABELS.get(source.platform, source.platform or "Chat")
        is_group = getattr(source, "chat_type", None) == ChatType.GROUP
        return f"{label}·{'群聊' if is_group else '私聊'}"

    def _sync_chat_to_desktop(
        self,
        store: Any,
        source: MessageSource,
        user_input: str,
        response: Optional[str],
    ) -> None:
        """Persist an IM conversation turn to the desktop store.

        One desktop session per chat (+ topic), keyed by
        ``{platform}-{chat_id}[-{thread_id}]`` so a Feishu topic, a plain group
        chat, and a DM never collapse into the same sidebar entry.
        """
        desktop_sid = self._chat_desktop_session_id(source)
        existing = store.get(desktop_sid)

        if existing is None:
            first_line = (user_input or "").strip().split("\n")[0][:60]
            base = self._platform_display(source)
            title = f"{base} · {first_line}" if first_line else base
            agent_type = store.get_config("default_agent", "claude-code-sdk")
            store.create(
                session_id=desktop_sid,
                agent_type=agent_type,
                title=title,
                platform=source.platform,
                chat_id=source.chat_id,
                thread_id=source.thread_id,
                chat_type=(
                    "group"
                    if getattr(source, "chat_type", None) == ChatType.GROUP
                    else "p2p"
                ),
                source=base,
            )
            history: list[dict[str, Any]] = []
        else:
            history = list(existing.history)

        history.append({"role": "user", "content": user_input})
        if response:
            history.append({"role": "assistant", "content": str(response)})

        store.update_history(desktop_sid, history)
        store.update(desktop_sid, last_active=time.time())
        logger.debug(
            "Synced %d messages to desktop session %s (%s)",
            len(history), desktop_sid, source.platform,
        )

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

        # IM platforms use their own chat/topic-keyed sync; email keeps the
        # In-Reply-To / subject threading below.
        if source.platform != "email":
            self._sync_chat_to_desktop(store, source, user_input, response)
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
            agent_type = store.get_config("default_agent", "claude-code-sdk")
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
