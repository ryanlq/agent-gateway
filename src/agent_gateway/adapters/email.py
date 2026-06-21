"""
Email platform adapter for the agent gateway.

Allows users to interact with the agent by sending emails.
Uses IMAP to receive and SMTP to send messages.

Environment variables:
    EMAIL_IMAP_HOST     — IMAP server host (e.g., imap.gmail.com)
    EMAIL_IMAP_PORT     — IMAP server port (default: 993)
    EMAIL_SMTP_HOST     — SMTP server host (e.g., smtp.gmail.com)
    EMAIL_SMTP_PORT     — SMTP server port (default: 587)
    EMAIL_ADDRESS       — Email address for the agent
    EMAIL_PASSWORD      — Email password or app-specific password
    EMAIL_POLL_INTERVAL — Seconds between mailbox checks (default: 15)
    EMAIL_ALLOWED_USERS — Comma-separated list of allowed sender addresses
    EMAIL_ALLOW_ALL_USERS — Set to "true" to allow all senders
    EMAIL_HOME_ADDRESS  — Home channel address for cron delivery

Ported from the original Hermes agent gateway.
"""

from __future__ import annotations

import asyncio
import email as email_lib
import imaplib
import logging
import re
import smtplib
import ssl
import uuid
from email.header import decode_header
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import formatdate
from email import encoders
from pathlib import Path
from typing import Any, Optional
from urllib.parse import unquote as _unquote

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
from agent_gateway.adapters._runtime import resolve_credential

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Automated sender filtering
# ---------------------------------------------------------------------------

# Automated sender patterns — emails from these are silently ignored
_NOREPLY_PATTERNS = (
    "noreply", "no-reply", "no_reply", "donotreply", "do-not-reply",
    "mailer-daemon", "postmaster", "bounce", "notifications@",
    "automated@", "auto-confirm", "auto-reply", "automailer",
)

# RFC headers that indicate bulk/automated mail
_AUTOMATED_HEADERS = {
    "Auto-Submitted": lambda v: v.lower() != "no",
    "Precedence": lambda v: v.lower() in {"bulk", "list", "junk"},
    "X-Auto-Response-Suppress": lambda v: bool(v),
    "List-Unsubscribe": lambda v: bool(v),
}

# Gmail-safe max length per email body
MAX_MESSAGE_LENGTH = 50_000

# Supported image extensions for inline detection
_IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".gif", ".webp"}

# ---------------------------------------------------------------------------
# IMAP helpers
# ---------------------------------------------------------------------------


def _send_imap_id(imap: imaplib.IMAP4) -> None:
    """Send RFC 2971 IMAP ID command identifying this client.

    Required by 163/NetEase mailbox after LOGIN: without it, every UID
    SEARCH/FETCH returns ``BYE Unsafe Login`` and disconnects.  Other
    IMAP servers either honor it silently or reject the unknown command;
    we swallow failures so non-supporting servers keep working.
    """
    try:
        try:
            from agent_gateway import __version__ as _gw_version
        except Exception:
            _gw_version = "0"
        imap.xatom(
            "ID",
            f'("name" "agent-gateway" "version" "{_gw_version}" '
            '"vendor" "agent-gateway" '
            '"support-email" "noreply@agent-gateway")',
        )
    except Exception as e:  # noqa: BLE001 — best-effort, never fatal
        logger.debug("[Email] IMAP ID command not accepted: %s", e)


# ---------------------------------------------------------------------------
# Email parsing helpers
# ---------------------------------------------------------------------------


def _is_automated_sender(address: str, headers: dict) -> bool:
    """Return True if this email is from an automated/noreply source."""
    addr = address.lower()
    if any(pattern in addr for pattern in _NOREPLY_PATTERNS):
        return True
    for header, check in _AUTOMATED_HEADERS.items():
        value = headers.get(header, "")
        if value and check(value):
            return True
    return False


def _decode_header_value(raw: str) -> str:
    """Decode an RFC 2047 encoded email header into a plain string."""
    parts = decode_header(raw)
    decoded = []
    for part, charset in parts:
        if isinstance(part, bytes):
            decoded.append(part.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(part)
    return " ".join(decoded)


def _extract_text_body(msg: email_lib.message.Message) -> str:
    """Extract the plain-text body from a potentially multipart email."""
    if msg.is_multipart():
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue
            if content_type == "text/plain":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    return payload.decode(charset, errors="replace")
        # Fallback: try text/html and strip tags
        for part in msg.walk():
            content_type = part.get_content_type()
            disposition = str(part.get("Content-Disposition", ""))
            if "attachment" in disposition:
                continue
            if content_type == "text/html":
                payload = part.get_payload(decode=True)
                if payload:
                    charset = part.get_content_charset() or "utf-8"
                    html = payload.decode(charset, errors="replace")
                    return _strip_html(html)
        return ""
    else:
        payload = msg.get_payload(decode=True)
        if payload:
            charset = msg.get_content_charset() or "utf-8"
            text = payload.decode(charset, errors="replace")
            if msg.get_content_type() == "text/html":
                return _strip_html(text)
            return text
        return ""


def _strip_html(html: str) -> str:
    """Naive HTML tag stripper for fallback text extraction."""
    text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
    text = re.sub(r"<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"</p>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    text = re.sub(r"&nbsp;", " ", text)
    text = re.sub(r"&amp;", "&", text)
    text = re.sub(r"&lt;", "<", text)
    text = re.sub(r"&gt;", ">", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def _normalize_subject(subject: str) -> str:
    """Strip Re:/Fwd: prefixes to produce a stable thread identifier."""
    s = subject.strip()
    while True:
        lower = s.lower()
        if lower.startswith("re:"):
            s = s[3:].strip()
        elif lower.startswith("fwd:"):
            s = s[4:].strip()
        else:
            break
    return s


def _extract_email_address(raw: str) -> str:
    """Extract bare email address from 'Name <addr>' format."""
    match = re.search(r"<([^>]+)>", raw)
    if match:
        return match.group(1).strip().lower()
    return raw.strip().lower()


def _extract_attachments(
    msg: email_lib.message.Message,
    media_cache: MediaCache,
    *,
    skip_attachments: bool = False,
) -> list[dict[str, Any]]:
    """Extract attachment metadata and cache files locally.

    When *skip_attachments* is True, all attachment/inline parts are ignored
    (useful for malware protection or bandwidth savings).
    """
    attachments: list[dict[str, Any]] = []
    if not msg.is_multipart():
        return attachments

    for part in msg.walk():
        disposition = str(part.get("Content-Disposition", ""))
        if skip_attachments and ("attachment" in disposition or "inline" in disposition):
            continue
        if "attachment" not in disposition and "inline" not in disposition:
            continue
        # Skip text/plain and text/html body parts
        content_type = part.get_content_type()
        if content_type in {"text/plain", "text/html"} and "attachment" not in disposition:
            continue

        filename = part.get_filename()
        if filename:
            filename = _decode_header_value(filename)
        else:
            ext = part.get_content_subtype() or "bin"
            filename = f"attachment.{ext}"

        payload = part.get_payload(decode=True)
        if not payload:
            continue

        ext = Path(filename).suffix.lower()
        if ext in _IMAGE_EXTS:
            try:
                cached_path = media_cache.save_image(payload, ext=ext, filename=filename)
            except ValueError:
                logger.debug("Skipping non-image attachment %s (invalid magic bytes)", filename)
                continue
            attachments.append({
                "path": cached_path,
                "filename": filename,
                "type": "image",
                "media_type": content_type,
            })
        else:
            cached_path = media_cache.save_document(payload, filename=filename)
            attachments.append({
                "path": cached_path,
                "filename": filename,
                "type": "document",
                "media_type": content_type,
            })

    return attachments


# ---------------------------------------------------------------------------
# Dependency check
# ---------------------------------------------------------------------------


def _check_email_deps() -> bool:
    """Check if email platform dependencies are available.

    Email uses only the Python standard library (imaplib, smtplib, email),
    so this always returns True.
    """
    return True


# ---------------------------------------------------------------------------
# Adapter
# ---------------------------------------------------------------------------


class EmailAdapter(BasePlatformAdapter):
    """Email gateway adapter using IMAP (receive) and SMTP (send)."""

    def __init__(self, config: dict[str, Any]) -> None:
        super().__init__(config)
        # When called via GatewayRunner, config is PlatformConfig.__dict__ which
        # nests all YAML fields under "extra".  Flatten so we read from one level.
        cfg = config.get("extra") if isinstance(config.get("extra"), dict) else config

        self._address = resolve_credential(cfg.get("address"), env="EMAIL_ADDRESS")
        self._password = resolve_credential(cfg.get("password"), env="EMAIL_PASSWORD")
        self._imap_host = resolve_credential(cfg.get("imap_host"), env="EMAIL_IMAP_HOST")
        self._imap_port = resolve_credential(
            cfg.get("imap_port"), env="EMAIL_IMAP_PORT", default="993", cast=int
        )
        self._smtp_host = resolve_credential(cfg.get("smtp_host"), env="EMAIL_SMTP_HOST")
        self._smtp_port = resolve_credential(
            cfg.get("smtp_port"), env="EMAIL_SMTP_PORT", default="587", cast=int
        )
        self._poll_interval = resolve_credential(
            cfg.get("poll_interval"), env="EMAIL_POLL_INTERVAL", default="15", cast=int
        )

        # Skip attachments — configured via gateway.yaml or config dict
        self._skip_attachments = cfg.get("skip_attachments", False)

        # Access control — config dict > env var
        raw_allowed = resolve_credential(cfg.get("allowed_users"), env="EMAIL_ALLOWED_USERS")
        if isinstance(raw_allowed, list):
            self._allowed_users: set[str] = {a.strip().lower() for a in raw_allowed if a.strip()}
        elif isinstance(raw_allowed, str) and raw_allowed.strip():
            self._allowed_users = {a.strip().lower() for a in raw_allowed.split(",") if a.strip()}
        else:
            self._allowed_users = set()

        raw_allow_all = resolve_credential(cfg.get("allow_all_users"), env="EMAIL_ALLOW_ALL_USERS")
        self._allow_all_users = str(raw_allow_all).lower() in ("true", "1", "yes")

        self._media_cache = MediaCache()

        # Track message IDs we've already processed to avoid duplicates
        self._seen_uids: set[bytes] = set()
        self._seen_uids_max: int = 2000
        self._pending_on_connect: int = 0
        self._poll_task: Optional[asyncio.Task] = None

        # Map (chat_id, normalized_subject) -> last subject + message-id for threading
        self._thread_context: dict[tuple[str, str], dict[str, str]] = {}

        # Reverse index: RFC Message-ID -> (addr, normalized_subject) for
        # In-Reply-To thread resolution (covers non-standard Re: prefixes,
        # subject changes, and localised reply markers like Aw:, Réf :, etc.)
        self._msg_id_to_thread: dict[str, tuple[str, str]] = {}

        self._name = "Email"
        logger.info("[Email] Adapter initialized for %s", self._address)

    def _trim_seen_uids(self) -> None:
        """Keep only the most recent UIDs to prevent unbounded memory growth."""
        if len(self._seen_uids) <= self._seen_uids_max:
            return
        try:
            sorted_uids = sorted(self._seen_uids, key=lambda u: int(u))
            keep = self._seen_uids_max // 2
            self._seen_uids = set(sorted_uids[-keep:])
            logger.debug("[Email] Trimmed seen UIDs to %d entries", len(self._seen_uids))
        except (ValueError, TypeError):
            self._seen_uids = set(list(self._seen_uids)[-self._seen_uids_max // 2:])

    # -- Seen-UID persistence ----------------------------------------------

    def _seen_uids_state_key(self) -> str:
        """Per-address key for the persistent state file."""
        safe = self._address.replace("@", "_at_").replace(".", "_")
        return f"email_seen_uids_{safe}"

    def _load_seen_uids(self) -> None:
        """Restore ``_seen_uids`` from disk so restarts don't reprocess."""
        try:
            from agent_gateway.utils.state import load_state
            data = load_state(self._seen_uids_state_key())
            raw = data.get("uids", [])
            if isinstance(raw, list):
                for entry in raw:
                    if isinstance(entry, str) and entry:
                        self._seen_uids.add(entry.encode())
            if self._seen_uids:
                logger.info(
                    "[Email] Loaded %d persisted seen UID(s)", len(self._seen_uids)
                )
        except Exception as exc:
            logger.debug("[Email] Could not load persisted seen UIDs: %s", exc)

    def _save_seen_uids(self) -> None:
        """Persist ``_seen_uids`` to disk."""
        if not self._seen_uids:
            return
        try:
            from agent_gateway.utils.state import save_state
            save_state(
                self._seen_uids_state_key(),
                {"uids": [u.decode(errors="replace") for u in self._seen_uids]},
            )
        except Exception as exc:
            logger.debug("[Email] Could not persist seen UIDs: %s", exc)

    def _lookup_thread_context(
        self, to_addr: str, thread_id: str | None = None
    ) -> dict[str, str]:
        """Look up thread context by (addr, normalized_subject).

        Falls back to the most recent entry for *to_addr* when *thread_id*
        is ``None`` or not found.
        """
        if thread_id:
            ctx = self._thread_context.get((to_addr, thread_id))
            if ctx:
                return ctx
        # Fallback: find any entry for this address
        for (addr, _subj), ctx in self._thread_context.items():
            if addr == to_addr:
                return ctx
        return {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _connect_imap_sync(self, baseline_uids: set[bytes] | None = None) -> dict:
        """Blocking IMAP connection test — runs in executor thread.

        If *baseline_uids* is provided (and empty), fills it with all current
        inbox UIDs so that pre-existing messages are skipped on first run.
        Returns ``{"pending": int, "baseline_seeded": bool}``.
        """
        imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
        imap.login(self._address, self._password)
        _send_imap_id(imap)
        imap.select("INBOX")

        result = {"pending": 0, "baseline_seeded": False}

        if baseline_uids is not None and len(baseline_uids) == 0:
            # First run with no persisted state — seed _seen_uids with every
            # existing inbox UID so old mail is never processed.  This preserves
            # the pre-refactor behaviour where users never saw surprise backlog.
            status, data = imap.uid("search", None, "ALL")
            if status == "OK" and data and data[0]:
                for uid in data[0].split():
                    baseline_uids.add(uid)
                result["baseline_seeded"] = True
                logger.info(
                    "[Email] First run: seeded %d existing message(s) as seen "
                    "(will not be processed).",
                    len(baseline_uids),
                )
        else:
            # We have persisted state — count UNSEEN for backlog reporting.
            status, data = imap.uid("search", None, "UNSEEN")
            if status == "OK" and data and data[0]:
                # Only count messages we haven't seen before
                unseen_uids = set(data[0].split())
                if baseline_uids is not None:
                    unseen_uids -= baseline_uids
                result["pending"] = len(unseen_uids)

        imap.logout()
        logger.info(
            "[Email] IMAP connection test passed. %d pending message(s).",
            result["pending"],
        )
        return result

    def _connect_smtp_sync(self) -> bool:
        """Blocking SMTP connection — runs in executor thread."""
        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        smtp.starttls(context=ssl.create_default_context())
        smtp.login(self._address, self._password)
        smtp.quit()
        logger.info("[Email] SMTP connection test passed.")
        return True

    async def connect(self) -> bool:
        """Connect to the IMAP server and start polling for new messages."""
        loop = asyncio.get_running_loop()

        # Restore persisted seen UIDs so restarts don't reprocess already-handled mail.
        self._load_seen_uids()

        try:
            result = await loop.run_in_executor(
                None, self._connect_imap_sync, self._seen_uids
            )
            self._pending_on_connect = result.get("pending", 0)
            if result.get("baseline_seeded"):
                # Persist the seeded UIDs so subsequent restarts don't re-seed.
                self._save_seen_uids()
        except Exception as e:
            logger.error("[Email] IMAP connection failed: %s", e)
            self._set_fatal_error("imap_failed", str(e))
            return False

        try:
            await loop.run_in_executor(None, self._connect_smtp_sync)
        except Exception as e:
            logger.error("[Email] SMTP connection failed: %s", e)
            self._set_fatal_error("smtp_failed", str(e))
            return False

        self._mark_connected()
        self._poll_task = asyncio.create_task(self._poll_loop())
        print(f"[Email] Connected as {self._address}")
        return True

    async def disconnect(self) -> None:
        """Stop polling and disconnect."""
        self._running = False
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        self._save_seen_uids()
        self._mark_disconnected()
        logger.info("[Email] Disconnected.")

    # ------------------------------------------------------------------
    # Inbound: IMAP polling
    # ------------------------------------------------------------------

    async def _poll_loop(self) -> None:
        """Poll IMAP for new messages at regular intervals."""
        while self._running:
            try:
                await self._check_inbox()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("[Email] Poll error: %s", e)
            await asyncio.sleep(self._poll_interval)

    async def _check_inbox(self) -> None:
        """Check INBOX for unseen messages and dispatch them."""
        loop = asyncio.get_running_loop()
        messages = await loop.run_in_executor(None, self._fetch_new_messages)

        # Notify if backlog detected on first poll after connect
        if self._pending_on_connect > 0 and messages:
            backlog_count = len(messages)
            if backlog_count > 1:
                logger.info(
                    "[Email] 📬 Recovering %d message(s) that arrived during offline period",
                    backlog_count,
                )
            self._pending_on_connect = 0

        for msg_data in messages:
            await self._dispatch_message(msg_data)

        # Persist seen UIDs after each batch so a crash/restart won't reprocess
        # messages we already handled.
        if messages:
            self._save_seen_uids()

    def _fetch_new_messages(self) -> list[dict[str, Any]]:
        """Fetch new (unseen) messages from IMAP. Runs in executor thread."""
        results: list[dict[str, Any]] = []
        try:
            imap = imaplib.IMAP4_SSL(self._imap_host, self._imap_port, timeout=30)
            try:
                imap.login(self._address, self._password)
                _send_imap_id(imap)
                imap.select("INBOX")

                status, data = imap.uid("search", None, "UNSEEN")
                if status != "OK" or not data or not data[0]:
                    return results

                for uid in data[0].split():
                    if uid in self._seen_uids:
                        continue
                    self._seen_uids.add(uid)
                    if len(self._seen_uids) > self._seen_uids_max:
                        self._trim_seen_uids()

                    # Mark as \Seen on IMAP server so restarts don't reprocess
                    try:
                        imap.uid("store", uid, "+FLAGS", "(\\Seen)")
                    except Exception:
                        pass

                    status, msg_data = imap.uid("fetch", uid, "(RFC822)")
                    if status != "OK":
                        continue

                    raw_email = msg_data[0][1]
                    msg = email_lib.message_from_bytes(raw_email)

                    sender_raw = msg.get("From", "")
                    sender_addr = _extract_email_address(sender_raw)
                    sender_name = _decode_header_value(sender_raw)
                    if "<" in sender_name:
                        sender_name = sender_name.split("<")[0].strip().strip('"')

                    subject = _decode_header_value(msg.get("Subject", "(no subject)"))
                    message_id = msg.get("Message-ID", "")
                    in_reply_to = msg.get("In-Reply-To", "")
                    references = msg.get("References", "")

                    # Skip automated/noreply senders before any processing
                    msg_headers = dict(msg.items())
                    if _is_automated_sender(sender_addr, msg_headers):
                        logger.debug("[Email] Skipping automated sender: %s", sender_addr)
                        continue

                    body = _extract_text_body(msg)
                    attachments = _extract_attachments(
                        msg, self._media_cache,
                        skip_attachments=self._skip_attachments,
                    )

                    results.append({
                        "uid": uid,
                        "sender_addr": sender_addr,
                        "sender_name": sender_name,
                        "subject": subject,
                        "message_id": message_id,
                        "in_reply_to": in_reply_to,
                        "references": references,
                        "body": body,
                        "attachments": attachments,
                        "date": msg.get("Date", ""),
                    })
            finally:
                try:
                    imap.logout()
                except Exception:
                    pass
        except Exception as e:
            logger.error("[Email] IMAP fetch error: %s", e)
        return results

    async def _dispatch_message(self, msg_data: dict[str, Any]) -> None:
        """Convert a fetched email into a MessageEvent and dispatch it."""
        sender_addr = msg_data["sender_addr"]

        # Skip self-messages
        if sender_addr == self._address.lower():
            return

        # Never reply to automated senders
        if _is_automated_sender(sender_addr, {}):
            logger.debug("[Email] Dropping automated sender at dispatch: %s", sender_addr)
            return

        # Skip senders not in allowed_users (unless allow_all_users is set)
        if not self._allow_all_users and self._allowed_users:
            if sender_addr.lower() not in self._allowed_users:
                logger.debug("[Email] Dropping non-allowlisted sender at dispatch: %s", sender_addr)
                return

        subject = msg_data["subject"]
        body = msg_data["body"].strip()
        attachments = msg_data["attachments"]

        # Build message text: include subject as context
        text = body
        if subject and not subject.startswith("Re:"):
            text = f"[Subject: {subject}]\n\n{body}"

        # Determine message type and media
        media_urls: list[str] = []
        media_types: list[str] = []
        msg_type = MessageType.TEXT

        for att in attachments:
            media_urls.append(att["path"])
            media_types.append(att["media_type"])
            if att["type"] == "image":
                msg_type = MessageType.PHOTO

        # Resolve thread identity:
        # 1. In-Reply-To → look up the parent thread (most reliable — handles
        #    localised reply prefixes like Aw:, Réf :, Ответ: and subject edits)
        # 2. Subject normalization → strip Re:/Fwd: prefixes (fallback)
        in_reply_to = msg_data["in_reply_to"]
        normalized = _normalize_subject(subject)
        if in_reply_to and in_reply_to in self._msg_id_to_thread:
            parent_addr, parent_subject = self._msg_id_to_thread[in_reply_to]
            if parent_addr == sender_addr:
                normalized = parent_subject

        # Store thread context (keyed by sender + normalized subject)
        self._thread_context[(sender_addr, normalized)] = {
            "subject": subject,
            "message_id": msg_data["message_id"],
            "references": msg_data.get("references", ""),
        }

        # Register this message's Message-ID for future In-Reply-To lookups
        if msg_data["message_id"]:
            self._msg_id_to_thread[msg_data["message_id"]] = (sender_addr, normalized)

        source = MessageSource(
            platform="email",
            user_id=sender_addr,
            chat_id=sender_addr,
            thread_id=normalized or None,
            chat_type=ChatType.DM,
            display_name=msg_data["sender_name"] or sender_addr,
        )

        event = MessageEvent(
            text=text or "(empty email)",
            message_type=msg_type,
            source=source,
            message_id=msg_data["message_id"],
            media_urls=media_urls,
            media_types=media_types,
            reply_to_message_id=msg_data["in_reply_to"] or None,
            # Pass email metadata for session routing in the runner
            raw_message={
                "subject": subject,
                "sender": sender_addr,
                "message_id": msg_data.get("message_id"),
                "in_reply_to": msg_data.get("in_reply_to"),
                "references": msg_data.get("references", ""),
            },
        )

        logger.info("[Email] New message from %s: %s", sender_addr, subject)
        await self.handle_message(event)

    # ------------------------------------------------------------------
    # Outbound: SMTP sending
    # ------------------------------------------------------------------

    async def send(
        self,
        chat_id: str,
        content: str,
        *,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send an email reply to the given address."""
        try:
            loop = asyncio.get_running_loop()
            thread_id = (metadata or {}).get("thread_id")
            message_id = await loop.run_in_executor(
                None, self._send_email, chat_id, content, reply_to, thread_id
            )
            if message_id and thread_id:
                self._msg_id_to_thread[message_id] = (chat_id, thread_id)
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("[Email] Send failed to %s: %s", chat_id, e)
            return SendResult(success=False, error=str(e))

    def _send_email(
        self,
        to_addr: str,
        body: str,
        reply_to_msg_id: Optional[str] = None,
        thread_id: Optional[str] = None,
    ) -> str:
        """Send an email via SMTP. Runs in executor thread."""
        msg = MIMEMultipart()
        msg["From"] = self._address
        msg["To"] = to_addr

        # Thread context for reply
        ctx = self._lookup_thread_context(to_addr, thread_id)
        subject = ctx.get("subject", "Agent Gateway")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject

        # Threading headers — build full ancestor chain for References
        original_msg_id = reply_to_msg_id or ctx.get("message_id")
        if original_msg_id:
            msg["In-Reply-To"] = original_msg_id
            # Append the original msg-id to any existing references chain
            existing_refs = ctx.get("references", "").strip()
            if existing_refs:
                msg["References"] = f"{existing_refs} {original_msg_id}"
            else:
                msg["References"] = original_msg_id

        msg["Date"] = formatdate(localtime=True)
        msg_id = f"<agent-gw-{uuid.uuid4().hex[:12]}@{self._address.split('@')[1]}>"
        msg["Message-ID"] = msg_id

        msg.attach(MIMEText(body, "plain", "utf-8"))

        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        try:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(self._address, self._password)
            smtp.send_message(msg)
        finally:
            try:
                smtp.quit()
            except Exception:
                smtp.close()

        logger.info("[Email] Sent reply to %s (subject: %s)", to_addr, subject)
        return msg_id

    def _send_new_email(
        self,
        to_addr: str,
        subject: str,
        body: str,
    ) -> str:
        """Send a fresh (non-reply) email via SMTP. Runs in executor thread."""
        msg = MIMEMultipart()
        msg["From"] = self._address
        msg["To"] = to_addr
        msg["Subject"] = subject
        msg["Date"] = formatdate(localtime=True)
        msg_id = f"<agent-gw-{uuid.uuid4().hex[:12]}@{self._address.split('@')[1]}>"
        msg["Message-ID"] = msg_id

        msg.attach(MIMEText(body, "plain", "utf-8"))

        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        try:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(self._address, self._password)
            smtp.send_message(msg)
        finally:
            try:
                smtp.quit()
            except Exception:
                smtp.close()

        logger.info("[Email] Sent new email to %s (subject: %s)", to_addr, subject)
        return msg_id

    async def send_typing(self, chat_id: str, metadata: Optional[dict[str, Any]] = None) -> None:
        """Email has no typing indicator — no-op."""

    async def send_image(
        self,
        chat_id: str,
        image_url: str,
        caption: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send an image URL as part of an email body."""
        text = caption or ""
        text += f"\n\nImage: {image_url}"
        return await self.send(chat_id, text.strip(), reply_to=reply_to, metadata=metadata)

    async def send_multiple_images(
        self,
        chat_id: str,
        images: list[tuple[str, str]],
        metadata: Optional[dict[str, Any]] = None,
        human_delay: float = 0.0,
    ) -> None:
        """Send a batch of images as a single email with multiple MIME attachments."""
        if not images:
            return

        body_parts: list[str] = []
        local_paths: list[str] = []
        for image_url, alt_text in images:
            if alt_text:
                body_parts.append(alt_text)
            if image_url.startswith("file://"):
                local_path = _unquote(image_url[7:])
                if Path(local_path).exists():
                    local_paths.append(local_path)
                else:
                    logger.warning("[Email] Skipping missing image: %s", local_path)
            else:
                body_parts.append(f"Image: {image_url}")

        if not local_paths and not body_parts:
            return

        body = "\n\n".join(body_parts)

        try:
            loop = asyncio.get_running_loop()
            thread_id = (metadata or {}).get("thread_id")
            out_msg_id = await loop.run_in_executor(
                None,
                self._send_email_with_attachments,
                chat_id,
                body,
                local_paths,
                thread_id,
            )
            if out_msg_id and thread_id:
                self._msg_id_to_thread[out_msg_id] = (chat_id, thread_id)
        except Exception as e:
            logger.error("[Email] Multi-image send failed, falling back: %s", e, exc_info=True)
            await super().send_multiple_images(chat_id, images, metadata, human_delay)

    def _send_email_with_attachments(
        self,
        to_addr: str,
        body: str,
        file_paths: list[str],
        thread_id: Optional[str] = None,
    ) -> str:
        """Send an email with multiple file attachments via SMTP."""
        msg = MIMEMultipart()
        msg["From"] = self._address
        msg["To"] = to_addr

        ctx = self._lookup_thread_context(to_addr, thread_id)
        subject = ctx.get("subject", "Agent Gateway")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject

        original_msg_id = ctx.get("message_id")
        if original_msg_id:
            msg["In-Reply-To"] = original_msg_id
            existing_refs = ctx.get("references", "").strip()
            if existing_refs:
                msg["References"] = f"{existing_refs} {original_msg_id}"
            else:
                msg["References"] = original_msg_id

        msg["Date"] = formatdate(localtime=True)
        msg_id = f"<agent-gw-{uuid.uuid4().hex[:12]}@{self._address.split('@')[1]}>"
        msg["Message-ID"] = msg_id

        if body:
            msg.attach(MIMEText(body, "plain", "utf-8"))

        for file_path in file_paths:
            p = Path(file_path)
            try:
                with open(p, "rb") as f:
                    part = MIMEBase("application", "octet-stream")
                    part.set_payload(f.read())
                    encoders.encode_base64(part)
                    part.add_header("Content-Disposition", f"attachment; filename={p.name}")
                    msg.attach(part)
            except Exception as e:
                logger.warning("[Email] Failed to attach %s: %s", file_path, e)

        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        try:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(self._address, self._password)
            smtp.send_message(msg)
        finally:
            try:
                smtp.quit()
            except Exception:
                smtp.close()

        logger.info("[Email] Sent multi-attachment email to %s (%d files)", to_addr, len(file_paths))
        return msg_id

    async def send_document(
        self,
        chat_id: str,
        file_path: str,
        caption: Optional[str] = None,
        file_name: Optional[str] = None,
        reply_to: Optional[str] = None,
        metadata: Optional[dict[str, Any]] = None,
    ) -> SendResult:
        """Send a file as an email attachment."""
        try:
            loop = asyncio.get_running_loop()
            thread_id = (metadata or {}).get("thread_id")
            message_id = await loop.run_in_executor(
                None,
                self._send_email_with_attachment,
                chat_id,
                caption or "",
                file_path,
                file_name,
                thread_id,
            )
            if message_id and thread_id:
                self._msg_id_to_thread[message_id] = (chat_id, thread_id)
            return SendResult(success=True, message_id=message_id)
        except Exception as e:
            logger.error("[Email] Send document failed: %s", e)
            return SendResult(success=False, error=str(e))

    def _send_email_with_attachment(
        self,
        to_addr: str,
        body: str,
        file_path: str,
        file_name: Optional[str] = None,
        thread_id: Optional[str] = None,
    ) -> str:
        """Send an email with a file attachment via SMTP."""
        msg = MIMEMultipart()
        msg["From"] = self._address
        msg["To"] = to_addr

        ctx = self._lookup_thread_context(to_addr, thread_id)
        subject = ctx.get("subject", "Agent Gateway")
        if not subject.startswith("Re:"):
            subject = f"Re: {subject}"
        msg["Subject"] = subject

        original_msg_id = ctx.get("message_id")
        if original_msg_id:
            msg["In-Reply-To"] = original_msg_id
            existing_refs = ctx.get("references", "").strip()
            if existing_refs:
                msg["References"] = f"{existing_refs} {original_msg_id}"
            else:
                msg["References"] = original_msg_id

        msg["Date"] = formatdate(localtime=True)
        msg_id = f"<agent-gw-{uuid.uuid4().hex[:12]}@{self._address.split('@')[1]}>"
        msg["Message-ID"] = msg_id

        if body:
            msg.attach(MIMEText(body, "plain", "utf-8"))

        p = Path(file_path)
        fname = file_name or p.name
        with open(p, "rb") as f:
            part = MIMEBase("application", "octet-stream")
            part.set_payload(f.read())
            encoders.encode_base64(part)
            part.add_header("Content-Disposition", f"attachment; filename={fname}")
            msg.attach(part)

        smtp = smtplib.SMTP(self._smtp_host, self._smtp_port, timeout=30)
        try:
            smtp.starttls(context=ssl.create_default_context())
            smtp.login(self._address, self._password)
            smtp.send_message(msg)
        finally:
            try:
                smtp.quit()
            except Exception:
                smtp.close()

        return msg_id

    async def get_chat_info(self, chat_id: str) -> dict[str, Any]:
        """Return basic info about the email chat."""
        ctx = self._lookup_thread_context(chat_id)
        return {
            "name": chat_id,
            "type": "dm",
            "chat_id": chat_id,
            "subject": ctx.get("subject", ""),
        }


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


def register_email() -> None:
    """Register the Email adapter with the global registry."""
    from agent_gateway.core.registry import EnvVarDef

    registry.register(PlatformEntry(
        name="email",
        label="Email",
        adapter_factory=lambda cfg: EmailAdapter(cfg),
        check_fn=_check_email_deps,
        install_hint="Standard library only — no extra packages needed",
        required_env=[
            "EMAIL_ADDRESS",
            "EMAIL_PASSWORD",
            "EMAIL_IMAP_HOST",
            "EMAIL_SMTP_HOST",
        ],
        max_message_length=MAX_MESSAGE_LENGTH,
        emoji="📧",
        platform_hint="You are responding via email. Use plain text, avoid complex markdown.",
        source="builtin",
        allowed_users_env="EMAIL_ALLOWED_USERS",
        allow_all_env="EMAIL_ALLOW_ALL_USERS",
        env_var_defs=[
            EnvVarDef(
                key="EMAIL_ADDRESS",
                description="Email address for the agent",
                prompt="agent@example.com",
                required=True,
            ),
            EnvVarDef(
                key="EMAIL_PASSWORD",
                description="Email password or app-specific password",
                prompt="Enter password",
                is_password=True,
                required=True,
            ),
            EnvVarDef(
                key="EMAIL_IMAP_HOST",
                description="IMAP server host",
                prompt="imap.gmail.com",
                required=True,
            ),
            EnvVarDef(
                key="EMAIL_SMTP_HOST",
                description="SMTP server host",
                prompt="smtp.gmail.com",
                required=True,
            ),
            EnvVarDef(
                key="EMAIL_IMAP_PORT",
                description="IMAP server port",
                prompt="993",
                required=False,
                advanced=True,
            ),
            EnvVarDef(
                key="EMAIL_SMTP_PORT",
                description="SMTP server port",
                prompt="587",
                required=False,
                advanced=True,
            ),
            EnvVarDef(
                key="EMAIL_POLL_INTERVAL",
                description="Seconds between mailbox checks",
                prompt="15",
                required=False,
                advanced=True,
            ),
            EnvVarDef(
                key="EMAIL_ALLOWED_USERS",
                description="Comma-separated list of allowed sender addresses",
                prompt="user@example.com",
                required=False,
                advanced=False,
            ),
            EnvVarDef(
                key="EMAIL_ALLOW_ALL_USERS",
                description='Set to "true" to allow all senders',
                prompt="true",
                required=False,
                advanced=True,
            ),
            EnvVarDef(
                key="EMAIL_HOME_ADDRESS",
                description="Home channel address for cron delivery",
                prompt="user@example.com",
                required=False,
                advanced=True,
            ),
        ],
    ))
