import asyncio
import logging
import sys
import time
import traceback

for s in (sys.stdout, sys.stderr):
    try:
        s.reconfigure(line_buffering=True)
    except Exception:
        pass
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("diag")


async def main():
    log.info("importing ...")
    from agent_gateway.agents.claude_code_sdk import ClaudeCodeSdkBridge

    log.info("instantiating bridge ...")
    bridge = ClaudeCodeSdkBridge(
        model="claude-sonnet-4-20250514", max_turns=2, permission_mode="acceptEdits"
    )
    log.info("calling bridge.stream() — first event should arrive within ~15s")
    count = 0
    t0 = time.monotonic()
    try:
        async for event in bridge.stream("diag", "Reply with exactly: OK", [], ""):
            now = time.monotonic() - t0
            count += 1
            kind = getattr(event, "kind", type(event).__name__)
            text = (getattr(event, "text", "") or "")[:60].replace("\n", "\\n")
            tool = getattr(event, "tool_name", "")
            log.info(
                "event #%d (t=%.2fs) kind=%s tool=%s text=%r",
                count,
                now,
                kind,
                tool,
                text,
            )
            if count > 30:
                break
    except Exception:
        traceback.print_exc()
    log.info(
        "total: %d events in %.2fs, session_id=%s",
        count,
        time.monotonic() - t0,
        bridge.captured_cli_session_id,
    )


asyncio.run(main())
