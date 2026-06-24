"""
CronManager — lifecycle wrapper for the cron subsystem.

Owns the background ticker task and provides CRUD methods that delegate
to :mod:`agent_gateway.cron.jobs`.  Integrated into the FastAPI app via
the ``lifespan`` context manager in :mod:`agent_gateway.server.app`.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, List, Optional

from agent_gateway.cron import jobs
from agent_gateway.cron.scheduler import tick

logger = logging.getLogger(__name__)


class CronManager:
    """Manages the cron ticker lifecycle and exposes CRUD operations.

    Usage::

        manager = CronManager(store)
        await manager.start()
        # ... app runs ...
        await manager.stop()
    """

    def __init__(self, store: Any, runner: Any = None) -> None:
        self._store = store
        self._runner = runner
        self._task: Optional[asyncio.Task] = None
        self._running = False

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the background ticker."""
        if self._running:
            return
        self._running = True
        self._task = asyncio.create_task(self._tick_loop())
        logger.info("Cron manager started")

    async def stop(self) -> None:
        """Stop the background ticker."""
        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Cron manager stopped")

    async def _tick_loop(self) -> None:
        """Background loop: tick every 60 seconds."""
        while self._running:
            try:
                await tick(
                    self._store,
                    verbose=True,
                    runner=self._runner,
                    cron_manager=self,
                )
            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error("Cron tick error: %s", exc)
            await asyncio.sleep(60)

    # ------------------------------------------------------------------
    # CRUD — delegates to cron/jobs.py
    # ------------------------------------------------------------------

    def list_jobs(self) -> List[Dict[str, Any]]:
        """List all enabled cron jobs."""
        return jobs.list_jobs(include_disabled=True)

    def create_job(self, **kwargs) -> Dict[str, Any]:
        """Create a new cron job."""
        return jobs.create_job(**kwargs)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Get a single cron job by ID."""
        return jobs.get_job(job_id)

    def update_job(self, job_id: str, updates: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Update a cron job."""
        return jobs.update_job(job_id, updates)

    def delete_job(self, job_id: str) -> bool:
        """Delete a cron job."""
        return jobs.remove_job(job_id)

    def pause_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Pause a cron job."""
        return jobs.pause_job(job_id)

    def resume_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Resume a paused cron job."""
        return jobs.resume_job(job_id)

    def trigger_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Trigger a cron job to run on the next tick."""
        return jobs.trigger_job(job_id)

    def list_job_outputs(self, job_id: str) -> List[Dict[str, Any]]:
        """List a job's saved per-tick outputs (newest-first)."""
        return jobs.list_job_outputs(job_id)
