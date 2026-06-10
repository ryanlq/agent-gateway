"""
Cron job scheduling system for agent-gateway.

Allows scheduling automated tasks (cron expressions, intervals, one-shot)
that are executed by the configured AI agent.

Jobs are stored in ``~/.nexus-agent/cron/jobs.json`` with output saved to
``~/.nexus-agent/cron/output/{job_id}/``.  The scheduler ticks every 60
seconds via a background asyncio task managed by :class:`CronManager`.
"""
