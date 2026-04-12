"""
monitor.py — NodeMonitor

Polls Redis for health heartbeats published by each WorkerNode's
HealthReporter. If a node's heartbeat expires (TTL lapses), the
Monitor logs an alert and can trigger a restart.
"""

from __future__ import annotations

import asyncio

import orjson

from pos4africa.infra.redis_client import get_redis
from pos4africa.manager.registry import WorkerRegistry
from pos4africa.shared.utils.logger import get_logger

log = get_logger(__name__)

_POLL_INTERVAL = 15  # seconds


class NodeMonitor:
      def __init__(self, registry: WorkerRegistry) -> None:
            self._registry = registry

      async def run(self) -> None:
            redis = await get_redis()
            while True:
                  await asyncio.sleep(_POLL_INTERVAL)
                  for node_id in self._registry.node_ids:
                        key = f"health:{node_id}"
                        raw = await redis.get(key)
                        if raw is None:
                              log.error(
                                    "monitor.node_heartbeat_missing",
                                    node_id=node_id,
                                    action="investigate — node may be dead or stalled",
                              )
                        else:
                              report = orjson.loads(raw)
                              self._evaluate(node_id, report)

      def _evaluate(self, node_id: str, report: dict) -> None:
            for comp in report.get("components", []):
                  errors = comp.get("errors", 0)
                  processed = comp.get("processed", 0)
                  if processed > 0:
                        error_rate = errors / processed
                  if error_rate > 0.1:  # >10% error rate
                        log.warning(
                              "monitor.high_error_rate",
                              node_id=node_id,
                              component=comp["component"],
                              error_rate=f"{error_rate:.1%}",
                        )