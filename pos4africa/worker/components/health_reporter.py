"""
health_reporter.py — HealthReporter

Collects stats from all components on a node and publishes
them to the Manager via a Redis hash (polled by the Monitor).

Key: health:{node_id}
TTL: 60s — if a node dies, its health entry expires automatically
     and the Monitor can detect it.
"""

from __future__ import annotations

import asyncio
import time

import orjson

from pos4africa.config.settings import settings
from pos4africa.worker.components.base import BaseComponent
from pos4africa.manager.memory.store import MemoryStore

_HEALTH_TTL = 60  # seconds — node is considered dead after this

class HealthReporter(BaseComponent):
      def __init__(self, node_id: str, memory: MemoryStore) -> None:
            super().__init__(node_id, memory)
            
            self._start_time = time.monotonic()
            self._component_refs: list[BaseComponent] = []
            self._report_task: asyncio.Task | None = None
            
      def register(self, *components: BaseComponent) -> None:
            """Register pipeline components so we can pull their stats."""
            self._component_refs.extend(components)
            
      async def start(self) -> None:
            self._report_task = asyncio.create_task(self._report_loop())
            
            
      async def stop(self) -> None:
            if self._report_task:
                  self._report_task.cancel()
                  
      async def run(self) -> dict:
            
            report = {
                  "node_id": self.node_id,
                  "uptime_seconds": round(time.monotonic() - self._start_time, 1),
                  "timestamp": time.time(),
                  "components": [c.stats for c in self._component_refs]
            }
            
            key = f"health:{self.node_id}"
            await self.memory.ltm.redis_set(key, report, ex=_HEALTH_TTL)
            return report
            
            
      async def _report_loop(self) -> None:
            interval = 10
            while True:
                  try:
                        await self.run()
                  except Exception as exc:
                        self.log.warning("health_reporter.error", error=str(exc))
                        await asyncio.sleep(interval)