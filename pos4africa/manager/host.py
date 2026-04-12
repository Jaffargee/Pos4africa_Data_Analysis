"""
host.py — HostManager

The top-level orchestrator. Responsibilities:
  - Spawn and track all WorkerNode instances
  - Run the Scheduler to assign ScrapeJobs to nodes
  - Run the Monitor to watch node health
  - Run the Manager Egress (RabbitMQ → Supabase)
  - Handle graceful shutdown
"""

from __future__ import annotations

import asyncio
import signal

from pos4africa.config.settings import settings
from pos4africa.manager.egress.consumer import ManagerEgressConsumer
from pos4africa.manager.monitor import NodeMonitor
from pos4africa.manager.registry import WorkerRegistry
from pos4africa.manager.scheduler import Scheduler
from pos4africa.shared.utils.logger import get_logger
from pos4africa.worker.node import WorkerNode
from uuid import uuid5, NAMESPACE_DNS
from hashlib import sha256

log = get_logger(__name__)


class HostManager:
      def __init__(self) -> None:
            self._registry = WorkerRegistry()
            self._nodes: list[WorkerNode] = []
            self._scheduler: Scheduler | None = None
            self._monitor: NodeMonitor | None = None
            self._egress_consumer: ManagerEgressConsumer | None = None
            self._shutdown_event = asyncio.Event()

      async def run(self) -> None:
            log.info("host_manager.starting", worker_count=settings.worker_count)

            # Spawn worker nodes
            for i in range(settings.worker_count):
                  node_id = sha256(str(uuid5(NAMESPACE_DNS, f"node_{i+1:02d}")).encode()).hexdigest()
                  node = WorkerNode(node_id=node_id)
                  self._nodes.append(node)
                  self._registry.register(node_id, node)

            # Build manager subsystems
            self._scheduler = Scheduler(registry=self._registry)
            self._monitor = NodeMonitor(registry=self._registry)
            self._egress_consumer = ManagerEgressConsumer()

            # Wire shutdown signal
            # loop = asyncio.get_running_loop()
            # for sig in (signal.SIGINT, signal.SIGTERM):
            #       loop.add_signal_handler(sig, self._shutdown_event.set)

            # Run everything concurrently
            # node_tasks = [asyncio.create_task(n.start(), name=n.node_id) for n in self._nodes]
            await asyncio.gather(*[node.start() for node in self._nodes])

            manager_tasks = [
                  asyncio.create_task(self._scheduler.run(), name="scheduler"),
                  asyncio.create_task(self._monitor.run(), name="monitor"),
                  asyncio.create_task(self._egress_consumer.run(), name="egress_consumer"),
                  asyncio.create_task(self._shutdown_event.wait(), name="shutdown_watcher"),
            ]

            all_tasks = node_tasks + manager_tasks

            try:
                  # Wait until shutdown signal
                  done, pending = await asyncio.wait(
                        all_tasks,
                        return_when=asyncio.FIRST_COMPLETED,
                  )
                  
                  for task in done:
                        if task.exception():
                              log.error(
                                    "host_manager.task_failed",
                                    task=task.get_name(),
                                    error=str(task.exception()),
                              )
            finally:
                  log.info("host_manager.shutting_down")
                  await self._graceful_shutdown(all_tasks)
                  
            try:
                  # Wait for keyboard interrupt or error
                  await self._wait_for_shutdown(all_tasks)
            except KeyboardInterrupt:
                  log.info("host_manager.keyboard_interrupt")
            except Exception as exc:
                  log.error("host_manager.unexpected_error", error=str(exc))
            finally:
                  log.info("host_manager.shutting_down")
                  await self._graceful_shutdown(all_tasks)
                  
      async def _wait_for_shutdown(self, tasks: list[asyncio.Task]) -> None:
            """Wait for shutdown signal or task failure"""
            while not self._shutdown_event.is_set():
                  # Check if any task failed
                  for task in tasks:
                        if task.done() and task.exception():
                              log.error(
                                    "host_manager.task_failed",
                                    task=task.get_name(),
                                    error=str(task.exception())
                              )
                              self._shutdown_event.set()
                              break

      async def _graceful_shutdown(self, tasks: list[asyncio.Task]) -> None:
            # Stop nodes
            for node in self._nodes:
                  try:
                        await node.stop()
                  except Exception as exc:
                        log.error("host_manager.node_stop_error", node_id=node.node_id, error=str(exc))
            for task in tasks:
                  task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            log.info("host_manager.shutdown_complete")