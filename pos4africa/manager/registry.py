"""
registry.py — WorkerRegistry

Tracks all live worker nodes and provides a lookup by node_id.
Also exposes a round-robin node selector for the Scheduler.
"""

from __future__ import annotations

import itertools

from pos4africa.worker.node import WorkerNode


class WorkerRegistry:
      def __init__(self) -> None:
            self._nodes: dict[str, WorkerNode] = {}
            self._rr: itertools.cycle | None = None  # round-robin iterator

      def register(self, node_id: str, node: WorkerNode) -> None:
            self._nodes[node_id] = node
            self._rr = itertools.cycle(list(self._nodes.keys()))

      def get(self, node_id: str) -> WorkerNode | None:
            return self._nodes.get(node_id)

      def all_nodes(self) -> list[WorkerNode]:
            return list(self._nodes.values())

      def next_node(self) -> WorkerNode | None:
            """Round-robin node selection for job assignment."""
            if not self._rr:
                  return None
            node_id = next(self._rr)
            return self._nodes.get(node_id)

      @property
      def node_ids(self) -> list[str]:
            return list(self._nodes.keys())