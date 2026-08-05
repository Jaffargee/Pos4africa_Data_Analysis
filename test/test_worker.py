import pytest

from pos4africa.worker.node import WorkerNode, OnErrorPolicy
from pos4africa.worker.pipeline.base import BaseComponent


class DummyMemoryStore:
      """A minimal in-memory stand-in for the real MemoryStore used in tests."""

      async def initialise(self) -> None:  # pragma: no cover - trivial
            return None

      async def close(self) -> None:  # pragma: no cover - trivial
            return None


class IncComponent(BaseComponent):
      async def run(self, ctx) -> None:
            # increment a metric to show the component ran
            ctx.metrics["loaded"] += 1


class RaisingComponent(BaseComponent):
      def __init__(self, on_error=OnErrorPolicy.CONTINUE) -> None:
            super().__init__(on_error=on_error)

      async def run(self, ctx) -> None:
            raise RuntimeError("component failure")


@pytest.mark.asyncio
async def test_run_once_requires_start():
      node = WorkerNode("test-node", components=[])

      with pytest.raises(RuntimeError):
            await node.run_once()


@pytest.mark.asyncio
async def test_run_once_continues_on_error(monkeypatch):
      # Ensure WorkerNode uses our dummy MemoryStore so no external resources are needed
      monkeypatch.setattr("pos4africa.worker.node.MemoryStore", DummyMemoryStore)

      c1 = IncComponent()
      c2 = RaisingComponent(on_error=OnErrorPolicy.CONTINUE)
      c3 = IncComponent()

      node = WorkerNode("test-node", components=[c1, c2, c3])

      await node.start()
      try:
            summary = await node.run_once()
      finally:
            await node.stop()

      # two components incremented the "loaded" counter
      assert summary["loaded"] == 2


@pytest.mark.asyncio
async def test_run_once_aborts_on_error(monkeypatch):
      monkeypatch.setattr("pos4africa.worker.node.MemoryStore", DummyMemoryStore)

      c = RaisingComponent(on_error=OnErrorPolicy.ABORT)
      node = WorkerNode("test-node", components=[c])

      await node.start()
      try:
            with pytest.raises(RuntimeError):
                  await node.run_once()
      finally:
            await node.stop()