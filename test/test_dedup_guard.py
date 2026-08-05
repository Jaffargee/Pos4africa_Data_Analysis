# tests/test_dedup_guard.py
import pytest

from pos4africa.worker.components.dedup_guard import DedupGuard


class FakeLTM:
      def __init__(self):
            self._seen = set()

      async def is_duplicate(self, fp):
            return fp in self._seen

      async def mark_seen(self, fp):
            self._seen.add(fp)
            return None


class FakeMemory:
      def __init__(self):
            self.ltm = FakeLTM()


@pytest.mark.asyncio
async def test_dedup_guard_marks_and_detects_duplicates():
      memory = FakeMemory()
      guard = DedupGuard(node_id="n", memory=memory)

      first = await guard.run(1)
      assert first is False
      assert guard.skipped_count == 0

      # Second time should be detected as duplicate
      second = await guard.run(1)
      assert second is True
      assert guard.skipped_count == 1