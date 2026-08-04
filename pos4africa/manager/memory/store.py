from __future__ import annotations

from .long_term import LongTermMemory


class MemoryStore:
      def __init__(self) -> None:
            self.ltm = LongTermMemory()

      async def initialise(self) -> None:
            await self.ltm.warm_up()

      async def close(self) -> None:
            await self.ltm.shutdown()
