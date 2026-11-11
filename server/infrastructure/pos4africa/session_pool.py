from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager

from server.domain.errors import PosSessionExpiredError, PosUnavailableError

log = logging.getLogger(__name__)


class PosSessionPool:
    """A fixed set of logged-in ``PosConnector`` sessions.

    POS keeps the register cart *in the session*, so a session may run only one
    stateful flow at a time.  ``acquire()`` hands a session to one caller at a time;
    other callers wait (bounded) instead of corrupting each other's cart.
    A session that comes back expired is replaced with a fresh login.
    """

    def __init__(self, size: int, acquire_timeout: float = 30.0, factory: Callable | None = None):
        self._size = size
        self._timeout = acquire_timeout
        self._factory = factory or self._default_factory
        self._queue: asyncio.Queue = asyncio.Queue()
        self._all: list = []

    @staticmethod
    def _default_factory():
        from pos4africa.infra.pos_connector import PosConnector  # lazy: needs POS credentials

        return PosConnector()

    async def start(self) -> None:
        for _ in range(self._size):
            conn = self._factory()
            await conn.init()
            self._all.append(conn)
            self._queue.put_nowait(conn)

    async def stop(self) -> None:
        for conn in self._all:
            try:
                await conn.close()
            except Exception:  # pragma: no cover
                log.exception("error closing POS session")
        self._all.clear()

    @asynccontextmanager
    async def acquire(self) -> AsyncIterator:
        try:
            conn = await asyncio.wait_for(self._queue.get(), timeout=self._timeout)
        except asyncio.TimeoutError as exc:
            raise PosUnavailableError("All POS sessions are busy; try again shortly") from exc
        expired = False
        try:
            yield conn
        except PosSessionExpiredError:
            expired = True
            raise
        finally:
            if expired:
                conn = await self._relogin(conn)
            self._queue.put_nowait(conn)

    async def _relogin(self, old):
        try:
            await old.close()
            fresh = self._factory()
            await fresh.init()
            self._all = [fresh if c is old else c for c in self._all]
            return fresh
        except Exception:
            log.exception("POS re-login failed; keeping the old session")
            return old
