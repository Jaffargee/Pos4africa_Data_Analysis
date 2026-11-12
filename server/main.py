"""Run from the repository root:

    uvicorn server.main:create_app --factory --port 8000

One worker process only (the POS session pool and the register cart live in-process).
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from server.api import customers_router, recon_router, sales_router
from server.api.errors import register_error_handlers
from server.container import Container

log = logging.getLogger("server")


async def _reconcile_loop(container: Container) -> None:
    interval = container.settings.reconcile_interval_seconds
    while True:
        await asyncio.sleep(interval)
        try:
            done = await container.reconciler.all_open()
            if done:
                log.info("reconciled %d stuck write(s)", len(done))
        except Exception:
            log.exception("reconcile pass failed")


def create_app(container: Container | None = None) -> FastAPI:
    """``container`` is injected by tests; in production it is built from the environment."""

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        c = container
        if c is None:
            from server.config import ServerSettings
            from server.container import build_container

            c = build_container(ServerSettings())
        app.state.container = c
        await c.start()
        log.info("server up, POS write mode = %s", c.settings.pos_write_mode)
        task = asyncio.create_task(_reconcile_loop(c)) if c.settings.reconcile_interval_seconds > 0 else None
        try:
            yield
        finally:
            if task:
                task.cancel()
            await c.stop()

    # Settings are read once here (not only in lifespan) so CORS can be configured up front.
    if container is None:
        from server.config import ServerSettings

        settings = ServerSettings()
    else:
        settings = container.settings

    app = FastAPI(title="Pos4Africa Server", lifespan=lifespan)
    if container is not None:
        app.state.container = container  # available even when lifespan is not run (tests)
    if settings.cors_origins:
        app.add_middleware(CORSMiddleware, allow_origins=settings.cors_origins, allow_methods=["*"], allow_headers=["*"])

    register_error_handlers(app)

    @app.get("/health", tags=["Health"])
    async def health():
        c: Container = app.state.container
        return {"status": "ok", "pos_write_mode": c.settings.pos_write_mode}

    for router in (customers_router, sales_router, recon_router):
        app.include_router(router)
    return app
