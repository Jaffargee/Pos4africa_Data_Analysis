from __future__ import annotations

import hmac
import re
from typing import Annotated

from fastapi import Depends, Header, HTTPException, Request

from server.container import Container

_KEY = re.compile(r"^[A-Za-z0-9_\-]{8,128}$")


def get_container(request: Request) -> Container:
    return request.app.state.container


async def require_api_key(
    container: Annotated[Container, Depends(get_container)],
    x_api_key: Annotated[str | None, Header()] = None,
) -> None:
    expected = container.settings.server_api_key.get_secret_value()
    if not x_api_key or not hmac.compare_digest(x_api_key.encode(), expected.encode()):
        raise HTTPException(status_code=401, detail="Missing or invalid X-API-Key")


async def idempotency_key(idempotency_key: Annotated[str | None, Header()] = None) -> str:
    """Required on every write.  It is what makes a retried request safe."""
    if not idempotency_key or not _KEY.match(idempotency_key):
        raise HTTPException(
            status_code=400,
            detail="Idempotency-Key header is required (8-128 chars: letters, digits, '-' or '_'). "
                   "Generate one UUID per user action and reuse it when retrying.",
        )
    return idempotency_key


ContainerDep = Annotated[Container, Depends(get_container)]
KeyDep = Annotated[str, Depends(idempotency_key)]
