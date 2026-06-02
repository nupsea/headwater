"""FastAPI application — Headwater 2 API.

The guided H2 workflow is the product surface.  Each H2 route manages its own
metadata store (SQLite via ``HeadwaterStore``) and materializes analytical data
on demand, so the app needs no shared pipeline/runtime state.
"""

from __future__ import annotations

import logging
import traceback

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from headwater.api.routes import health

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logging.getLogger("headwater").setLevel(logging.INFO)
logger = logging.getLogger(__name__)


def create_app(*, in_memory: bool = False) -> FastAPI:
    """Create and configure the FastAPI application.

    ``in_memory`` is accepted for backwards-compatible call sites; H2 routes are
    stateless at the app level, so it has no effect.
    """
    app = FastAPI(
        title="Headwater",
        description="Advisory data platform for data professionals.",
        version="0.2.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Log full tracebacks for unhandled 500s.
    @app.exception_handler(Exception)
    async def unhandled_exception_handler(request: Request, exc: Exception):
        logger.error(
            "Unhandled exception on %s %s:\n%s",
            request.method,
            request.url.path,
            traceback.format_exc(),
        )
        return JSONResponse(
            status_code=500,
            content={"detail": f"{type(exc).__name__}: {exc}"},
        )

    @app.middleware("http")
    async def log_requests(request: Request, call_next):
        logger.info("%s %s", request.method, request.url.path)
        response = await call_next(request)
        if response.status_code >= 400:
            logger.warning(
                "%s %s -> %d", request.method, request.url.path, response.status_code
            )
        return response

    app.include_router(health.router, prefix="/api", tags=["health"])

    from headwater.api.routes.h2 import router as h2_router

    app.include_router(h2_router, prefix="/api")

    return app


app = create_app()
