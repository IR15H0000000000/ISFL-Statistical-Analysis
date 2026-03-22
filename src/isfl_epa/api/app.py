"""FastAPI application for ISFL EPA data."""

import logging
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from sqlalchemy import Engine
from sqlalchemy.exc import OperationalError
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from isfl_epa.api.routes import epa, players, plays, stats
from isfl_epa.storage.database import create_tables, get_engine

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    from isfl_epa.logging_config import setup_logging

    setup_logging()
    engine = get_engine()
    create_tables(engine)
    app.state.engine = engine
    yield
    engine.dispose()


app = FastAPI(title="ISFL EPA API", lifespan=lifespan)


@app.exception_handler(OperationalError)
async def db_exception_handler(request: Request, exc: OperationalError):
    logger.error("Database connection error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={"detail": "Database unavailable. Please try again later."},
    )


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Add Cache-Control headers to successful GET responses."""

    async def dispatch(self, request: Request, call_next) -> Response:
        response = await call_next(request)
        if request.method == "GET" and 200 <= response.status_code < 300:
            if "Cache-Control" not in response.headers:
                response.headers["Cache-Control"] = "public, max-age=300"
        return response


app.add_middleware(CacheControlMiddleware)

app.include_router(plays.router, prefix="/plays", tags=["plays"])
app.include_router(stats.router, prefix="/stats", tags=["stats"])
app.include_router(players.router, prefix="/players", tags=["players"])
app.include_router(epa.router, prefix="/epa", tags=["epa"])

# Static files for frontend
_static_dir = Path(__file__).parent / "static"
_static_dir.mkdir(exist_ok=True)
app.mount("/static", StaticFiles(directory=_static_dir), name="static")


@app.get("/")
def root():
    return RedirectResponse(url="/static/teams.html")


@app.get("/leaderboard")
def leaderboard_redirect():
    return RedirectResponse(url="/static/leaderboard.html")


@app.get("/plays-browser")
def plays_browser_redirect():
    return RedirectResponse(url="/static/plays.html")


@app.get("/drives")
def drives_redirect():
    return RedirectResponse(url="/static/drives.html")


def get_db_engine(app_instance: FastAPI = None) -> Engine:
    """Get the database engine from app state."""
    return app.state.engine
