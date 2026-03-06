"""FastAPI application for ISFL EPA data."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy import Engine

from isfl_epa.api.routes import players, plays, stats
from isfl_epa.storage.database import create_tables, get_engine


@asynccontextmanager
async def lifespan(app: FastAPI):
    engine = get_engine()
    create_tables(engine)
    app.state.engine = engine
    yield
    engine.dispose()


app = FastAPI(title="ISFL EPA API", lifespan=lifespan)
app.include_router(plays.router, prefix="/plays", tags=["plays"])
app.include_router(stats.router, prefix="/stats", tags=["stats"])
app.include_router(players.router, prefix="/players", tags=["players"])


def get_db_engine(app_instance: FastAPI = None) -> Engine:
    """Get the database engine from app state."""
    return app.state.engine
