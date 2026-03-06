"""Tests for FastAPI endpoints using SQLite in-memory database."""

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.pool import StaticPool

from isfl_epa.api.app import app
from isfl_epa.parser.schema import Game, ParsedPlay, PlayType
from isfl_epa.players.registry import PlayerRegistry
from isfl_epa.storage.database import (
    create_tables,
    load_registry,
    load_season,
    metadata,
)


def _make_game() -> Game:
    return Game(
        id=1000, season=50, league="ISFL",
        home_team="HOM", away_team="AWY",
        home_team_id=1, away_team_id=2,
        plays=[
            ParsedPlay(
                game_id=1000, quarter=1, clock="10:00",
                play_type=PlayType.PASS,
                description="Pass by QB, A., complete to WR, B. for 15 yds",
                passer="QB, A.", receiver="WR, B.",
                yards_gained=15, possession_team_id=1,
                score_away=0, score_home=0,
                away_team="AWY", home_team="HOM",
            ),
            ParsedPlay(
                game_id=1000, quarter=1, clock="9:30",
                play_type=PlayType.RUSH,
                description="Rush by RB, C. for 5 yds. Tackled by LB, D.",
                rusher="RB, C.", tackler="LB, D.",
                yards_gained=5, possession_team_id=2,
                score_away=0, score_home=0,
                away_team="AWY", home_team="HOM",
            ),
            ParsedPlay(
                game_id=1000, quarter=2, clock="5:00",
                play_type=PlayType.PASS,
                description="Pass by QB, A., complete to WR, B. for 30 yds TOUCHDOWN",
                passer="QB, A.", receiver="WR, B.",
                yards_gained=30, touchdown=True, possession_team_id=1,
                score_away=0, score_home=7,
                away_team="AWY", home_team="HOM",
            ),
        ],
    )


@pytest.fixture
def client():
    """Create a test client with SQLite in-memory database."""
    from contextlib import asynccontextmanager

    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    create_tables(engine)

    game = _make_game()
    registry = PlayerRegistry()
    registry.build_from_games([game])
    load_registry(engine, registry)
    load_season(engine, [game], registry)

    # Replace lifespan to use our test engine instead of Postgres
    @asynccontextmanager
    async def test_lifespan(a):
        a.state.engine = engine
        yield

    original_lifespan = app.router.lifespan_context
    app.router.lifespan_context = test_lifespan
    with TestClient(app) as c:
        yield c
    app.router.lifespan_context = original_lifespan


class TestPlaysEndpoints:
    def test_list_plays(self, client):
        resp = client.get("/plays/?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 3

    def test_list_plays_by_game(self, client):
        resp = client.get("/plays/1000")
        assert resp.status_code == 200
        assert len(resp.json()) == 3

    def test_filter_by_play_type(self, client):
        resp = client.get("/plays/?play_type=pass")
        assert resp.status_code == 200
        assert all(p["play_type"] == "pass" for p in resp.json())


class TestStatsEndpoints:
    def test_passing_leaders(self, client):
        resp = client.get("/stats/passing?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "QB, A."
        assert data[0]["yards"] == 45  # 15 + 30

    def test_rushing_leaders(self, client):
        resp = client.get("/stats/rushing?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "RB, C."

    def test_receiving_leaders(self, client):
        resp = client.get("/stats/receiving?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert data[0]["player"] == "WR, B."

    def test_team_stats(self, client):
        resp = client.get("/stats/team?season=50")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 2

    def test_player_game_log(self, client):
        # Find the player_id for QB, A.
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/stats/player/{pid}/game-log?category=passing")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1  # one game


class TestPlayersEndpoints:
    def test_search_by_name(self, client):
        resp = client.get("/players/?name=QB")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) >= 1
        assert "QB" in data[0]["canonical_name"]

    def test_get_player_profile(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/players/{pid}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["player"]["canonical_name"] == "QB, A."
        assert data["career_passing"]["yards"] == 45

    def test_get_player_plays(self, client):
        players = client.get("/players/?name=QB").json()
        pid = players[0]["player_id"]
        resp = client.get(f"/players/{pid}/plays")
        assert resp.status_code == 200
        assert len(resp.json()) >= 2  # 2 pass plays
